defmodule SymphonyElixir.AgentRunner do
  @moduledoc """
  Executes a single Linear issue in its workspace with Codex.
  """

  require Logger
  alias SymphonyElixir.Codex.AppServer
  alias SymphonyElixir.{Config, Linear.Issue, ObservabilitySanitizer, PromptBuilder, Tracker, Workspace}

  @type worker_host :: String.t() | nil

  @spec run(map(), pid() | nil, keyword()) :: :ok | no_return()
  def run(issue, codex_update_recipient \\ nil, opts \\ []) do
    # The orchestrator owns host retries so one worker lifetime never hops machines.
    worker_host = selected_worker_host(Keyword.get(opts, :worker_host), Config.settings!().worker.ssh_hosts)

    Logger.info("Starting agent run for #{issue_context(issue)} worker_host=#{worker_host_for_log(worker_host)}")

    case run_on_worker_host(issue, codex_update_recipient, opts, worker_host) do
      :ok ->
        :ok

      {:error, reason} ->
        error_code = ObservabilitySanitizer.error_code(reason, "agent_run_failed")
        Logger.error("Agent run failed for #{issue_context(issue)} error_code=#{error_code}")
        raise RuntimeError, "Agent run failed for #{issue_context(issue)} error_code=#{error_code}"
    end
  end

  defp run_on_worker_host(issue, codex_update_recipient, opts, worker_host) do
    Logger.info("Starting worker attempt for #{issue_context(issue)} worker_host=#{worker_host_for_log(worker_host)}")

    with {:ok, prepared} <- prepared_workspace(issue, worker_host, opts),
         :ok <- send_worker_runtime_info(codex_update_recipient, issue, worker_host, prepared, opts) do
      try do
        with :ok <- Workspace.run_after_create_hook(prepared.path, issue, prepared.created?, worker_host),
             :ok <- Workspace.run_before_run_hook(prepared.path, issue, worker_host) do
          run_codex_turns(prepared.path, issue, codex_update_recipient, opts, worker_host)
        end
      after
        Workspace.run_after_run_hook(prepared.path, issue, worker_host)
      end
    end
  end

  defp prepared_workspace(issue, worker_host, opts) do
    case Keyword.get(opts, :prepared_workspace) do
      nil ->
        Workspace.prepare_for_issue(issue, worker_host, expected_workspace_path: Keyword.get(opts, :expected_workspace_path))

      prepared ->
        Workspace.validate_prepared_workspace(prepared, worker_host)
    end
  end

  defp codex_message_handler(recipient, issue, opts) do
    fn message ->
      send_codex_update(recipient, issue, message, Keyword.get(opts, :run_id))
    end
  end

  defp send_codex_update(recipient, %Issue{id: issue_id}, message, run_id)
       when is_binary(issue_id) and is_pid(recipient) do
    message = if is_map(message), do: Map.put(message, :run_id, run_id), else: message
    send(recipient, {:codex_worker_update, issue_id, message})
    :ok
  end

  defp send_codex_update(_recipient, _issue, _message, _run_id), do: :ok

  defp send_worker_runtime_info(
         recipient,
         %Issue{id: issue_id} = issue,
         worker_host,
         prepared,
         opts
       )
       when is_binary(issue_id) and is_pid(recipient) and is_map(prepared) do
    runtime_info = %{
      worker_host: worker_host,
      workspace_path: prepared.path,
      workspace_root: prepared.root,
      run_id: Keyword.get(opts, :run_id),
      runner_generation: Keyword.get(opts, :runner_generation),
      session_title: AppServer.session_title(issue)
    }

    if Keyword.get(opts, :runtime_ack_required, false) do
      acknowledgment_ref = make_ref()
      send(recipient, {:worker_runtime_info, issue_id, runtime_info, self(), acknowledgment_ref})

      receive do
        {:worker_runtime_ack, ^acknowledgment_ref, :ok} -> :ok
        {:worker_runtime_ack, ^acknowledgment_ref, {:error, reason}} -> {:error, reason}
      after
        Config.settings!().codex.read_timeout_ms -> {:error, :worker_runtime_ack_timeout}
      end
    else
      send(recipient, {:worker_runtime_info, issue_id, runtime_info})
      :ok
    end
  end

  defp send_worker_runtime_info(_recipient, _issue, _worker_host, _prepared, _opts), do: :ok

  defp run_codex_turns(workspace, issue, codex_update_recipient, opts, worker_host) do
    max_turns = Keyword.get(opts, :max_turns, Config.settings!().agent.max_turns)
    tracker_context = Keyword.get_lazy(opts, :tracker_context, &Tracker.current_poll_context/0)
    opts = Keyword.put(opts, :tracker_context, tracker_context)

    issue_state_fetcher =
      Keyword.get(opts, :issue_state_fetcher, fn issue_ids ->
        fetch_issue_states(issue_ids, tracker_context)
      end)

    with {:ok, session} <-
           AppServer.start_session(workspace,
             worker_host: worker_host,
             session_title: AppServer.session_title(issue),
             tracker_context: tracker_context
           ) do
      try do
        with :ok <- send_worker_model_resolution(codex_update_recipient, issue, session, opts) do
          do_run_codex_turns(session, workspace, issue, codex_update_recipient, opts, issue_state_fetcher, 1, max_turns)
        end
      after
        AppServer.stop_session(session)
      end
    end
  end

  defp send_worker_model_resolution(
         recipient,
         %Issue{id: issue_id},
         session,
         opts
       )
       when is_binary(issue_id) and is_pid(recipient) and is_map(session) do
    if Keyword.get(opts, :runtime_ack_required, false) do
      acknowledgment_ref = make_ref()

      resolution_info = %{
        run_id: Keyword.get(opts, :run_id),
        runner_generation: Keyword.get(opts, :runner_generation),
        resolved_model: session.resolved_model,
        reasoning_effort: session.reasoning_effort,
        model_catalog_source: get_in(session, [:metadata, :model_catalog_source]),
        model_catalog: get_in(session, [:metadata, :model_catalog])
      }

      send(
        recipient,
        {:worker_model_resolution, issue_id, resolution_info, self(), acknowledgment_ref}
      )

      receive do
        {:worker_model_resolution_ack, ^acknowledgment_ref, :ok} -> :ok
        {:worker_model_resolution_ack, ^acknowledgment_ref, {:error, reason}} -> {:error, reason}
      after
        Config.settings!().codex.read_timeout_ms -> {:error, :worker_model_resolution_ack_timeout}
      end
    else
      :ok
    end
  end

  defp send_worker_model_resolution(_recipient, _issue, _session, _opts), do: :ok

  defp do_run_codex_turns(app_session, workspace, issue, codex_update_recipient, opts, issue_state_fetcher, turn_number, max_turns) do
    prompt = build_turn_prompt(issue, opts, turn_number, max_turns)

    with {:ok, turn_session} <-
           AppServer.run_turn(
             app_session,
             prompt,
             issue,
             on_message: codex_message_handler(codex_update_recipient, issue, opts)
           ) do
      Logger.info("Completed agent run for #{issue_context(issue)} session_id=#{turn_session[:session_id]} workspace=#{workspace} turn=#{turn_number}/#{max_turns}")

      active_states = Keyword.fetch!(opts, :tracker_context).active_states

      case continue_with_issue?(issue, issue_state_fetcher, active_states) do
        {:continue, refreshed_issue} when turn_number < max_turns ->
          Logger.info("Continuing agent run for #{issue_context(refreshed_issue)} after normal turn completion turn=#{turn_number}/#{max_turns}")

          do_run_codex_turns(
            app_session,
            workspace,
            refreshed_issue,
            codex_update_recipient,
            opts,
            issue_state_fetcher,
            turn_number + 1,
            max_turns
          )

        {:continue, refreshed_issue} ->
          Logger.info("Reached agent.max_turns for #{issue_context(refreshed_issue)} with issue still active; parking for operator action")

          send_worker_budget_exhausted(
            codex_update_recipient,
            refreshed_issue,
            opts,
            max_turns
          )

          :ok

        {:done, _refreshed_issue} ->
          :ok

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp send_worker_budget_exhausted(recipient, issue, opts, max_turns)
       when is_pid(recipient) do
    send(
      recipient,
      {:worker_budget_exhausted, issue.id,
       %{
         run_id: Keyword.get(opts, :run_id),
         terminal_reason: "turn_budget_exhausted",
         limit: max_turns
       }}
    )

    :ok
  end

  defp send_worker_budget_exhausted(_recipient, _issue, _opts, _max_turns), do: :ok

  defp build_turn_prompt(issue, opts, 1, _max_turns) do
    [AppServer.session_title(issue), "\n\n", PromptBuilder.build_prompt(issue, opts)]
    |> IO.iodata_to_binary()
  end

  defp build_turn_prompt(_issue, _opts, turn_number, max_turns) do
    """
    Continuation guidance:

    - The previous Codex turn completed normally, but the Linear issue is still in an active state.
    - This is continuation turn ##{turn_number} of #{max_turns} for the current agent run.
    - Resume from the current workspace and workpad state instead of restarting from scratch.
    - The original task instructions and prior turn context are already present in this thread, so do not restate them before acting.
    - Focus on the remaining ticket work and do not end the turn while the issue stays active unless you are truly blocked.
    """
  end

  defp continue_with_issue?(%Issue{id: issue_id} = issue, issue_state_fetcher, active_states)
       when is_binary(issue_id) do
    case issue_state_fetcher.([issue_id]) do
      {:ok, [%Issue{} = refreshed_issue | _]} ->
        if active_issue_state?(refreshed_issue.state, active_states) do
          {:continue, refreshed_issue}
        else
          {:done, refreshed_issue}
        end

      {:ok, []} ->
        {:done, issue}

      {:error, reason} ->
        {:error, {:issue_state_refresh_failed, reason}}
    end
  end

  defp continue_with_issue?(issue, _issue_state_fetcher, _active_states), do: {:done, issue}

  defp active_issue_state?(state_name, active_states)
       when is_binary(state_name) and is_list(active_states) do
    normalized_state = normalize_issue_state(state_name)

    active_states
    |> Enum.any?(fn active_state -> normalize_issue_state(active_state) == normalized_state end)
  end

  defp active_issue_state?(_state_name, _active_states), do: false

  defp fetch_issue_states(issue_ids, tracker_context) do
    if Tracker.authority_valid?(tracker_context) do
      Tracker.fetch_issue_states_by_ids(issue_ids, tracker_context)
    else
      {:error, :tracker_authority_invalidated}
    end
  end

  defp selected_worker_host(nil, []), do: nil

  defp selected_worker_host(preferred_host, configured_hosts) when is_list(configured_hosts) do
    hosts =
      configured_hosts
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.uniq()

    case preferred_host do
      host when is_binary(host) and host != "" -> host
      _ when hosts == [] -> nil
      _ -> List.first(hosts)
    end
  end

  defp worker_host_for_log(nil), do: "local"
  defp worker_host_for_log(worker_host), do: worker_host

  defp normalize_issue_state(state_name) when is_binary(state_name) do
    state_name
    |> String.trim()
    |> String.downcase()
  end

  defp issue_context(%Issue{id: issue_id, identifier: identifier}) do
    "issue_id=#{issue_id} issue_identifier=#{identifier}"
  end
end
