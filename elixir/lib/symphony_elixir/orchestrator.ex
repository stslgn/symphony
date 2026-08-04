defmodule SymphonyElixir.Orchestrator do
  @moduledoc """
  Polls Linear and dispatches repository copies to Codex-backed workers.
  """

  use GenServer
  require Logger
  import Bitwise, only: [<<<: 2]

  alias SymphonyElixir.{
    AgentRunner,
    Config,
    ObservabilitySanitizer,
    OperatorCommand,
    OperatorWait,
    RateLimitTelemetry,
    RunBudget,
    RunLedger,
    StatusDashboard,
    Tracker,
    Workspace
  }

  alias SymphonyElixir.Linear.Issue

  @continuation_retry_delay_ms 1_000
  @failure_retry_base_ms 10_000
  # Slightly above the dashboard render interval so "checking now…" can render.
  @poll_transition_render_delay_ms 20
  @empty_codex_totals %{
    input_tokens: 0,
    output_tokens: 0,
    total_tokens: 0,
    seconds_running: 0
  }

  defmodule State do
    @moduledoc """
    Runtime state for the orchestrator polling loop.
    """

    defstruct [
      :poll_interval_ms,
      :max_concurrent_agents,
      :next_poll_due_at_ms,
      :poll_check_in_progress,
      :tick_timer_ref,
      :tick_token,
      :run_ledger_path,
      :runner_generation,
      dispatch_paused: false,
      running: %{},
      parked: %{},
      completed: MapSet.new(),
      claimed: MapSet.new(),
      recovered_attempts: %{},
      recovered_dispatches: %{},
      queued_resumes: %{},
      retry_attempts: %{},
      processed_operator_comment_ids: MapSet.new(),
      operator_comment_cursors: %{},
      codex_totals: nil,
      codex_rate_limits: nil
    ]
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    now_ms = System.monotonic_time(:millisecond)
    config = Config.settings!()
    run_ledger_path = Keyword.get(opts, :run_ledger_path, RunLedger.default_path())
    runner_generation = RunLedger.new_id("runner")
    restore_parked_waits_fn = Keyword.get(opts, :restore_parked_waits_fn, &restore_parked_waits/1)

    case RunLedger.reconcile_startup(run_ledger_path, runner_generation) do
      {:ok, recovery} ->
        with {:ok, parked} <- restore_parked_waits_fn.(recovery.parked),
             {:ok, queued_resumes} <- restore_queued_resumes(recovery.queued_resumes) do
          state = %State{
            poll_interval_ms: config.polling.interval_ms,
            max_concurrent_agents: config.agent.max_concurrent_agents,
            next_poll_due_at_ms: now_ms,
            poll_check_in_progress: false,
            tick_timer_ref: nil,
            tick_token: nil,
            run_ledger_path: run_ledger_path,
            runner_generation: runner_generation,
            dispatch_paused: recovery.dispatch_paused,
            recovered_attempts: recovery.recovered_attempts,
            recovered_dispatches: recovery.recovered_dispatches,
            queued_resumes: queued_resumes,
            parked: parked,
            processed_operator_comment_ids: recovery.processed_operator_comment_ids,
            operator_comment_cursors: restore_operator_comment_cursors(recovery.operator_comment_cursors),
            codex_totals: @empty_codex_totals,
            codex_rate_limits: nil
          }

          run_terminal_workspace_cleanup()
          state = schedule_tick(state, 0)

          {:ok, state}
        else
          {:error, reason} ->
            {:stop, {:operator_wait_restore_failed, reason}}
        end

      {:error, reason} ->
        {:stop, {:run_ledger_unavailable, reason}}
    end
  end

  @impl true
  def handle_info({:tick, tick_token}, %{tick_token: tick_token} = state)
      when is_reference(tick_token) do
    state = refresh_runtime_config(state)

    state = %{
      state
      | poll_check_in_progress: true,
        next_poll_due_at_ms: nil,
        tick_timer_ref: nil,
        tick_token: nil
    }

    notify_dashboard()
    :ok = schedule_poll_cycle_start()
    {:noreply, state}
  end

  def handle_info({:tick, _tick_token}, state), do: {:noreply, state}

  def handle_info(:tick, state) do
    state = refresh_runtime_config(state)

    state = %{
      state
      | poll_check_in_progress: true,
        next_poll_due_at_ms: nil,
        tick_timer_ref: nil,
        tick_token: nil
    }

    notify_dashboard()
    :ok = schedule_poll_cycle_start()
    {:noreply, state}
  end

  def handle_info(:run_poll_cycle, state) do
    state = refresh_runtime_config(state)
    state = maybe_dispatch(state)
    state = schedule_tick(state, state.poll_interval_ms)
    state = %{state | poll_check_in_progress: false}

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{running: running} = state
      ) do
    case find_issue_id_for_ref(running, ref) do
      nil ->
        {:noreply, state}

      issue_id ->
        running_entry =
          state.running
          |> Map.fetch!(issue_id)
          |> Map.put(:worker_exit_established, true)

        state = %{state | running: Map.put(state.running, issue_id, running_entry)}
        session_id = running_entry_session_id(running_entry)

        state =
          if Map.has_key?(running_entry, :terminal_pending) do
            mark_pending_worker_exited(state, issue_id, running_entry)
          else
            finish_worker_run(state, issue_id, running_entry, reason, session_id)
          end

        Logger.info("Agent task finished for issue_id=#{issue_id} session_id=#{session_id}")

        notify_dashboard()
        {:noreply, state}
    end
  end

  def handle_info({:worker_runtime_info, issue_id, runtime_info}, %{running: running} = state)
      when is_binary(issue_id) and is_map(runtime_info) do
    case Map.get(running, issue_id) do
      nil ->
        {:noreply, state}

      running_entry ->
        if runtime_info_matches_run?(runtime_info, running_entry) do
          updated_running_entry =
            running_entry
            |> maybe_put_runtime_value(:worker_host, runtime_info[:worker_host])
            |> maybe_put_runtime_value(:workspace_path, runtime_info[:workspace_path])
            |> maybe_put_runtime_value(:session_title, runtime_info[:session_title])

          state = record_run_event(state, updated_running_entry, "run_runtime_ready", "running")
          notify_dashboard()
          {:noreply, %{state | running: Map.put(running, issue_id, updated_running_entry)}}
        else
          Logger.warning("Ignoring stale worker runtime info issue_id=#{issue_id} run_id=#{inspect(runtime_info[:run_id])}")
          {:noreply, state}
        end
    end
  end

  def handle_info(
        {:codex_worker_update, issue_id, %{event: _, timestamp: _} = update},
        %{running: running} = state
      ) do
    case Map.get(running, issue_id) do
      nil ->
        {:noreply, state}

      running_entry ->
        if runtime_info_matches_run?(update, running_entry) do
          {updated_running_entry, token_delta} = integrate_codex_update(running_entry, update)

          state =
            state
            |> maybe_record_model_resolution(running_entry, updated_running_entry)
            |> apply_codex_token_delta(token_delta)
            |> apply_codex_rate_limits(update)
            |> Map.put(:running, Map.put(running, issue_id, updated_running_entry))
            |> maybe_park_exhausted_run(issue_id)

          notify_dashboard()
          {:noreply, state}
        else
          Logger.warning("Ignoring stale Codex worker update issue_id=#{issue_id} run_id=#{inspect(update[:run_id])}")
          {:noreply, state}
        end
    end
  end

  def handle_info({:codex_worker_update, _issue_id, _update}, state), do: {:noreply, state}

  def handle_info(
        {:worker_budget_exhausted, issue_id, %{run_id: run_id, terminal_reason: "turn_budget_exhausted"}},
        state
      )
      when is_binary(issue_id) do
    state =
      case Map.get(state.running, issue_id) do
        %{run_id: ^run_id} -> park_budget_exhausted(state, issue_id, "turn_budget_exhausted")
        _other -> state
      end

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info({:run_budget_timeout, issue_id, run_id}, state)
      when is_binary(issue_id) and is_binary(run_id) do
    state =
      case Map.get(state.running, issue_id) do
        %{run_id: ^run_id} -> park_budget_exhausted(state, issue_id, "time_budget_exhausted")
        _other -> state
      end

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info({:retry_issue, issue_id, retry_token}, state) do
    result =
      if state.dispatch_paused do
        {:noreply, defer_retry_while_paused(state, issue_id, retry_token)}
      else
        case pop_retry_attempt_state(state, issue_id, retry_token) do
          {:ok, attempt, metadata, state} -> handle_retry_issue(state, issue_id, attempt, metadata)
          :missing -> {:noreply, state}
        end
      end

    notify_dashboard()
    result
  end

  def handle_info({:retry_issue, _issue_id}, state), do: {:noreply, state}

  def handle_info(msg, state) do
    Logger.debug("Orchestrator ignored message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp maybe_dispatch(%State{} = state) do
    state =
      state
      |> reconcile_running_issues()
      |> reconcile_parked_issues()
      |> process_operator_comments()

    with false <- state.dispatch_paused,
         :ok <- Config.validate!(),
         :ok <- Config.validate_runtime_capabilities(),
         {:ok, issues} <- Tracker.fetch_candidate_issues(),
         true <- available_slots(state) > 0 do
      choose_issues(issues, state)
    else
      {:error, :missing_linear_api_token} ->
        Logger.error("Linear API token missing in WORKFLOW.md")
        state

      {:error, :missing_linear_project_slug} ->
        Logger.error("Linear project slug missing in WORKFLOW.md")
        state

      {:error, :missing_tracker_kind} ->
        Logger.error("Tracker kind missing in WORKFLOW.md")

        state

      {:error, {:unsupported_tracker_kind, kind}} ->
        Logger.error("Unsupported tracker kind in WORKFLOW.md: #{inspect(kind)}")

        state

      {:error, {:invalid_workflow_config, message}} ->
        Logger.error("Invalid WORKFLOW.md config: #{message}")
        state

      {:error, {:missing_required_dynamic_tools, tools}} ->
        Logger.error("Runtime capability preflight blocked dispatch: missing_required_dynamic_tools=#{Enum.join(tools, ",")}")
        state

      {:error, {:missing_workflow_file, path, reason}} ->
        Logger.error("Missing WORKFLOW.md at #{path}: #{inspect(reason)}")
        state

      {:error, :workflow_front_matter_not_a_map} ->
        Logger.error("Failed to parse WORKFLOW.md: workflow front matter must decode to a map")
        state

      {:error, {:workflow_parse_error, reason}} ->
        Logger.error("Failed to parse WORKFLOW.md: #{inspect(reason)}")
        state

      {:error, reason} ->
        Logger.error("Failed to fetch from Linear: #{inspect(reason)}")
        state

      false ->
        state

      true ->
        state
    end
  end

  defp reconcile_running_issues(%State{} = state) do
    state = state |> retry_pending_terminal_transitions() |> reconcile_stalled_running_issues()

    running_ids =
      state.running
      |> Enum.reject(fn {_issue_id, running_entry} ->
        Map.has_key?(running_entry, :terminal_pending)
      end)
      |> Enum.map(&elem(&1, 0))

    if running_ids == [] do
      state
    else
      case Tracker.fetch_issue_states_by_ids(running_ids) do
        {:ok, issues} ->
          issues
          |> reconcile_running_issue_states(
            state,
            active_state_set(),
            terminal_state_set()
          )
          |> reconcile_missing_running_issue_ids(running_ids, issues)

        {:error, reason} ->
          Logger.debug("Failed to refresh running issue states: #{inspect(reason)}; keeping active workers")

          state
      end
    end
  end

  defp reconcile_parked_issues(%State{parked: parked} = state) when map_size(parked) == 0,
    do: state

  defp reconcile_parked_issues(%State{} = state) do
    parked_ids = Map.keys(state.parked)

    case Tracker.fetch_issue_states_by_ids(parked_ids) do
      {:ok, issues} ->
        Enum.reduce(issues, state, &reconcile_parked_issue/2)

      {:error, reason} ->
        Logger.debug("Failed to refresh parked issue states: #{inspect(reason)}; keeping operator waits")
        state
    end
  end

  defp reconcile_parked_issue(%Issue{} = issue, %State{} = state) do
    if terminal_issue_state?(issue.state, terminal_state_set()) or
         !issue_routable_to_worker?(issue) do
      release_parked_issue(state, issue.id, "tracker_released")
    else
      update_in(state.parked[issue.id], fn
        nil -> nil
        wait -> %{wait | tracker_state: issue.state, identifier: issue.identifier}
      end)
    end
  end

  defp reconcile_parked_issue(_issue, state), do: state

  @doc false
  @spec reconcile_issue_states_for_test([Issue.t()], term()) :: term()
  def reconcile_issue_states_for_test(issues, %State{} = state) when is_list(issues) do
    reconcile_running_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  def reconcile_issue_states_for_test(issues, state) when is_list(issues) do
    reconcile_running_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec should_dispatch_issue_for_test(Issue.t(), term()) :: boolean()
  def should_dispatch_issue_for_test(%Issue{} = issue, %State{} = state) do
    should_dispatch_issue?(issue, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec revalidate_issue_for_dispatch_for_test(Issue.t(), ([String.t()] -> term())) ::
          {:ok, Issue.t()} | {:skip, Issue.t() | :missing} | {:error, term()}
  def revalidate_issue_for_dispatch_for_test(%Issue{} = issue, issue_fetcher)
      when is_function(issue_fetcher, 1) do
    revalidate_issue_for_dispatch(issue, issue_fetcher, terminal_state_set())
  end

  @doc false
  @spec sort_issues_for_dispatch_for_test([Issue.t()]) :: [Issue.t()]
  def sort_issues_for_dispatch_for_test(issues) when is_list(issues) do
    sort_issues_for_dispatch(issues)
  end

  @doc false
  @spec select_worker_host_for_test(term(), String.t() | nil) ::
          String.t() | nil | :no_worker_capacity
  def select_worker_host_for_test(%State{} = state, preferred_worker_host) do
    select_worker_host(state, preferred_worker_host)
  end

  @doc false
  @spec select_worker_host_for_test(term(), String.t() | nil, boolean()) ::
          String.t() | nil | :no_worker_capacity | :affinity_unavailable
  def select_worker_host_for_test(%State{} = state, preferred_worker_host, affinity_required) do
    select_worker_host(state, preferred_worker_host, affinity_required)
  end

  @doc false
  @spec retry_pending_terminal_transitions_for_test(term()) :: term()
  def retry_pending_terminal_transitions_for_test(%State{} = state) do
    retry_pending_terminal_transitions(state)
  end

  defp reconcile_running_issue_states([], state, _active_states, _terminal_states), do: state

  defp reconcile_running_issue_states([issue | rest], state, active_states, terminal_states) do
    reconcile_running_issue_states(
      rest,
      reconcile_issue_state(issue, state, active_states, terminal_states),
      active_states,
      terminal_states
    )
  end

  defp reconcile_issue_state(%Issue{} = issue, state, active_states, terminal_states) do
    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Issue moved to terminal state: #{issue_context(issue)} state=#{issue.state}; stopping active agent")

        terminate_running_issue(state, issue.id, true, "tracker_terminal")

      !issue_routable_to_worker?(issue) ->
        Logger.info("Issue no longer routed to this worker: #{issue_context(issue)} assignee=#{inspect(issue.assignee_id)}; stopping active agent")

        terminate_running_issue(state, issue.id, false, "worker_route_removed")

      parked_reason = OperatorWait.reason_for_tracker_state(issue.state) ->
        park_running_issue(state, issue, parked_reason)

      active_issue_state?(issue.state, active_states) ->
        refresh_running_issue_state(state, issue)

      true ->
        Logger.info("Issue moved to non-active state: #{issue_context(issue)} state=#{issue.state}; stopping active agent")

        terminate_running_issue(state, issue.id, false, "tracker_non_active")
    end
  end

  defp reconcile_issue_state(_issue, state, _active_states, _terminal_states), do: state

  defp reconcile_missing_running_issue_ids(%State{} = state, requested_issue_ids, issues)
       when is_list(requested_issue_ids) and is_list(issues) do
    visible_issue_ids =
      issues
      |> Enum.flat_map(fn
        %Issue{id: issue_id} when is_binary(issue_id) -> [issue_id]
        _ -> []
      end)
      |> MapSet.new()

    Enum.reduce(requested_issue_ids, state, fn issue_id, state_acc ->
      if MapSet.member?(visible_issue_ids, issue_id) do
        state_acc
      else
        log_missing_running_issue(state_acc, issue_id)
        terminate_running_issue(state_acc, issue_id, false, "tracker_not_visible")
      end
    end)
  end

  defp reconcile_missing_running_issue_ids(state, _requested_issue_ids, _issues), do: state

  defp log_missing_running_issue(%State{} = state, issue_id) when is_binary(issue_id) do
    case Map.get(state.running, issue_id) do
      %{identifier: identifier} ->
        Logger.info("Issue no longer visible during running-state refresh: issue_id=#{issue_id} issue_identifier=#{identifier}; stopping active agent")

      _ ->
        Logger.info("Issue no longer visible during running-state refresh: issue_id=#{issue_id}; stopping active agent")
    end
  end

  defp log_missing_running_issue(_state, _issue_id), do: :ok

  defp refresh_running_issue_state(%State{} = state, %Issue{} = issue) do
    case Map.get(state.running, issue.id) do
      %{issue: _} = running_entry ->
        %{state | running: Map.put(state.running, issue.id, %{running_entry | issue: issue})}

      _ ->
        state
    end
  end

  defp park_running_issue(%State{} = state, %Issue{} = issue, reason, opts \\ []) do
    case Map.get(state.running, issue.id) do
      nil ->
        state

      %{terminal_pending: _pending} ->
        state

      running_entry ->
        with {:ok, wait} <-
               OperatorWait.new(reason, %{
                 issue_id: issue.id,
                 identifier: issue.identifier,
                 run_id: Map.get(running_entry, :run_id),
                 attempt: Map.get(running_entry, :retry_attempt, 0),
                 tracker_state: issue.state,
                 terminal_reason: Keyword.get(opts, :terminal_reason),
                 worker_host: Map.get(running_entry, :worker_host),
                 workspace_path: Map.get(running_entry, :workspace_path)
               }),
             :ok <- append_run_event(state, operator_wait_event(wait, "run_parked")) do
          Logger.info("Issue parked for operator action: #{issue_context(issue)} reason=#{reason} wait_id=#{wait.wait_id}")

          state = record_session_completion_totals(state, running_entry)
          stop_running_entry(running_entry)

          %{
            state
            | running: Map.delete(state.running, issue.id),
              parked: Map.put(state.parked, issue.id, wait),
              claimed: MapSet.delete(state.claimed, issue.id),
              retry_attempts: Map.delete(state.retry_attempts, issue.id)
          }
        else
          {:error, error} ->
            Logger.error("Failed to durably park issue_id=#{issue.id} reason=#{reason}: #{inspect(error)}")
            state
        end
    end
  end

  defp maybe_park_exhausted_run(%State{} = state, issue_id) do
    case Map.get(state.running, issue_id) do
      nil ->
        state

      running_entry ->
        reason =
          RunBudget.exhausted_reason(
            Map.get(running_entry, :run_budget, disabled_run_budget()),
            run_budget_metrics(running_entry, DateTime.utc_now())
          )

        if reason do
          park_budget_exhausted(state, issue_id, reason)
        else
          state
        end
    end
  end

  defp park_budget_exhausted(%State{} = state, issue_id, terminal_reason) do
    case Map.get(state.running, issue_id) do
      %{issue: %Issue{} = issue} ->
        if RunBudget.valid_terminal_reason?(terminal_reason) do
          Logger.warning("Run budget exhausted: #{issue_context(issue)} terminal_reason=#{terminal_reason}")

          park_running_issue(state, issue, "run_budget_exhausted", terminal_reason: terminal_reason)
        else
          state
        end

      _other ->
        state
    end
  end

  defp release_parked_issue(%State{} = state, issue_id, terminal_reason) do
    case Map.get(state.parked, issue_id) do
      nil ->
        state

      wait ->
        event =
          wait
          |> operator_wait_event("wait_released")
          |> Map.put(:terminal_reason, terminal_reason)

        case append_run_event(state, event) do
          :ok ->
            %{state | parked: Map.delete(state.parked, issue_id)}

          {:error, reason} ->
            Logger.error("Failed to release parked issue_id=#{issue_id}: #{inspect(reason)}")
            state
        end
    end
  end

  defp stop_running_entry(running_entry) do
    cancel_run_budget_timer(running_entry)

    if is_pid(running_entry[:pid]) and
         not Map.get(running_entry, :worker_exit_established, false) and
         Process.alive?(running_entry.pid) do
      :ok = terminate_task(running_entry.pid)
    end

    if is_reference(running_entry[:ref]), do: Process.demonitor(running_entry.ref, [:flush])
    :ok
  end

  defp cancel_run_budget_timer(running_entry) when is_map(running_entry) do
    case Map.get(running_entry, :run_budget_timer_ref) do
      ref when is_reference(ref) -> Process.cancel_timer(ref)
      _other -> false
    end
  end

  defp cancel_run_budget_timer(_running_entry), do: false

  defp arm_run_budget_timer(running_entry) do
    case get_in(running_entry, [:run_budget, :max_seconds]) do
      seconds when is_integer(seconds) and seconds > 0 ->
        timer_ref =
          Process.send_after(
            self(),
            {:run_budget_timeout, running_entry.issue.id, running_entry.run_id},
            seconds * 1_000
          )

        Map.put(running_entry, :run_budget_timer_ref, timer_ref)

      _other ->
        running_entry
    end
  end

  defp terminate_running_issue(
         %State{} = state,
         issue_id,
         cleanup_workspace,
         terminal_reason,
         opts \\ []
       ) do
    case Map.get(state.running, issue_id) do
      nil ->
        release_issue_claim(state, issue_id)

      running_entry ->
        pending = %{
          transition: "run_stopped",
          terminal_reason: terminal_reason,
          action: {:stop, cleanup_workspace, Keyword.get(opts, :retry)}
        }

        persist_or_block_terminal(state, issue_id, running_entry, pending)
    end
  end

  defp reconcile_stalled_running_issues(%State{} = state) do
    timeout_ms = Config.settings!().codex.stall_timeout_ms

    cond do
      timeout_ms <= 0 ->
        state

      map_size(state.running) == 0 ->
        state

      true ->
        now = DateTime.utc_now()

        Enum.reduce(state.running, state, fn {issue_id, running_entry}, state_acc ->
          restart_stalled_issue(state_acc, issue_id, running_entry, now, timeout_ms)
        end)
    end
  end

  defp restart_stalled_issue(state, issue_id, running_entry, now, timeout_ms) do
    elapsed_ms = stall_elapsed_ms(running_entry, now)

    if not Map.has_key?(running_entry, :terminal_pending) and is_integer(elapsed_ms) and
         elapsed_ms > timeout_ms do
      identifier = Map.get(running_entry, :identifier, issue_id)
      session_id = running_entry_session_id(running_entry)

      Logger.warning("Issue stalled: issue_id=#{issue_id} issue_identifier=#{identifier} session_id=#{session_id} elapsed_ms=#{elapsed_ms}; restarting with backoff")

      next_attempt = next_retry_attempt_from_running(running_entry)

      terminate_running_issue(state, issue_id, false, "stall_timeout",
        retry: %{
          attempt: next_attempt,
          metadata: %{
            identifier: identifier,
            error: "stalled for #{elapsed_ms}ms without codex activity",
            previous_run_id: Map.get(running_entry, :run_id),
            previous_attempt: Map.get(running_entry, :retry_attempt, 0),
            worker_host: Map.get(running_entry, :worker_host),
            workspace_path: Map.get(running_entry, :workspace_path)
          }
        }
      )
    else
      state
    end
  end

  defp stall_elapsed_ms(running_entry, now) do
    running_entry
    |> last_activity_timestamp()
    |> case do
      %DateTime{} = timestamp ->
        max(0, DateTime.diff(now, timestamp, :millisecond))

      _ ->
        nil
    end
  end

  defp last_activity_timestamp(running_entry) when is_map(running_entry) do
    Map.get(running_entry, :last_codex_timestamp) || Map.get(running_entry, :started_at)
  end

  defp last_activity_timestamp(_running_entry), do: nil

  defp terminate_task(pid) when is_pid(pid) do
    exit_ref = Process.monitor(pid)

    case Task.Supervisor.terminate_child(SymphonyElixir.TaskSupervisor, pid) do
      :ok ->
        await_worker_exit(pid, exit_ref)

      {:error, :not_found} ->
        Process.exit(pid, :shutdown)
        await_worker_exit(pid, exit_ref)
    end
  end

  defp terminate_task(_pid), do: :ok

  defp await_worker_exit(pid, exit_ref) do
    receive do
      {:DOWN, ^exit_ref, :process, ^pid, _reason} ->
        :ok
    after
      5_000 ->
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^exit_ref, :process, ^pid, _reason} -> :ok
        after
          1_000 -> {:error, :worker_exit_timeout}
        end
    end
  end

  defp choose_issues(issues, state) do
    active_states = active_state_set()
    terminal_states = terminal_state_set()

    issues
    |> sort_issues_for_dispatch()
    |> Enum.reduce(state, fn issue, state_acc ->
      if should_dispatch_issue?(issue, state_acc, active_states, terminal_states) do
        dispatch = dispatch_context(state_acc, issue.id)

        dispatch_issue(
          state_acc,
          issue,
          dispatch.attempt,
          dispatch.worker_host,
          dispatch.workspace_path,
          dispatch.affinity_required
        )
      else
        state_acc
      end
    end)
  end

  defp sort_issues_for_dispatch(issues) when is_list(issues) do
    Enum.sort_by(issues, fn
      %Issue{} = issue ->
        {priority_rank(issue.priority), issue_created_at_sort_key(issue), issue.identifier || issue.id || ""}

      _ ->
        {priority_rank(nil), issue_created_at_sort_key(nil), ""}
    end)
  end

  defp priority_rank(priority) when is_integer(priority) and priority in 1..4, do: priority
  defp priority_rank(_priority), do: 5

  defp issue_created_at_sort_key(%Issue{created_at: %DateTime{} = created_at}) do
    DateTime.to_unix(created_at, :microsecond)
  end

  defp issue_created_at_sort_key(%Issue{}), do: 9_223_372_036_854_775_807
  defp issue_created_at_sort_key(_issue), do: 9_223_372_036_854_775_807

  defp should_dispatch_issue?(
         %Issue{} = issue,
         %State{running: running, parked: parked, claimed: claimed} = state,
         active_states,
         terminal_states
       ) do
    !state.dispatch_paused and
      candidate_issue?(issue, active_states, terminal_states) and
      !todo_issue_blocked_by_non_terminal?(issue, terminal_states) and
      !MapSet.member?(claimed, issue.id) and
      !Map.has_key?(running, issue.id) and
      !Map.has_key?(parked, issue.id) and
      available_slots(state) > 0 and
      state_slots_available?(issue, running) and
      worker_slots_available?(state)
  end

  defp should_dispatch_issue?(_issue, _state, _active_states, _terminal_states), do: false

  defp state_slots_available?(%Issue{state: issue_state}, running) when is_map(running) do
    limit = Config.max_concurrent_agents_for_state(issue_state)
    used = running_issue_count_for_state(running, issue_state)
    limit > used
  end

  defp state_slots_available?(_issue, _running), do: false

  defp running_issue_count_for_state(running, issue_state) when is_map(running) do
    normalized_state = normalize_issue_state(issue_state)

    Enum.count(running, fn
      {_id, %{issue: %Issue{state: state_name}}} ->
        normalize_issue_state(state_name) == normalized_state

      _ ->
        false
    end)
  end

  defp candidate_issue?(
         %Issue{
           id: id,
           identifier: identifier,
           title: title,
           state: state_name
         } = issue,
         active_states,
         terminal_states
       )
       when is_binary(id) and is_binary(identifier) and is_binary(title) and is_binary(state_name) do
    issue_routable_to_worker?(issue) and
      active_issue_state?(state_name, active_states) and
      !terminal_issue_state?(state_name, terminal_states)
  end

  defp candidate_issue?(_issue, _active_states, _terminal_states), do: false

  defp issue_routable_to_worker?(%Issue{assigned_to_worker: assigned_to_worker})
       when is_boolean(assigned_to_worker),
       do: assigned_to_worker

  defp issue_routable_to_worker?(_issue), do: true

  defp todo_issue_blocked_by_non_terminal?(
         %Issue{state: issue_state, blocked_by: blockers},
         terminal_states
       )
       when is_binary(issue_state) and is_list(blockers) do
    normalize_issue_state(issue_state) == "todo" and
      Enum.any?(blockers, fn
        %{state: blocker_state} when is_binary(blocker_state) ->
          !terminal_issue_state?(blocker_state, terminal_states)

        _ ->
          true
      end)
  end

  defp todo_issue_blocked_by_non_terminal?(_issue, _terminal_states), do: false

  defp terminal_issue_state?(state_name, terminal_states) when is_binary(state_name) do
    MapSet.member?(terminal_states, normalize_issue_state(state_name))
  end

  defp terminal_issue_state?(_state_name, _terminal_states), do: false

  defp active_issue_state?(state_name, active_states) when is_binary(state_name) do
    MapSet.member?(active_states, normalize_issue_state(state_name))
  end

  defp normalize_issue_state(state_name) when is_binary(state_name) do
    String.downcase(String.trim(state_name))
  end

  defp terminal_state_set do
    Config.settings!().tracker.terminal_states
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> MapSet.new()
  end

  defp active_state_set do
    Config.settings!().tracker.active_states
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> MapSet.new()
  end

  defp dispatch_issue(
         %State{} = state,
         issue,
         attempt,
         preferred_worker_host,
         expected_workspace_path,
         affinity_required
       ) do
    case revalidate_issue_for_dispatch(issue, &Tracker.fetch_issue_states_by_ids/1, terminal_state_set()) do
      {:ok, %Issue{} = refreshed_issue} ->
        do_dispatch_issue(
          state,
          refreshed_issue,
          attempt,
          preferred_worker_host,
          expected_workspace_path,
          affinity_required
        )

      {:skip, :missing} ->
        Logger.info("Skipping dispatch; issue no longer active or visible: #{issue_context(issue)}")
        state

      {:skip, %Issue{} = refreshed_issue} ->
        Logger.info("Skipping stale dispatch after issue refresh: #{issue_context(refreshed_issue)} state=#{inspect(refreshed_issue.state)} blocked_by=#{length(refreshed_issue.blocked_by)}")

        state

      {:error, reason} ->
        Logger.warning("Skipping dispatch; issue refresh failed for #{issue_context(issue)}: #{inspect(reason)}")
        state
    end
  end

  defp do_dispatch_issue(
         %State{} = state,
         issue,
         attempt,
         preferred_worker_host,
         expected_workspace_path,
         affinity_required
       ) do
    recipient = self()

    case select_worker_host(state, preferred_worker_host, affinity_required) do
      :affinity_unavailable ->
        Logger.warning("Workspace affinity blocks dispatch for #{issue_context(issue)} worker_host=#{inspect(preferred_worker_host)} workspace_path=#{inspect(expected_workspace_path)}")
        state

      :no_worker_capacity ->
        Logger.debug("No SSH worker slots available for #{issue_context(issue)} preferred_worker_host=#{inspect(preferred_worker_host)}")
        state

      worker_host ->
        if affinity_required and not valid_expected_workspace_path?(expected_workspace_path) do
          Logger.warning("Workspace affinity blocks dispatch for #{issue_context(issue)}: canonical workspace path is missing")
          state
        else
          spawn_issue_on_worker_host(
            state,
            issue,
            attempt,
            recipient,
            worker_host,
            expected_workspace_path
          )
        end
    end
  end

  defp spawn_issue_on_worker_host(
         %State{} = state,
         issue,
         attempt,
         recipient,
         worker_host,
         expected_workspace_path
       ) do
    case Config.validate_runtime_capabilities() do
      :ok ->
        claim_and_start_issue(
          state,
          issue,
          attempt,
          recipient,
          worker_host,
          expected_workspace_path
        )

      {:error, {:missing_required_dynamic_tools, tools}} ->
        Logger.error("Runtime capability preflight blocked claim for #{issue_context(issue)}: missing_required_dynamic_tools=#{Enum.join(tools, ",")}")
        state

      {:error, reason} ->
        Logger.error("Runtime capability preflight failed for #{issue_context(issue)}: #{inspect(reason)}")
        state
    end
  end

  defp claim_and_start_issue(
         %State{} = state,
         issue,
         attempt,
         recipient,
         worker_host,
         expected_workspace_path
       ) do
    run_id = RunLedger.new_id("run")
    normalized_attempt = normalize_retry_attempt(attempt)

    claim_event = %{
      transition: "run_claimed",
      stage: "claimed",
      run_id: run_id,
      issue_id: issue.id,
      issue_identifier: issue.identifier,
      attempt: normalized_attempt,
      worker_host: worker_host,
      workspace_path: expected_workspace_path
    }

    case append_run_event(state, claim_event) do
      :ok ->
        state = consume_dispatch_queue(state, issue.id)

        start_issue_task(
          state,
          issue,
          attempt,
          recipient,
          worker_host,
          expected_workspace_path,
          run_id,
          normalized_attempt
        )

      {:error, reason} ->
        Logger.error("Unable to record durable claim for #{issue_context(issue)}: #{inspect(reason)}")
        state
    end
  end

  defp start_issue_task(
         state,
         issue,
         attempt,
         recipient,
         worker_host,
         expected_workspace_path,
         run_id,
         normalized_attempt
       ) do
    run_budget = RunBudget.from_agent_config(Config.settings!().agent)

    case Task.Supervisor.start_child(SymphonyElixir.TaskSupervisor, fn ->
           AgentRunner.run(issue, recipient,
             attempt: attempt,
             worker_host: worker_host,
             expected_workspace_path: expected_workspace_path,
             run_id: run_id,
             runner_generation: state.runner_generation,
             stage: "running",
             max_turns: run_budget.max_turns
           )
         end) do
      {:ok, pid} ->
        ref = Process.monitor(pid)

        Logger.info("Dispatching issue to agent: #{issue_context(issue)} pid=#{inspect(pid)} attempt=#{inspect(attempt)} worker_host=#{worker_host || "local"}")

        running_entry = %{
          run_id: run_id,
          pid: pid,
          ref: ref,
          identifier: issue.identifier,
          issue: issue,
          worker_host: worker_host,
          workspace_path: expected_workspace_path,
          session_id: nil,
          session_title: nil,
          resolved_model: nil,
          reasoning_effort: nil,
          model_catalog_source: nil,
          model_catalog: nil,
          last_codex_message: nil,
          last_codex_timestamp: nil,
          last_codex_event: nil,
          codex_app_server_pid: nil,
          codex_input_tokens: 0,
          codex_output_tokens: 0,
          codex_total_tokens: 0,
          codex_last_reported_input_tokens: 0,
          codex_last_reported_output_tokens: 0,
          codex_last_reported_total_tokens: 0,
          codex_token_telemetry_observed: false,
          turn_count: 0,
          run_budget: run_budget,
          run_budget_timer_ref: nil,
          retry_attempt: normalized_attempt,
          started_at: DateTime.utc_now()
        }

        case append_run_event(
               state,
               run_event(running_entry, "run_started", "running")
             ) do
          :ok ->
            running_entry = arm_run_budget_timer(running_entry)
            running = Map.put(state.running, issue.id, running_entry)

            %{
              state
              | running: running,
                claimed: MapSet.put(state.claimed, issue.id),
                recovered_attempts: Map.delete(state.recovered_attempts, issue.id),
                recovered_dispatches: Map.delete(state.recovered_dispatches, issue.id),
                queued_resumes: Map.delete(state.queued_resumes, issue.id),
                retry_attempts: Map.delete(state.retry_attempts, issue.id)
            }
            |> initialize_operator_cursor(issue.id, running_entry.started_at)

          {:error, reason} ->
            Logger.error("Unable to record durable run start for #{issue_context(issue)}: #{inspect(reason)}")
            terminate_task(pid)
            Process.demonitor(ref, [:flush])
            state
        end

      {:error, reason} ->
        Logger.error("Unable to spawn agent for #{issue_context(issue)}: #{inspect(reason)}")
        next_attempt = if is_integer(attempt), do: attempt + 1, else: nil

        failed_entry = %{
          run_id: run_id,
          pid: nil,
          ref: nil,
          identifier: issue.identifier,
          issue: issue,
          worker_host: worker_host,
          workspace_path: expected_workspace_path,
          retry_attempt: normalized_attempt,
          started_at: DateTime.utc_now()
        }

        pending = %{
          transition: "run_failed",
          terminal_reason: "spawn_failed",
          action:
            {:retry, next_attempt,
             %{
               identifier: issue.identifier,
               error: "failed to spawn agent: #{inspect(reason)}",
               previous_run_id: run_id,
               previous_attempt: normalized_attempt,
               worker_host: worker_host,
               workspace_path: expected_workspace_path
             }}
        }

        state = %{state | claimed: MapSet.put(state.claimed, issue.id)}
        persist_or_block_terminal(state, issue.id, failed_entry, pending)
    end
  end

  defp revalidate_issue_for_dispatch(%Issue{id: issue_id}, issue_fetcher, terminal_states)
       when is_binary(issue_id) and is_function(issue_fetcher, 1) do
    case issue_fetcher.([issue_id]) do
      {:ok, [%Issue{} = refreshed_issue | _]} ->
        if retry_candidate_issue?(refreshed_issue, terminal_states) do
          {:ok, refreshed_issue}
        else
          {:skip, refreshed_issue}
        end

      {:ok, []} ->
        {:skip, :missing}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp revalidate_issue_for_dispatch(issue, _issue_fetcher, _terminal_states), do: {:ok, issue}

  defp complete_issue(%State{} = state, issue_id) do
    %{
      state
      | completed: MapSet.put(state.completed, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }
  end

  defp schedule_issue_retry(%State{} = state, issue_id, attempt, metadata)
       when is_binary(issue_id) and is_map(metadata) do
    previous_retry = Map.get(state.retry_attempts, issue_id, %{attempt: 0})
    next_attempt = if is_integer(attempt), do: attempt, else: previous_retry.attempt + 1
    delay_ms = retry_delay(next_attempt, metadata)
    old_timer = Map.get(previous_retry, :timer_ref)
    retry_token = make_ref()
    due_at_ms = System.monotonic_time(:millisecond) + delay_ms
    identifier = pick_retry_identifier(issue_id, previous_retry, metadata)

    error =
      case pick_retry_error(previous_retry, metadata) do
        nil -> nil
        value -> ObservabilitySanitizer.retry_error_code(value)
      end

    worker_host = pick_retry_worker_host(previous_retry, metadata)
    workspace_path = pick_retry_workspace_path(previous_retry, metadata)

    previous_run_id =
      Map.get(metadata, :previous_run_id) || Map.get(previous_retry, :previous_run_id)

    previous_attempt =
      Map.get(metadata, :previous_attempt, Map.get(previous_retry, :previous_attempt, 0))

    if is_reference(old_timer) do
      Process.cancel_timer(old_timer)
    end

    timer_ref = Process.send_after(self(), {:retry_issue, issue_id, retry_token}, delay_ms)

    error_suffix = if is_binary(error), do: " error=#{error}", else: ""

    Logger.warning("Retrying issue_id=#{issue_id} issue_identifier=#{identifier} in #{delay_ms}ms (attempt #{next_attempt})#{error_suffix}")

    log_run_event_result(
      append_run_event(state, %{
        transition: "retry_scheduled",
        stage: "retry_queued",
        run_id: previous_run_id,
        issue_id: issue_id,
        issue_identifier: identifier,
        attempt: previous_attempt,
        next_attempt: next_attempt,
        worker_host: worker_host,
        workspace_path: workspace_path
      }),
      issue_id
    )

    %{
      state
      | retry_attempts:
          Map.put(state.retry_attempts, issue_id, %{
            attempt: next_attempt,
            timer_ref: timer_ref,
            retry_token: retry_token,
            due_at_ms: due_at_ms,
            identifier: identifier,
            error: error,
            previous_run_id: previous_run_id,
            previous_attempt: previous_attempt,
            worker_host: worker_host,
            workspace_path: workspace_path
          })
    }
  end

  defp pop_retry_attempt_state(%State{} = state, issue_id, retry_token) when is_reference(retry_token) do
    case Map.get(state.retry_attempts, issue_id) do
      %{attempt: attempt, retry_token: ^retry_token} = retry_entry ->
        metadata = %{
          identifier: Map.get(retry_entry, :identifier),
          error: Map.get(retry_entry, :error),
          previous_run_id: Map.get(retry_entry, :previous_run_id),
          previous_attempt: Map.get(retry_entry, :previous_attempt),
          worker_host: Map.get(retry_entry, :worker_host),
          workspace_path: Map.get(retry_entry, :workspace_path)
        }

        {:ok, attempt, metadata, %{state | retry_attempts: Map.delete(state.retry_attempts, issue_id)}}

      _ ->
        :missing
    end
  end

  defp defer_retry_while_paused(%State{} = state, issue_id, retry_token)
       when is_binary(issue_id) and is_reference(retry_token) do
    case Map.get(state.retry_attempts, issue_id) do
      %{retry_token: ^retry_token} = retry ->
        delay_ms = max(state.poll_interval_ms || 1_000, 1_000)
        next_retry_token = make_ref()

        updated_retry = %{
          retry
          | timer_ref:
              Process.send_after(
                self(),
                {:retry_issue, issue_id, next_retry_token},
                delay_ms
              ),
            retry_token: next_retry_token,
            due_at_ms: System.monotonic_time(:millisecond) + delay_ms
        }

        %{state | retry_attempts: Map.put(state.retry_attempts, issue_id, updated_retry)}

      _retry ->
        state
    end
  end

  defp defer_retry_while_paused(state, _issue_id, _retry_token), do: state

  defp wake_paused_retries(%State{} = state) do
    now_ms = System.monotonic_time(:millisecond)

    retry_attempts =
      Map.new(state.retry_attempts, fn {issue_id, retry} ->
        if is_reference(retry.timer_ref), do: Process.cancel_timer(retry.timer_ref)
        retry_token = make_ref()

        {
          issue_id,
          %{
            retry
            | timer_ref: Process.send_after(self(), {:retry_issue, issue_id, retry_token}, 0),
              retry_token: retry_token,
              due_at_ms: now_ms
          }
        }
      end)

    %{state | retry_attempts: retry_attempts}
  end

  defp handle_retry_issue(%State{} = state, issue_id, attempt, metadata) do
    case Tracker.fetch_candidate_issues() do
      {:ok, issues} ->
        issues
        |> find_issue_by_id(issue_id)
        |> handle_retry_issue_lookup(state, issue_id, attempt, metadata)

      {:error, reason} ->
        Logger.warning("Retry poll failed for issue_id=#{issue_id} issue_identifier=#{metadata[:identifier] || issue_id}: #{inspect(reason)}")

        {:noreply,
         schedule_issue_retry(
           state,
           issue_id,
           attempt + 1,
           Map.merge(metadata, %{error: "retry poll failed: #{inspect(reason)}"})
         )}
    end
  end

  defp handle_retry_issue_lookup(%Issue{} = issue, state, issue_id, attempt, metadata) do
    terminal_states = terminal_state_set()

    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Issue state is terminal: issue_id=#{issue_id} issue_identifier=#{issue.identifier} state=#{issue.state}; removing associated workspace")

        cleanup_issue_workspace(issue.identifier, metadata[:worker_host])
        {:noreply, release_issue_claim(state, issue_id)}

      retry_candidate_issue?(issue, terminal_states) ->
        handle_active_retry(state, issue, attempt, metadata)

      true ->
        Logger.debug("Issue left active states, removing claim issue_id=#{issue_id} issue_identifier=#{issue.identifier}")

        {:noreply, release_issue_claim(state, issue_id)}
    end
  end

  defp handle_retry_issue_lookup(nil, state, issue_id, _attempt, _metadata) do
    Logger.debug("Issue no longer visible, removing claim issue_id=#{issue_id}")
    {:noreply, release_issue_claim(state, issue_id)}
  end

  defp cleanup_issue_workspace(identifier, worker_host \\ nil)

  defp cleanup_issue_workspace(identifier, worker_host) when is_binary(identifier) do
    Workspace.remove_issue_workspaces(identifier, worker_host)
  end

  defp cleanup_issue_workspace(_identifier, _worker_host), do: :ok

  defp run_terminal_workspace_cleanup do
    case Tracker.fetch_issues_by_states(Config.settings!().tracker.terminal_states) do
      {:ok, issues} ->
        issues
        |> Enum.each(fn
          %Issue{identifier: identifier} when is_binary(identifier) ->
            cleanup_issue_workspace(identifier)

          _ ->
            :ok
        end)

      {:error, reason} ->
        Logger.warning("Skipping startup terminal workspace cleanup; failed to fetch terminal issues: #{inspect(reason)}")
    end
  end

  defp notify_dashboard do
    StatusDashboard.notify_update()
  end

  defp handle_active_retry(state, issue, attempt, metadata) do
    affinity_required = valid_expected_workspace_path?(metadata[:workspace_path])

    if retry_candidate_issue?(issue, terminal_state_set()) and
         dispatch_slots_available?(issue, state) and
         worker_slots_available?(state, metadata[:worker_host], affinity_required) do
      {:noreply,
       dispatch_issue(
         state,
         issue,
         attempt,
         metadata[:worker_host],
         metadata[:workspace_path],
         affinity_required
       )}
    else
      Logger.debug("No available slots for retrying #{issue_context(issue)}; retrying again")

      {:noreply,
       schedule_issue_retry(
         state,
         issue.id,
         attempt + 1,
         Map.merge(metadata, %{
           identifier: issue.identifier,
           error: "no available orchestrator slots"
         })
       )}
    end
  end

  defp release_issue_claim(%State{} = state, issue_id) do
    %{state | claimed: MapSet.delete(state.claimed, issue_id)}
  end

  defp retry_delay(attempt, metadata) when is_integer(attempt) and attempt > 0 and is_map(metadata) do
    if metadata[:delay_type] == :continuation and attempt == 1 do
      @continuation_retry_delay_ms
    else
      failure_retry_delay(attempt)
    end
  end

  defp failure_retry_delay(attempt) do
    max_delay_power = min(attempt - 1, 10)
    min(@failure_retry_base_ms * (1 <<< max_delay_power), Config.settings!().agent.max_retry_backoff_ms)
  end

  defp normalize_retry_attempt(attempt) when is_integer(attempt) and attempt > 0, do: attempt
  defp normalize_retry_attempt(_attempt), do: 0

  defp dispatch_context(%State{} = state, issue_id) do
    recovered = Map.get(state.recovered_dispatches, issue_id)
    queued = Map.get(state.queued_resumes, issue_id)

    [recovered, queued]
    |> Enum.filter(&is_map/1)
    |> Enum.max_by(&Map.get(&1, :attempt, 0), fn -> %{} end)
    |> then(fn dispatch ->
      %{
        attempt: Map.get(dispatch, :attempt),
        worker_host: Map.get(dispatch, :worker_host),
        workspace_path: Map.get(dispatch, :workspace_path),
        affinity_required: map_size(dispatch) > 0
      }
    end)
  end

  defp consume_dispatch_queue(%State{} = state, issue_id) do
    %{
      state
      | claimed: MapSet.put(state.claimed, issue_id),
        recovered_attempts: Map.delete(state.recovered_attempts, issue_id),
        recovered_dispatches: Map.delete(state.recovered_dispatches, issue_id),
        queued_resumes: Map.delete(state.queued_resumes, issue_id)
    }
  end

  defp next_retry_attempt_from_running(running_entry) do
    case Map.get(running_entry, :retry_attempt) do
      attempt when is_integer(attempt) and attempt > 0 -> attempt + 1
      _ -> nil
    end
  end

  defp pick_retry_identifier(issue_id, previous_retry, metadata) do
    metadata[:identifier] || Map.get(previous_retry, :identifier) || issue_id
  end

  defp pick_retry_error(previous_retry, metadata) do
    metadata[:error] || Map.get(previous_retry, :error)
  end

  defp pick_retry_worker_host(previous_retry, metadata) do
    metadata[:worker_host] || Map.get(previous_retry, :worker_host)
  end

  defp pick_retry_workspace_path(previous_retry, metadata) do
    metadata[:workspace_path] || Map.get(previous_retry, :workspace_path)
  end

  defp maybe_put_runtime_value(running_entry, _key, nil), do: running_entry

  defp maybe_put_runtime_value(running_entry, key, value) when is_map(running_entry) do
    Map.put(running_entry, key, value)
  end

  defp runtime_info_matches_run?(runtime_info, running_entry) do
    incoming_run_id = Map.get(runtime_info, :run_id)
    active_run_id = Map.get(running_entry, :run_id)
    is_nil(incoming_run_id) or is_nil(active_run_id) or incoming_run_id == active_run_id
  end

  defp select_worker_host(%State{} = state, preferred_worker_host) do
    select_worker_host(state, preferred_worker_host, false)
  end

  defp select_worker_host(%State{} = state, preferred_worker_host, false) do
    case Config.settings!().worker.ssh_hosts do
      [] ->
        nil

      hosts ->
        available_hosts = Enum.filter(hosts, &worker_host_slots_available?(state, &1))

        cond do
          available_hosts == [] ->
            :no_worker_capacity

          preferred_worker_host_available?(preferred_worker_host, available_hosts) ->
            preferred_worker_host

          true ->
            least_loaded_worker_host(state, available_hosts)
        end
    end
  end

  defp select_worker_host(%State{} = state, preferred_worker_host, true) do
    case Config.settings!().worker.ssh_hosts do
      [] when is_binary(preferred_worker_host) ->
        :affinity_unavailable

      [] ->
        nil

      hosts ->
        select_affinity_worker_host(state, preferred_worker_host, hosts)
    end
  end

  defp select_affinity_worker_host(state, preferred_worker_host, hosts) do
    cond do
      not preferred_worker_host_available?(preferred_worker_host, hosts) ->
        :affinity_unavailable

      not worker_host_slots_available?(state, preferred_worker_host) ->
        :no_worker_capacity

      true ->
        preferred_worker_host
    end
  end

  defp preferred_worker_host_available?(preferred_worker_host, hosts)
       when is_binary(preferred_worker_host) and is_list(hosts) do
    preferred_worker_host != "" and preferred_worker_host in hosts
  end

  defp preferred_worker_host_available?(_preferred_worker_host, _hosts), do: false

  defp least_loaded_worker_host(%State{} = state, hosts) when is_list(hosts) do
    hosts
    |> Enum.with_index()
    |> Enum.min_by(fn {host, index} ->
      {running_worker_host_count(state.running, host), index}
    end)
    |> elem(0)
  end

  defp running_worker_host_count(running, worker_host) when is_map(running) and is_binary(worker_host) do
    Enum.count(running, fn
      {_issue_id, %{worker_host: ^worker_host}} -> true
      _ -> false
    end)
  end

  defp worker_slots_available?(%State{} = state) do
    select_worker_host(state, nil) != :no_worker_capacity
  end

  defp worker_slots_available?(%State{} = state, preferred_worker_host, affinity_required) do
    select_worker_host(state, preferred_worker_host, affinity_required) not in [
      :no_worker_capacity,
      :affinity_unavailable
    ]
  end

  defp valid_expected_workspace_path?(workspace_path) when is_binary(workspace_path) do
    String.trim(workspace_path) != "" and
      not String.contains?(workspace_path, ["\n", "\r", <<0>>])
  end

  defp valid_expected_workspace_path?(_workspace_path), do: false

  defp worker_host_slots_available?(%State{} = state, worker_host) when is_binary(worker_host) do
    case Config.settings!().worker.max_concurrent_agents_per_host do
      limit when is_integer(limit) and limit > 0 ->
        running_worker_host_count(state.running, worker_host) < limit

      _ ->
        true
    end
  end

  defp find_issue_by_id(issues, issue_id) when is_binary(issue_id) do
    Enum.find(issues, fn
      %Issue{id: ^issue_id} ->
        true

      _ ->
        false
    end)
  end

  defp find_issue_id_for_ref(running, ref) do
    running
    |> Enum.find_value(fn {issue_id, %{ref: running_ref}} ->
      if running_ref == ref, do: issue_id
    end)
  end

  defp running_entry_session_id(%{session_id: session_id}) when is_binary(session_id),
    do: session_id

  defp running_entry_session_id(_running_entry), do: "n/a"

  defp issue_context(%Issue{id: issue_id, identifier: identifier}) do
    "issue_id=#{issue_id} issue_identifier=#{identifier}"
  end

  defp available_slots(%State{} = state) do
    max(
      (state.max_concurrent_agents || Config.settings!().agent.max_concurrent_agents) -
        map_size(state.running),
      0
    )
  end

  @spec request_refresh() :: map() | :unavailable
  def request_refresh do
    request_refresh(__MODULE__)
  end

  @spec request_refresh(GenServer.server()) :: map() | :unavailable
  def request_refresh(server) do
    if Process.whereis(server) do
      GenServer.call(server, :request_refresh)
    else
      :unavailable
    end
  end

  @spec set_dispatch_paused(boolean()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def set_dispatch_paused(paused) when is_boolean(paused) do
    set_dispatch_paused(__MODULE__, paused)
  end

  @spec set_dispatch_paused(GenServer.server(), boolean()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def set_dispatch_paused(server, paused) when is_boolean(paused) do
    if GenServer.whereis(server) do
      GenServer.call(server, {:set_dispatch_paused, paused})
    else
      :unavailable
    end
  end

  @spec park_issue(String.t(), String.t()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def park_issue(issue_id, reason), do: park_issue(__MODULE__, issue_id, reason)

  @spec park_issue(GenServer.server(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def park_issue(server, issue_id, reason) do
    if GenServer.whereis(server) do
      GenServer.call(server, {:park_issue, issue_id, reason})
    else
      :unavailable
    end
  end

  @spec resolve_wait(String.t(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def resolve_wait(issue_id, wait_id, action),
    do: resolve_wait(__MODULE__, issue_id, wait_id, action)

  @spec resolve_wait(GenServer.server(), String.t(), String.t(), String.t()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def resolve_wait(server, issue_id, wait_id, action) do
    if GenServer.whereis(server) do
      GenServer.call(server, {:resolve_wait, issue_id, wait_id, action})
    else
      :unavailable
    end
  end

  @spec snapshot() :: map() | :timeout | :unavailable
  def snapshot, do: snapshot(__MODULE__, 15_000)

  @spec snapshot(GenServer.server(), timeout()) :: map() | :timeout | :unavailable
  def snapshot(server, timeout) do
    if Process.whereis(server) do
      try do
        GenServer.call(server, :snapshot, timeout)
      catch
        :exit, {:timeout, _} -> :timeout
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @impl true
  def handle_call({:park_issue, issue_id, reason}, _from, state) do
    case Map.get(state.running, issue_id) do
      %{issue: %Issue{} = issue} ->
        updated_state = park_running_issue(state, issue, reason)

        case Map.get(updated_state.parked, issue_id) do
          nil -> {:reply, {:error, :park_failed}, state}
          wait -> {:reply, {:ok, wait}, updated_state}
        end

      _ ->
        {:reply, {:error, :issue_not_running}, state}
    end
  end

  def handle_call({:resolve_wait, issue_id, wait_id, action}, _from, state) do
    case Map.get(state.parked, issue_id) do
      nil ->
        {:reply, {:error, :wait_not_found}, state}

      %{wait_id: stored_wait_id} when stored_wait_id != wait_id ->
        {:reply, {:error, :wait_id_mismatch}, state}

      wait ->
        resolve_operator_wait(state, wait, action)
    end
  end

  def handle_call({:set_dispatch_paused, paused}, _from, state)
      when is_boolean(paused) do
    if state.dispatch_paused == paused do
      {:reply,
       {:ok,
        %{
          dispatch_paused: paused,
          changed: false,
          requested_at: DateTime.utc_now()
        }}, state}
    else
      persist_dispatch_pause(state, paused)
    end
  end

  def handle_call(:snapshot, _from, state) do
    state = refresh_runtime_config(state)
    now = DateTime.utc_now()
    now_ms = System.monotonic_time(:millisecond)

    running =
      state.running
      |> Enum.map(fn {issue_id, metadata} ->
        terminal_pending = Map.get(metadata, :terminal_pending)
        issue = Map.get(metadata, :issue)

        %{
          issue_id: issue_id,
          run_id: Map.get(metadata, :run_id),
          attempt: Map.get(metadata, :retry_attempt, 0),
          stage: if(is_map(terminal_pending), do: "terminal_pending", else: "running"),
          terminal_transition: if(is_map(terminal_pending), do: terminal_pending.transition),
          terminal_reason: if(is_map(terminal_pending), do: terminal_pending.terminal_reason),
          identifier: Map.get(metadata, :identifier),
          state: if(is_map(issue), do: Map.get(issue, :state)),
          worker_host: Map.get(metadata, :worker_host),
          workspace_path: Map.get(metadata, :workspace_path),
          session_id: Map.get(metadata, :session_id),
          session_title: Map.get(metadata, :session_title),
          resolved_model: Map.get(metadata, :resolved_model),
          reasoning_effort: Map.get(metadata, :reasoning_effort),
          model_catalog_source: Map.get(metadata, :model_catalog_source),
          model_catalog: Map.get(metadata, :model_catalog),
          codex_app_server_pid: Map.get(metadata, :codex_app_server_pid),
          codex_input_tokens: Map.get(metadata, :codex_input_tokens, 0),
          codex_output_tokens: Map.get(metadata, :codex_output_tokens, 0),
          codex_total_tokens: Map.get(metadata, :codex_total_tokens, 0),
          turn_count: Map.get(metadata, :turn_count, 0),
          budget: run_budget_snapshot(metadata, now),
          started_at: Map.get(metadata, :started_at),
          last_codex_timestamp: Map.get(metadata, :last_codex_timestamp),
          last_codex_message: Map.get(metadata, :last_codex_message),
          last_codex_event: Map.get(metadata, :last_codex_event),
          runtime_seconds: running_seconds(Map.get(metadata, :started_at), now)
        }
      end)

    retrying =
      state.retry_attempts
      |> Enum.map(fn {issue_id, %{attempt: attempt, due_at_ms: due_at_ms} = retry} ->
        %{
          issue_id: issue_id,
          run_id: Map.get(retry, :previous_run_id),
          attempt: attempt,
          stage: "retry_queued",
          due_in_ms: max(0, due_at_ms - now_ms),
          identifier: Map.get(retry, :identifier),
          error: Map.get(retry, :error),
          worker_host: Map.get(retry, :worker_host),
          workspace_path: Map.get(retry, :workspace_path)
        }
      end)
      |> Enum.concat(recovered_dispatch_snapshot_rows(state.recovered_dispatches))
      |> Enum.concat(queued_resume_snapshot_rows(state.queued_resumes))

    parked =
      state.parked
      |> Enum.map(fn {_issue_id, wait} ->
        %{
          issue_id: wait.issue_id,
          run_id: wait.run_id,
          attempt: wait.attempt,
          stage: wait.stage,
          identifier: wait.identifier,
          wait_id: wait.wait_id,
          reason: wait.reason,
          allowed_actions: wait.allowed_actions,
          tracker_state: wait.tracker_state,
          terminal_reason: Map.get(wait, :terminal_reason),
          worker_host: Map.get(wait, :worker_host),
          workspace_path: Map.get(wait, :workspace_path),
          parked_at: wait.parked_at
        }
      end)

    {:reply,
     %{
       runner_generation: state.runner_generation,
       control: %{
         dispatch_paused: state.dispatch_paused
       },
       capabilities: capability_snapshot(),
       running: running,
       retrying: retrying,
       parked: parked,
       codex_totals: state.codex_totals,
       rate_limits: Map.get(state, :codex_rate_limits),
       polling: %{
         checking?: state.poll_check_in_progress == true,
         next_poll_in_ms: next_poll_in_ms(state.next_poll_due_at_ms, now_ms),
         poll_interval_ms: state.poll_interval_ms
       }
     }, state}
  end

  def handle_call(:request_refresh, _from, state) do
    now_ms = System.monotonic_time(:millisecond)
    already_due? = is_integer(state.next_poll_due_at_ms) and state.next_poll_due_at_ms <= now_ms
    coalesced = state.poll_check_in_progress == true or already_due?
    state = if coalesced, do: state, else: schedule_tick(state, 0)

    {:reply,
     %{
       queued: true,
       coalesced: coalesced,
       requested_at: DateTime.utc_now(),
       operations: ["poll", "reconcile"]
     }, state}
  end

  defp capability_snapshot do
    codex = Config.settings!().codex

    %{
      dynamic_tools: codex.dynamic_tool_allowlist,
      mcp_tool_auto_approve: codex.mcp_tool_auto_approve_allowlist,
      mcp_elicitation_auto_approve: codex.mcp_elicitation_auto_approve_allowlist
    }
  end

  defp integrate_codex_update(running_entry, %{event: event, timestamp: timestamp} = update) do
    token_delta = extract_token_delta(running_entry, update)
    codex_input_tokens = Map.get(running_entry, :codex_input_tokens, 0)
    codex_output_tokens = Map.get(running_entry, :codex_output_tokens, 0)
    codex_total_tokens = Map.get(running_entry, :codex_total_tokens, 0)
    codex_app_server_pid = Map.get(running_entry, :codex_app_server_pid)
    last_reported_input = Map.get(running_entry, :codex_last_reported_input_tokens, 0)
    last_reported_output = Map.get(running_entry, :codex_last_reported_output_tokens, 0)
    last_reported_total = Map.get(running_entry, :codex_last_reported_total_tokens, 0)
    turn_count = Map.get(running_entry, :turn_count, 0)

    {
      Map.merge(running_entry, %{
        last_codex_timestamp: timestamp,
        last_codex_message: summarize_codex_update(update),
        session_id: session_id_for_update(running_entry.session_id, update),
        session_title: session_title_for_update(Map.get(running_entry, :session_title), update),
        resolved_model: resolved_model_for_update(Map.get(running_entry, :resolved_model), update),
        reasoning_effort: reasoning_effort_for_update(Map.get(running_entry, :reasoning_effort), update),
        model_catalog_source: model_catalog_source_for_update(Map.get(running_entry, :model_catalog_source), update),
        model_catalog: model_catalog_for_update(Map.get(running_entry, :model_catalog), update),
        last_codex_event: event,
        codex_app_server_pid: codex_app_server_pid_for_update(codex_app_server_pid, update),
        codex_input_tokens: codex_input_tokens + token_delta.input_tokens,
        codex_output_tokens: codex_output_tokens + token_delta.output_tokens,
        codex_total_tokens: codex_total_tokens + token_delta.total_tokens,
        codex_last_reported_input_tokens: max(last_reported_input, token_delta.input_reported),
        codex_last_reported_output_tokens: max(last_reported_output, token_delta.output_reported),
        codex_last_reported_total_tokens: max(last_reported_total, token_delta.total_reported),
        codex_token_telemetry_observed:
          Map.get(running_entry, :codex_token_telemetry_observed, false) or
            token_delta.telemetry_observed,
        turn_count: turn_count_for_update(turn_count, running_entry.session_id, update)
      }),
      token_delta
    }
  end

  defp maybe_record_model_resolution(
         %State{} = state,
         previous_running_entry,
         %{resolved_model: resolved_model} = updated_running_entry
       )
       when is_binary(resolved_model) do
    if is_binary(Map.get(previous_running_entry, :resolved_model)) do
      state
    else
      record_run_event(state, updated_running_entry, "model_resolved", "running")
    end
  end

  defp maybe_record_model_resolution(%State{} = state, _previous, _updated), do: state

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid})
       when is_binary(pid),
       do: pid

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid})
       when is_integer(pid),
       do: Integer.to_string(pid)

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid}) when is_list(pid),
    do: to_string(pid)

  defp codex_app_server_pid_for_update(existing, _update), do: existing

  defp session_id_for_update(_existing, %{session_id: session_id}) when is_binary(session_id),
    do: session_id

  defp session_id_for_update(existing, _update), do: existing

  defp session_title_for_update(_existing, %{session_title: session_title}) when is_binary(session_title),
    do: session_title

  defp session_title_for_update(existing, _update), do: existing

  defp resolved_model_for_update(_existing, %{resolved_model: model}) when is_binary(model), do: model
  defp resolved_model_for_update(existing, _update), do: existing

  defp reasoning_effort_for_update(_existing, %{reasoning_effort: effort}) when is_binary(effort),
    do: effort

  defp reasoning_effort_for_update(existing, _update), do: existing

  defp model_catalog_source_for_update(_existing, %{model_catalog_source: source})
       when is_binary(source),
       do: source

  defp model_catalog_source_for_update(existing, _update), do: existing

  defp model_catalog_for_update(_existing, %{model_catalog: catalog}) when is_map(catalog), do: catalog
  defp model_catalog_for_update(existing, _update), do: existing

  defp turn_count_for_update(existing_count, existing_session_id, %{
         event: :session_started,
         session_id: session_id
       })
       when is_integer(existing_count) and is_binary(session_id) do
    if session_id == existing_session_id do
      existing_count
    else
      existing_count + 1
    end
  end

  defp turn_count_for_update(existing_count, _existing_session_id, _update)
       when is_integer(existing_count),
       do: existing_count

  defp turn_count_for_update(_existing_count, _existing_session_id, _update), do: 0

  defp summarize_codex_update(update) do
    payload = update[:payload] || update[:raw]

    message =
      %{}
      |> maybe_put_codex_summary_value(
        :method,
        payload
        |> codex_payload_method()
        |> ObservabilitySanitizer.protocol_method()
      )
      |> maybe_put_codex_summary_value(
        :error_code,
        codex_update_error_code(update)
      )

    %{
      event: update[:event],
      message: message,
      timestamp: update[:timestamp]
    }
  end

  defp codex_payload_method(%{} = payload), do: Map.get(payload, :method) || Map.get(payload, "method")
  defp codex_payload_method(_payload), do: nil

  defp codex_update_error_code(%{event: event} = update)
       when event in [:app_server_error, :terminal_protocol_error, :turn_failed, :turn_ended_with_error, :startup_failed] do
    ObservabilitySanitizer.error_code(update, "runtime_error")
  end

  defp codex_update_error_code(_update), do: nil

  defp maybe_put_codex_summary_value(summary, _key, nil), do: summary
  defp maybe_put_codex_summary_value(summary, key, value), do: Map.put(summary, key, value)

  defp schedule_tick(%State{} = state, delay_ms) when is_integer(delay_ms) and delay_ms >= 0 do
    if is_reference(state.tick_timer_ref) do
      Process.cancel_timer(state.tick_timer_ref)
    end

    tick_token = make_ref()
    timer_ref = Process.send_after(self(), {:tick, tick_token}, delay_ms)

    %{
      state
      | tick_timer_ref: timer_ref,
        tick_token: tick_token,
        next_poll_due_at_ms: System.monotonic_time(:millisecond) + delay_ms
    }
  end

  defp schedule_poll_cycle_start do
    :timer.send_after(@poll_transition_render_delay_ms, self(), :run_poll_cycle)
    :ok
  end

  defp next_poll_in_ms(nil, _now_ms), do: nil

  defp next_poll_in_ms(next_poll_due_at_ms, now_ms) when is_integer(next_poll_due_at_ms) do
    max(0, next_poll_due_at_ms - now_ms)
  end

  defp pop_running_entry(state, issue_id) do
    running_entry = Map.get(state.running, issue_id)
    cancel_run_budget_timer(running_entry)
    {running_entry, %{state | running: Map.delete(state.running, issue_id)}}
  end

  defp finish_worker_run(state, issue_id, running_entry, :normal, session_id) do
    Logger.info("Agent task completed for issue_id=#{issue_id} session_id=#{session_id}; persisting completion before continuation")

    pending = %{
      transition: "run_completed",
      terminal_reason: "worker_completed",
      action:
        {:continuation, max(Map.get(running_entry, :retry_attempt, 0) + 1, 1),
         %{
           identifier: running_entry.identifier,
           delay_type: :continuation,
           previous_run_id: Map.get(running_entry, :run_id),
           previous_attempt: Map.get(running_entry, :retry_attempt, 0),
           worker_host: Map.get(running_entry, :worker_host),
           workspace_path: Map.get(running_entry, :workspace_path)
         }}
    }

    persist_or_block_terminal(state, issue_id, running_entry, pending)
  end

  defp finish_worker_run(state, issue_id, running_entry, reason, session_id) do
    error_code = ObservabilitySanitizer.error_code(reason, "agent_exit")

    Logger.warning("Agent task exited for issue_id=#{issue_id} session_id=#{session_id} error_code=#{error_code}; persisting failure before retry")

    pending = %{
      transition: "run_failed",
      terminal_reason: "worker_exit",
      action:
        {:retry, next_retry_attempt_from_running(running_entry),
         %{
           identifier: running_entry.identifier,
           error: "agent exited: #{inspect(reason)}",
           previous_run_id: Map.get(running_entry, :run_id),
           previous_attempt: Map.get(running_entry, :retry_attempt, 0),
           worker_host: Map.get(running_entry, :worker_host),
           workspace_path: Map.get(running_entry, :workspace_path)
         }}
    }

    persist_or_block_terminal(state, issue_id, running_entry, pending)
  end

  defp persist_or_block_terminal(state, issue_id, running_entry, pending) do
    event =
      run_event(
        running_entry,
        pending.transition,
        "released",
        terminal_reason: pending.terminal_reason
      )

    case append_run_event(state, event) do
      :ok ->
        apply_terminal_success(state, issue_id, running_entry, pending.action)

      {:error, reason} ->
        Logger.error("Failed to append terminal Symphony run ledger event issue_id=#{issue_id} transition=#{pending.transition}: #{inspect(reason)}; retaining blocked claim")

        block_terminal_transition(state, issue_id, running_entry, pending)
    end
  end

  defp block_terminal_transition(state, issue_id, running_entry, pending) do
    if not running_entry_alive?(running_entry) do
      cancel_run_budget_timer(running_entry)
    end

    blocked_entry =
      running_entry
      |> Map.put(:terminal_pending, Map.put(pending, :failed_at, DateTime.utc_now()))
      |> maybe_clear_terminal_timer()

    %{
      state
      | running: Map.put(state.running, issue_id, blocked_entry),
        claimed: MapSet.put(state.claimed, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }
  end

  defp mark_pending_worker_exited(state, issue_id, running_entry) do
    cancel_run_budget_timer(running_entry)

    updated_entry =
      running_entry
      |> Map.put(:worker_exited_while_terminal_pending, true)
      |> Map.put(:worker_exit_established, true)
      |> Map.put(:run_budget_timer_ref, nil)

    %{state | running: Map.put(state.running, issue_id, updated_entry)}
  end

  defp running_entry_alive?(running_entry) do
    case Map.get(running_entry, :pid) do
      pid when is_pid(pid) -> Process.alive?(pid)
      _other -> false
    end
  end

  defp maybe_clear_terminal_timer(running_entry) do
    if running_entry_alive?(running_entry) do
      running_entry
    else
      Map.put(running_entry, :run_budget_timer_ref, nil)
    end
  end

  defp retry_pending_terminal_transitions(%State{} = state) do
    Enum.reduce(state.running, state, fn {issue_id, running_entry}, state_acc ->
      case Map.get(running_entry, :terminal_pending) do
        nil -> state_acc
        pending -> persist_or_block_terminal(state_acc, issue_id, running_entry, pending)
      end
    end)
  end

  defp apply_terminal_success(state, issue_id, running_entry, action) do
    stop_running_entry(running_entry)
    {_popped_entry, state} = pop_running_entry(state, issue_id)
    state = record_session_completion_totals(state, running_entry)

    case action do
      {:continuation, attempt, metadata} ->
        state
        |> complete_issue(issue_id)
        |> schedule_issue_retry(issue_id, attempt, metadata)

      {:retry, attempt, metadata} ->
        schedule_issue_retry(state, issue_id, attempt, metadata)

      {:stop, cleanup_workspace, retry} ->
        worker_host = Map.get(running_entry, :worker_host)

        if cleanup_workspace do
          cleanup_issue_workspace(running_entry.identifier, worker_host)
        end

        state = %{
          state
          | claimed: MapSet.delete(state.claimed, issue_id),
            retry_attempts: Map.delete(state.retry_attempts, issue_id)
        }

        case retry do
          %{attempt: attempt, metadata: metadata} ->
            schedule_issue_retry(state, issue_id, attempt, metadata)

          _other ->
            state
        end
    end
  end

  defp record_run_event(%State{} = state, running_entry, transition, stage, extra \\ [])
       when is_map(running_entry) and is_binary(transition) and is_binary(stage) do
    event = run_event(running_entry, transition, stage, extra)

    case append_run_event(state, event) do
      :ok ->
        state

      {:error, reason} ->
        Logger.error("Failed to append Symphony run ledger event issue_id=#{event.issue_id}: #{inspect(reason)}")

        state
    end
  end

  defp run_event(running_entry, transition, stage, extra \\ []) do
    issue = Map.get(running_entry, :issue)

    %{
      transition: transition,
      stage: stage,
      run_id: Map.get(running_entry, :run_id),
      issue_id: if(is_map(issue), do: Map.get(issue, :id)),
      issue_identifier: Map.get(running_entry, :identifier),
      attempt: Map.get(running_entry, :retry_attempt, 0),
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path),
      resolved_model: Map.get(running_entry, :resolved_model),
      reasoning_effort: Map.get(running_entry, :reasoning_effort),
      model_catalog_source: Map.get(running_entry, :model_catalog_source),
      terminal_reason: Keyword.get(extra, :terminal_reason)
    }
  end

  defp append_run_event(%State{run_ledger_path: nil}, _event), do: :ok

  defp append_run_event(%State{} = state, event) when is_map(event) do
    event = Map.put(event, :runner_generation, state.runner_generation)
    RunLedger.append(state.run_ledger_path, event)
  end

  defp persist_dispatch_pause(%State{} = state, paused) when is_boolean(paused) do
    transition = if paused, do: "dispatch_paused", else: "dispatch_resumed"

    case append_run_event(state, %{transition: transition, stage: "operator"}) do
      :ok ->
        state = state |> Map.put(:dispatch_paused, paused) |> after_dispatch_control_change(paused)
        notify_dashboard()

        {:reply,
         {:ok,
          %{
            dispatch_paused: paused,
            changed: true,
            requested_at: DateTime.utc_now()
          }}, state}

      {:error, reason} ->
        {:reply, {:error, {:ledger_write_failed, reason}}, state}
    end
  end

  defp after_dispatch_control_change(state, true), do: state

  defp after_dispatch_control_change(state, false) do
    state
    |> wake_paused_retries()
    |> schedule_tick(0)
  end

  defp operator_wait_event(wait, transition) do
    %{
      transition: transition,
      stage: wait.stage,
      run_id: wait.run_id,
      issue_id: wait.issue_id,
      issue_identifier: wait.identifier,
      attempt: wait.attempt,
      wait_id: wait.wait_id,
      parked_reason: wait.reason,
      allowed_actions: wait.allowed_actions,
      tracker_state: wait.tracker_state,
      terminal_reason: Map.get(wait, :terminal_reason),
      worker_host: Map.get(wait, :worker_host),
      workspace_path: Map.get(wait, :workspace_path)
    }
  end

  defp resolve_operator_wait(state, wait, action) do
    case apply_operator_wait_action(state, wait, action) do
      {:ok, payload, state} -> {:reply, {:ok, payload}, state}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  defp apply_operator_wait_action(state, wait, action) do
    if OperatorWait.action_allowed?(wait, action) do
      transition = if action == "reject", do: "wait_rejected", else: "resume_queued"
      next_attempt = max(wait.attempt + 1, 1)

      event =
        wait
        |> operator_wait_event(transition)
        |> maybe_mark_resume_queued(action)
        |> maybe_put_resumed_attempt(action, next_attempt)

      case append_run_event(state, event) do
        :ok when action == "reject" ->
          {:ok, %{wait: wait, action: action, resumed: false}, state}

        :ok ->
          state =
            state
            |> Map.update!(:parked, &Map.delete(&1, wait.issue_id))
            |> Map.update!(:queued_resumes, fn queued ->
              Map.put(queued, wait.issue_id, queued_resume_entry(wait, next_attempt))
            end)
            |> schedule_tick(0)

          {:ok, %{wait: wait, action: action, resumed: true}, state}

        {:error, reason} ->
          {:error, {:ledger_write_failed, reason}, state}
      end
    else
      {:error, :action_not_allowed, state}
    end
  end

  defp maybe_put_resumed_attempt(event, "reject", _next_attempt), do: event
  defp maybe_put_resumed_attempt(event, _action, next_attempt), do: Map.put(event, :attempt, next_attempt)

  defp maybe_mark_resume_queued(event, "reject"), do: event
  defp maybe_mark_resume_queued(event, _action), do: Map.put(event, :stage, "resume_queued")

  defp queued_resume_entry(wait, next_attempt) do
    %{
      issue_id: wait.issue_id,
      identifier: wait.identifier,
      run_id: wait.run_id,
      wait_id: wait.wait_id,
      attempt: next_attempt,
      stage: "resume_queued",
      worker_host: Map.get(wait, :worker_host),
      workspace_path: Map.get(wait, :workspace_path),
      queued_at: DateTime.utc_now()
    }
  end

  defp process_operator_comments(%State{} = state) do
    process_operator_comments(state, operator_user_ids())
  end

  defp process_operator_comments(%State{} = state, []), do: state

  defp process_operator_comments(%State{} = state, operator_user_ids) do
    state
    |> operator_command_issue_ids()
    |> Enum.reduce(state, fn issue_id, state_acc ->
      state_acc = ensure_operator_cursor(state_acc, issue_id)

      case Map.get(state_acc.operator_comment_cursors, issue_id) do
        %{created_at: %DateTime{} = cursor} ->
          process_issue_operator_comments(
            state_acc,
            issue_id,
            cursor,
            operator_user_ids
          )

        _cursor ->
          state_acc
      end
    end)
  end

  defp operator_command_issue_ids(%State{} = state) do
    state.running
    |> Map.keys()
    |> Enum.concat(Map.keys(state.parked))
    |> MapSet.new()
  end

  defp ensure_operator_cursor(%State{} = state, issue_id) do
    if Map.has_key?(state.operator_comment_cursors, issue_id) do
      state
    else
      persist_operator_cursor(
        state,
        "operator_cursor_initialized",
        issue_id,
        DateTime.utc_now(),
        nil
      )
    end
  end

  defp initialize_operator_cursor(%State{} = state, issue_id, %DateTime{} = baseline) do
    persist_operator_cursor(state, "operator_cursor_initialized", issue_id, baseline, nil)
  end

  defp process_issue_operator_comments(
         %State{} = state,
         issue_id,
         cursor,
         operator_user_ids
       ) do
    case Tracker.fetch_comments_since(issue_id, cursor) do
      {:ok, comments} ->
        comments
        |> Enum.sort_by(&operator_comment_sort_key/1)
        |> Enum.reduce(state, fn comment, state_acc ->
          process_operator_comment(state_acc, issue_id, comment, operator_user_ids)
        end)

      {:error, reason} ->
        Logger.debug("Failed to fetch operator comments issue_id=#{issue_id}: #{inspect(reason)}")

        state
    end
  end

  defp operator_comment_sort_key(%{created_at: %DateTime{} = created_at, id: id}) do
    {DateTime.to_unix(created_at, :microsecond), id}
  end

  defp operator_comment_sort_key(comment), do: {0, inspect(comment)}

  defp process_operator_comment(
         %State{} = state,
         issue_id,
         %{id: comment_id, created_at: %DateTime{} = created_at} = comment,
         operator_user_ids
       )
       when is_binary(comment_id) do
    if operator_comment_seen_at_cursor?(state, issue_id, comment) do
      state
    else
      state
      |> maybe_apply_operator_comment(issue_id, comment, operator_user_ids)
      |> persist_operator_cursor(
        "operator_cursor_advanced",
        issue_id,
        created_at,
        comment_id
      )
    end
  end

  defp process_operator_comment(state, _issue_id, _comment, _operator_user_ids), do: state

  defp operator_comment_seen_at_cursor?(state, issue_id, comment) do
    case Map.get(state.operator_comment_cursors, issue_id) do
      %{created_at: %DateTime{} = cursor_at, comment_ids: comment_ids} ->
        DateTime.compare(comment.created_at, cursor_at) == :eq and
          MapSet.member?(comment_ids, comment.id)

      _cursor ->
        false
    end
  end

  defp maybe_apply_operator_comment(
         %State{} = state,
         issue_id,
         comment,
         operator_user_ids
       ) do
    if MapSet.member?(state.processed_operator_comment_ids, comment.id) do
      state
    else
      parse_and_apply_operator_comment(state, issue_id, comment, operator_user_ids)
    end
  end

  defp parse_and_apply_operator_comment(state, issue_id, comment, operator_user_ids) do
    case OperatorCommand.parse_comment(comment, operator_user_ids) do
      {:ok, action} -> apply_operator_comment(state, issue_id, comment, action)
      :ignore -> state
    end
  end

  defp operator_user_ids do
    Config.settings!().tracker.operator_user_ids || []
  end

  defp apply_operator_comment(state, issue_id, comment, "stop") do
    case Map.get(state.running, issue_id) do
      %{issue: %Issue{} = issue} ->
        updated_state =
          park_running_issue(state, issue, "operator_stopped", terminal_reason: "operator_stop")

        if Map.has_key?(updated_state.parked, issue_id) do
          record_operator_command_outcome(
            updated_state,
            issue_id,
            comment,
            "stop",
            "operator_command_applied"
          )
        else
          record_operator_command_outcome(
            state,
            issue_id,
            comment,
            "stop",
            "operator_command_rejected"
          )
        end

      _running ->
        record_operator_command_outcome(
          state,
          issue_id,
          comment,
          "stop",
          "operator_command_rejected"
        )
    end
  end

  defp apply_operator_comment(state, issue_id, comment, action) do
    case Map.get(state.parked, issue_id) do
      nil ->
        record_operator_command_outcome(
          state,
          issue_id,
          comment,
          action,
          "operator_command_rejected"
        )

      wait ->
        case apply_operator_wait_action(state, wait, action) do
          {:ok, _payload, updated_state} ->
            record_operator_command_outcome(
              updated_state,
              issue_id,
              comment,
              action,
              "operator_command_applied"
            )

          {:error, _reason, unchanged_state} ->
            record_operator_command_outcome(
              unchanged_state,
              issue_id,
              comment,
              action,
              "operator_command_rejected"
            )
        end
    end
  end

  defp record_operator_command_outcome(
         %State{} = state,
         issue_id,
         comment,
         action,
         transition
       ) do
    event = %{
      transition: transition,
      stage: "operator",
      issue_id: issue_id,
      comment_id: comment.id,
      comment_created_at: DateTime.to_iso8601(comment.created_at),
      operator_command: action
    }

    case append_run_event(state, event) do
      :ok ->
        Logger.info("Operator command #{action} #{operator_outcome_label(transition)} issue_id=#{issue_id} comment_id=#{comment.id}")

        %{
          state
          | processed_operator_comment_ids: MapSet.put(state.processed_operator_comment_ids, comment.id)
        }

      {:error, reason} ->
        Logger.error("Failed to record operator command issue_id=#{issue_id} comment_id=#{comment.id}: #{inspect(reason)}")

        state
    end
  end

  defp operator_outcome_label("operator_command_applied"), do: "applied"
  defp operator_outcome_label(_transition), do: "rejected"

  defp persist_operator_cursor(
         %State{} = state,
         transition,
         issue_id,
         %DateTime{} = created_at,
         comment_id
       ) do
    event = %{
      transition: transition,
      stage: "operator",
      issue_id: issue_id,
      comment_id: comment_id,
      comment_created_at: DateTime.to_iso8601(created_at)
    }

    case append_run_event(state, event) do
      :ok ->
        cursor = advance_operator_cursor(state, issue_id, created_at, comment_id)

        %{
          state
          | operator_comment_cursors: Map.put(state.operator_comment_cursors, issue_id, cursor)
        }

      {:error, reason} ->
        Logger.error("Failed to persist operator comment cursor issue_id=#{issue_id}: #{inspect(reason)}")

        state
    end
  end

  defp advance_operator_cursor(state, issue_id, created_at, comment_id) do
    case Map.get(state.operator_comment_cursors, issue_id) do
      %{created_at: %DateTime{} = current_at, comment_ids: comment_ids}
      when created_at == current_at ->
        %{
          created_at: created_at,
          comment_ids: maybe_put_operator_comment_id(comment_ids, comment_id)
        }

      %{created_at: %DateTime{} = current_at} = current_cursor ->
        case DateTime.compare(created_at, current_at) do
          :lt -> current_cursor
          _ -> new_operator_cursor(created_at, comment_id)
        end

      _cursor ->
        new_operator_cursor(created_at, comment_id)
    end
  end

  defp new_operator_cursor(created_at, comment_id) do
    %{
      created_at: created_at,
      comment_ids: maybe_put_operator_comment_id(MapSet.new(), comment_id)
    }
  end

  defp maybe_put_operator_comment_id(comment_ids, comment_id) when is_binary(comment_id),
    do: MapSet.put(comment_ids, comment_id)

  defp maybe_put_operator_comment_id(comment_ids, _comment_id), do: comment_ids

  defp restore_operator_comment_cursors(cursors) when is_map(cursors) do
    Enum.reduce(cursors, %{}, fn
      {issue_id, %{created_at: timestamp, comment_ids: comment_ids}}, restored ->
        case DateTime.from_iso8601(timestamp) do
          {:ok, created_at, _offset} ->
            Map.put(restored, issue_id, %{
              created_at: created_at,
              comment_ids: comment_ids
            })

          {:error, _reason} ->
            restored
        end

      {_issue_id, _cursor}, restored ->
        restored
    end)
  end

  defp restore_operator_comment_cursors(_cursors), do: %{}

  defp restore_parked_waits(events) when is_map(events) do
    Enum.reduce_while(events, {:ok, %{}}, fn {issue_id, event}, {:ok, restored} ->
      case OperatorWait.from_ledger_event(event) do
        {:ok, wait} ->
          {:cont, {:ok, Map.put(restored, issue_id, wait)}}

        {:error, reason} ->
          {:halt, {:error, {:invalid_parked_wait, issue_id, reason}}}
      end
    end)
  end

  defp restore_parked_waits(_events), do: {:error, :invalid_parked_wait_collection}

  defp restore_queued_resumes(events) when is_map(events) do
    Enum.reduce_while(events, {:ok, %{}}, fn {issue_id, event}, {:ok, restored} ->
      case queued_resume_from_ledger_event(event) do
        {:ok, queued_resume} ->
          {:cont, {:ok, Map.put(restored, issue_id, queued_resume)}}

        {:error, reason} ->
          {:halt, {:error, {:invalid_queued_resume, issue_id, reason}}}
      end
    end)
  end

  defp restore_queued_resumes(_events), do: {:error, :invalid_queued_resume_collection}

  defp queued_resume_from_ledger_event(event) do
    case DateTime.from_iso8601(event["occurred_at"]) do
      {:ok, queued_at, _offset} ->
        {:ok,
         %{
           issue_id: event["issue_id"],
           identifier: event["issue_identifier"],
           run_id: event["run_id"],
           wait_id: event["wait_id"],
           attempt: event["attempt"],
           stage: "resume_queued",
           worker_host: event["worker_host"],
           workspace_path: event["workspace_path"],
           queued_at: queued_at
         }}

      _other ->
        {:error, :invalid_occurred_at}
    end
  end

  defp queued_resume_snapshot_rows(queued_resumes) do
    Enum.map(queued_resumes, fn {issue_id, queued} ->
      %{
        issue_id: issue_id,
        run_id: Map.get(queued, :run_id),
        wait_id: Map.get(queued, :wait_id),
        attempt: Map.get(queued, :attempt),
        stage: "resume_queued",
        due_in_ms: 0,
        identifier: Map.get(queued, :identifier),
        error: affinity_error(queued),
        worker_host: Map.get(queued, :worker_host),
        workspace_path: Map.get(queued, :workspace_path),
        queued_at: Map.get(queued, :queued_at)
      }
    end)
  end

  defp recovered_dispatch_snapshot_rows(recovered_dispatches) do
    Enum.map(recovered_dispatches, fn {issue_id, dispatch} ->
      %{
        issue_id: issue_id,
        run_id: Map.get(dispatch, :previous_run_id),
        attempt: Map.get(dispatch, :attempt),
        stage: "recovery_queued",
        due_in_ms: 0,
        identifier: Map.get(dispatch, :identifier),
        error: affinity_error(dispatch),
        worker_host: Map.get(dispatch, :worker_host),
        workspace_path: Map.get(dispatch, :workspace_path)
      }
    end)
  end

  defp affinity_error(dispatch) do
    worker_host = Map.get(dispatch, :worker_host)

    cond do
      not valid_expected_workspace_path?(Map.get(dispatch, :workspace_path)) ->
        "workspace_affinity_missing"

      Config.settings!().worker.ssh_hosts == [] and is_binary(worker_host) ->
        "worker_affinity_unavailable"

      Config.settings!().worker.ssh_hosts != [] and
          not preferred_worker_host_available?(worker_host, Config.settings!().worker.ssh_hosts) ->
        "worker_affinity_unavailable"

      true ->
        nil
    end
  end

  defp log_run_event_result(:ok, _issue_id), do: :ok

  defp log_run_event_result({:error, reason}, issue_id) do
    Logger.error("Failed to append Symphony retry ledger event issue_id=#{issue_id}: #{inspect(reason)}")
    :ok
  end

  defp record_session_completion_totals(state, running_entry) when is_map(running_entry) do
    runtime_seconds = running_seconds(running_entry.started_at, DateTime.utc_now())

    codex_totals =
      apply_token_delta(
        state.codex_totals,
        %{
          input_tokens: 0,
          output_tokens: 0,
          total_tokens: 0,
          seconds_running: runtime_seconds
        }
      )

    %{state | codex_totals: codex_totals}
  end

  defp record_session_completion_totals(state, _running_entry), do: state

  defp refresh_runtime_config(%State{} = state) do
    config = Config.settings!()

    %{
      state
      | poll_interval_ms: config.polling.interval_ms,
        max_concurrent_agents: config.agent.max_concurrent_agents
    }
  end

  defp retry_candidate_issue?(%Issue{} = issue, terminal_states) do
    candidate_issue?(issue, active_state_set(), terminal_states) and
      !todo_issue_blocked_by_non_terminal?(issue, terminal_states)
  end

  defp dispatch_slots_available?(%Issue{} = issue, %State{} = state) do
    available_slots(state) > 0 and state_slots_available?(issue, state.running)
  end

  defp apply_codex_token_delta(
         %{codex_totals: codex_totals} = state,
         %{input_tokens: input, output_tokens: output, total_tokens: total} = token_delta
       )
       when is_integer(input) and is_integer(output) and is_integer(total) do
    %{state | codex_totals: apply_token_delta(codex_totals, token_delta)}
  end

  defp apply_codex_token_delta(state, _token_delta), do: state

  defp apply_codex_rate_limits(%State{} = state, update) when is_map(update) do
    case extract_rate_limits(update) do
      %{} = rate_limits ->
        %{state | codex_rate_limits: rate_limits}

      _ ->
        state
    end
  end

  defp apply_token_delta(codex_totals, token_delta) do
    input_tokens = Map.get(codex_totals, :input_tokens, 0) + token_delta.input_tokens
    output_tokens = Map.get(codex_totals, :output_tokens, 0) + token_delta.output_tokens
    total_tokens = Map.get(codex_totals, :total_tokens, 0) + token_delta.total_tokens

    seconds_running =
      Map.get(codex_totals, :seconds_running, 0) + Map.get(token_delta, :seconds_running, 0)

    %{
      input_tokens: max(0, input_tokens),
      output_tokens: max(0, output_tokens),
      total_tokens: max(0, total_tokens),
      seconds_running: max(0, seconds_running)
    }
  end

  defp extract_token_delta(running_entry, %{event: _, timestamp: _} = update) do
    usage = extract_token_usage(update)

    telemetry_observed =
      Enum.any?([:input, :output, :total], fn token_key ->
        is_integer(get_token_usage(usage, token_key))
      end)

    {
      compute_token_delta(
        running_entry,
        :input,
        usage,
        :codex_last_reported_input_tokens
      ),
      compute_token_delta(
        running_entry,
        :output,
        usage,
        :codex_last_reported_output_tokens
      ),
      compute_token_delta(
        running_entry,
        :total,
        usage,
        :codex_last_reported_total_tokens
      )
    }
    |> Tuple.to_list()
    |> then(fn [input, output, total] ->
      %{
        input_tokens: input.delta,
        output_tokens: output.delta,
        total_tokens: total.delta,
        input_reported: input.reported,
        output_reported: output.reported,
        total_reported: total.reported,
        telemetry_observed: telemetry_observed
      }
    end)
  end

  defp compute_token_delta(running_entry, token_key, usage, reported_key) do
    next_total = get_token_usage(usage, token_key)
    prev_reported = Map.get(running_entry, reported_key, 0)

    delta =
      if is_integer(next_total) and next_total >= prev_reported do
        next_total - prev_reported
      else
        0
      end

    %{
      delta: max(delta, 0),
      reported: if(is_integer(next_total), do: next_total, else: prev_reported)
    }
  end

  defp extract_token_usage(update) do
    payloads = [
      update[:usage],
      Map.get(update, "usage"),
      Map.get(update, :usage),
      update[:payload],
      Map.get(update, "payload"),
      update
    ]

    Enum.find_value(payloads, &absolute_token_usage_from_payload/1) ||
      Enum.find_value(payloads, &turn_completed_usage_from_payload/1) ||
      %{}
  end

  defp extract_rate_limits(update) do
    rate_limits_from_payload(update[:rate_limits]) ||
      rate_limits_from_payload(Map.get(update, "rate_limits")) ||
      rate_limits_from_payload(Map.get(update, :rate_limits)) ||
      rate_limits_from_payload(update[:payload]) ||
      rate_limits_from_payload(Map.get(update, "payload")) ||
      rate_limits_from_payload(update)
  end

  defp absolute_token_usage_from_payload(payload) when is_map(payload) do
    absolute_paths = [
      ["params", "msg", "payload", "info", "total_token_usage"],
      [:params, :msg, :payload, :info, :total_token_usage],
      ["params", "msg", "info", "total_token_usage"],
      [:params, :msg, :info, :total_token_usage],
      ["params", "tokenUsage", "total"],
      [:params, :tokenUsage, :total],
      ["tokenUsage", "total"],
      [:tokenUsage, :total]
    ]

    explicit_map_at_paths(payload, absolute_paths)
  end

  defp absolute_token_usage_from_payload(_payload), do: nil

  defp turn_completed_usage_from_payload(payload) when is_map(payload) do
    method = Map.get(payload, "method") || Map.get(payload, :method)

    if method in ["turn/completed", :turn_completed] do
      direct =
        Map.get(payload, "usage") ||
          Map.get(payload, :usage) ||
          map_at_path(payload, ["params", "usage"]) ||
          map_at_path(payload, [:params, :usage])

      if is_map(direct) and integer_token_map?(direct), do: direct
    end
  end

  defp turn_completed_usage_from_payload(_payload), do: nil

  defp rate_limits_from_payload(payload) when is_map(payload) do
    direct = Map.get(payload, "rate_limits") || Map.get(payload, :rate_limits)

    case RateLimitTelemetry.normalize(direct) || RateLimitTelemetry.normalize(payload) do
      %{} = rate_limits -> rate_limits
      nil -> rate_limit_payloads(payload)
    end
  end

  defp rate_limits_from_payload(payload) when is_list(payload) do
    rate_limit_payloads(payload)
  end

  defp rate_limits_from_payload(_payload), do: nil

  defp rate_limit_payloads(payload) when is_map(payload) do
    Map.values(payload)
    |> Enum.reduce_while(nil, fn
      value, nil ->
        case rate_limits_from_payload(value) do
          nil -> {:cont, nil}
          rate_limits -> {:halt, rate_limits}
        end

      _value, result ->
        {:halt, result}
    end)
  end

  defp rate_limit_payloads(payload) when is_list(payload) do
    payload
    |> Enum.reduce_while(nil, fn
      value, nil ->
        case rate_limits_from_payload(value) do
          nil -> {:cont, nil}
          rate_limits -> {:halt, rate_limits}
        end

      _value, result ->
        {:halt, result}
    end)
  end

  defp explicit_map_at_paths(payload, paths) when is_map(payload) and is_list(paths) do
    Enum.find_value(paths, fn path ->
      value = map_at_path(payload, path)

      if is_map(value) and integer_token_map?(value), do: value
    end)
  end

  defp explicit_map_at_paths(_payload, _paths), do: nil

  defp map_at_path(payload, path) when is_map(payload) and is_list(path) do
    Enum.reduce_while(path, payload, fn key, acc ->
      if is_map(acc) and Map.has_key?(acc, key) do
        {:cont, Map.get(acc, key)}
      else
        {:halt, nil}
      end
    end)
  end

  defp map_at_path(_payload, _path), do: nil

  defp integer_token_map?(payload) do
    token_fields = [
      :input_tokens,
      :output_tokens,
      :total_tokens,
      :prompt_tokens,
      :completion_tokens,
      :inputTokens,
      :outputTokens,
      :totalTokens,
      :promptTokens,
      :completionTokens,
      "input_tokens",
      "output_tokens",
      "total_tokens",
      "prompt_tokens",
      "completion_tokens",
      "inputTokens",
      "outputTokens",
      "totalTokens",
      "promptTokens",
      "completionTokens"
    ]

    token_fields
    |> Enum.any?(fn field ->
      value = payload_get(payload, field)
      !is_nil(integer_like(value))
    end)
  end

  defp get_token_usage(usage, :input),
    do:
      payload_get(usage, [
        "input_tokens",
        "prompt_tokens",
        :input_tokens,
        :prompt_tokens,
        :input,
        "promptTokens",
        :promptTokens,
        "inputTokens",
        :inputTokens
      ])

  defp get_token_usage(usage, :output),
    do:
      payload_get(usage, [
        "output_tokens",
        "completion_tokens",
        :output_tokens,
        :completion_tokens,
        :output,
        :completion,
        "outputTokens",
        :outputTokens,
        "completionTokens",
        :completionTokens
      ])

  defp get_token_usage(usage, :total),
    do:
      payload_get(usage, [
        "total_tokens",
        "total",
        :total_tokens,
        :total,
        "totalTokens",
        :totalTokens
      ])

  defp payload_get(payload, fields) when is_list(fields) do
    Enum.find_value(fields, fn field -> map_integer_value(payload, field) end)
  end

  defp payload_get(payload, field), do: map_integer_value(payload, field)

  defp map_integer_value(payload, field) do
    if is_map(payload) do
      value = Map.get(payload, field)
      integer_like(value)
    else
      nil
    end
  end

  defp run_budget_snapshot(running_entry, now) do
    RunBudget.snapshot(
      Map.get(running_entry, :run_budget, disabled_run_budget()),
      run_budget_metrics(running_entry, now)
    )
  end

  defp run_budget_metrics(running_entry, now) do
    %{
      turns: Map.get(running_entry, :turn_count, 0),
      tokens: Map.get(running_entry, :codex_total_tokens, 0),
      token_telemetry_observed: Map.get(running_entry, :codex_token_telemetry_observed, false),
      seconds: running_seconds(Map.get(running_entry, :started_at), now)
    }
  end

  defp disabled_run_budget do
    %{max_turns: Config.settings!().agent.max_turns, max_tokens: nil, max_seconds: nil}
  end

  defp running_seconds(%DateTime{} = started_at, %DateTime{} = now) do
    max(0, DateTime.diff(now, started_at, :second))
  end

  defp running_seconds(_started_at, _now), do: 0

  defp integer_like(value) when is_integer(value) and value >= 0, do: value

  defp integer_like(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {num, _} when num >= 0 -> num
      _ -> nil
    end
  end

  defp integer_like(_value), do: nil
end
