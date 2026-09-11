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
    MergeLane,
    ObservabilitySanitizer,
    OperatorCommand,
    OperatorWait,
    PollTaskGuard,
    RateLimitTelemetry,
    RunBudget,
    RunLedger,
    StatusDashboard,
    Tracker,
    TrackerAdmission,
    Workspace
  }

  alias SymphonyElixir.Linear.Issue

  @continuation_retry_delay_ms 1_000
  @failure_retry_base_ms 10_000
  @max_cumulative_token_count 9_223_372_036_854_775_807
  @poll_task_timeout_ms 30_000
  @poll_failure_backoff_base_ms 250
  @poll_failure_backoff_max_ms 5_000
  @refresh_call_timeout_ms 500
  @max_workspace_cleanup_tasks 1
  # Slightly above the dashboard render interval so "checking now…" can render.
  @poll_transition_render_delay_ms 20
  @empty_codex_totals %{
    input_tokens: 0,
    output_tokens: 0,
    total_tokens: 0,
    seconds_running: 0
  }

  defmodule OperatorCommandState do
    @moduledoc false

    defstruct processed_comment_ids: MapSet.new(),
              pending_outcomes: %{},
              operator_user_ids_generation: nil,
              operator_authority_generation: nil,
              operator_authority_invalidated: false,
              tracker_authority_generation: nil,
              tracker_authority_invalidated: false,
              tracker_context: nil,
              tracker_admissions: %{},
              tracker_fetch_by_ids_fn: nil,
              tracker_update_state_fn: nil,
              workflow_generation: nil
  end

  defmodule State do
    @moduledoc """
    Runtime state for the orchestrator polling loop.
    """

    defstruct [
      :poll_interval_ms,
      :max_concurrent_agents,
      :next_poll_due_at_ms,
      :poll_check_in_progress,
      :poll_task,
      :poll_task_timeout_ms,
      :poll_work_fn,
      :poll_owner_key,
      :tick_timer_ref,
      :tick_token,
      :run_ledger_path,
      :run_ledger_append_fn,
      :task_start_fn,
      :runner_generation,
      poll_generation: 0,
      poll_dirty: false,
      poll_failure_count: 0,
      dispatch_paused: false,
      running: %{},
      parked: %{},
      completed: MapSet.new(),
      claimed: MapSet.new(),
      recovered_attempts: %{},
      recovered_dispatches: %{},
      queued_resumes: %{},
      retry_attempts: %{},
      cleanup_pending: %{},
      operator_commands: %OperatorCommandState{},
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

    startup_settings_fn =
      Keyword.get(opts, :startup_settings_fn, &Config.settings_with_authority!/0)

    {config, authority_generation, tracker_authority_generation} = startup_settings_fn.()

    run_ledger_path = Keyword.get(opts, :run_ledger_path, RunLedger.default_path())
    run_ledger_append_fn = Keyword.get(opts, :run_ledger_append_fn, &RunLedger.append/2)
    runner_generation = RunLedger.new_id("runner")

    workflow_generation =
      Keyword.get(opts, :workflow_generation) ||
        System.get_env("SYMPHONY_EXPECTED_WORKFLOW_SHA256")

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
            poll_task: nil,
            poll_task_timeout_ms: Keyword.get(opts, :poll_task_timeout_ms, @poll_task_timeout_ms),
            poll_work_fn: Keyword.get(opts, :poll_work_fn),
            poll_owner_key: Keyword.get(opts, :name, __MODULE__),
            tick_timer_ref: nil,
            tick_token: nil,
            run_ledger_path: run_ledger_path,
            run_ledger_append_fn: run_ledger_append_fn,
            runner_generation: runner_generation,
            dispatch_paused: recovery.dispatch_paused,
            recovered_attempts: recovery.recovered_attempts,
            recovered_dispatches: recovery.recovered_dispatches,
            queued_resumes: queued_resumes,
            cleanup_pending: restore_cleanup_pending(recovery.cleanup_pending),
            parked: parked,
            claimed:
              recovery.cleanup_pending
              |> Map.keys()
              |> Kernel.++(Map.keys(recovery.tracker_admissions))
              |> MapSet.new(),
            operator_commands: %OperatorCommandState{
              processed_comment_ids: recovery.processed_operator_comment_ids,
              pending_outcomes: restore_pending_operator_outcomes(recovery.pending_operator_outcomes),
              operator_user_ids_generation: config.tracker.operator_user_ids || [],
              operator_authority_generation: authority_generation,
              tracker_authority_generation: tracker_authority_generation,
              tracker_context: Tracker.poll_context(config.tracker, tracker_authority_generation),
              tracker_admissions: recovery.tracker_admissions,
              tracker_fetch_by_ids_fn: Keyword.get(opts, :tracker_fetch_by_ids_fn),
              tracker_update_state_fn: Keyword.get(opts, :tracker_update_state_fn),
              workflow_generation: workflow_generation
            },
            operator_comment_cursors: restore_operator_comment_cursors(recovery.operator_comment_cursors),
            codex_totals: @empty_codex_totals,
            codex_rate_limits: nil
          }

          state =
            state
            |> retry_pending_operator_outcomes()
            |> initialize_workspace_cleanup_recovery()
            |> schedule_tick(0)

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
    state = %{
      state
      | next_poll_due_at_ms: nil,
        tick_timer_ref: nil,
        tick_token: nil
    }

    {:noreply, queue_poll_cycle(state)}
  end

  def handle_info({:tick, _tick_token}, state), do: {:noreply, state}

  def handle_info(:tick, state) do
    state = %{
      state
      | next_poll_due_at_ms: nil,
        tick_timer_ref: nil,
        tick_token: nil
    }

    {:noreply, queue_poll_cycle(state)}
  end

  def handle_info(:run_poll_cycle, %{poll_task: %{} = _poll_task} = state) do
    {:noreply, %{state | poll_dirty: true}}
  end

  def handle_info(:run_poll_cycle, state) do
    state = start_poll_task(state)
    notify_dashboard()
    {:noreply, state}
  end

  def handle_info(
        {:poll_guard_started, guard_pid, generation, worker_pid},
        %{poll_task: %{guard_pid: guard_pid, generation: generation}} = state
      )
      when is_pid(guard_pid) and is_pid(worker_pid) and is_integer(generation) do
    {:noreply, put_in(state.poll_task.pid, worker_pid)}
  end

  def handle_info({:poll_guard_started, _guard_pid, _generation, _worker_pid}, state),
    do: {:noreply, state}

  def handle_info(
        {:poll_guard_result, guard_pid, generation, result},
        %{poll_task: %{guard_pid: guard_pid, generation: generation}} = state
      )
      when is_pid(guard_pid) and is_integer(generation) do
    state = clear_poll_task(state)

    state =
      case result do
        %{} = poll_result ->
          state
          |> apply_poll_result(poll_result)
          |> finish_poll_cycle(:ok)

        invalid_result ->
          Logger.warning("Tracker poll task returned an invalid result: #{inspect(invalid_result)}")
          finish_poll_cycle(state, {:error, :invalid_poll_result})
      end

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info({:poll_guard_result, _guard_pid, _generation, _result}, state),
    do: {:noreply, state}

  def handle_info(
        {:poll_guard_failed, guard_pid, generation, reason},
        %{poll_task: %{guard_pid: guard_pid, generation: generation}} = state
      ) do
    Logger.warning("Tracker poll task crashed generation=#{generation} reason=#{inspect(reason)}")
    state = state |> clear_poll_task() |> finish_poll_cycle({:error, {:task_exit, reason}})
    notify_dashboard()
    {:noreply, state}
  end

  def handle_info({:poll_guard_failed, _guard_pid, _generation, _reason}, state),
    do: {:noreply, state}

  def handle_info(
        {:poll_guard_busy, guard_pid, generation, existing_guard},
        %{poll_task: %{guard_pid: guard_pid, generation: generation}} = state
      ) do
    Logger.warning("Tracker poll admission remains owned generation=#{generation} existing_guard=#{inspect(existing_guard)}")

    state = state |> clear_poll_task() |> finish_poll_cycle({:error, :poll_admission_busy})
    notify_dashboard()
    {:noreply, state}
  end

  def handle_info({:poll_guard_busy, _guard_pid, _generation, _existing_guard}, state),
    do: {:noreply, state}

  def handle_info(
        {:poll_task_timeout, generation, guard_pid, timeout_token},
        %{
          poll_task: %{
            generation: generation,
            guard_pid: guard_pid,
            timeout_token: timeout_token
          }
        } = state
      ) do
    :ok = PollTaskGuard.cancel(guard_pid, generation)
    Logger.warning("Tracker poll task timed out generation=#{generation}")
    state = state |> clear_poll_task() |> finish_poll_cycle({:error, :timeout})
    notify_dashboard()
    {:noreply, state}
  end

  def handle_info({:poll_task_timeout, _generation, _guard_pid, _timeout_token}, state),
    do: {:noreply, state}

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{poll_task: %{guard_ref: ref, generation: generation}} = state
      ) do
    Logger.warning("Tracker poll guard exited generation=#{generation} reason=#{inspect(reason)}")
    state = state |> clear_poll_task() |> finish_poll_cycle({:error, {:guard_exit, reason}})
    notify_dashboard()
    {:noreply, state}
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{running: running} = state
      ) do
    case find_issue_id_for_ref(running, ref) do
      nil ->
        handle_workspace_cleanup_down(ref, reason, state)

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

  def handle_info(
        {:worker_runtime_info, issue_id, runtime_info, worker_pid, acknowledgment_ref},
        state
      )
      when is_pid(worker_pid) and is_reference(acknowledgment_ref) do
    {state, result} = accept_worker_runtime_info(state, issue_id, runtime_info)
    send(worker_pid, {:worker_runtime_ack, acknowledgment_ref, result})
    notify_dashboard()
    {:noreply, state}
  end

  def handle_info({:worker_runtime_info, issue_id, runtime_info}, state) do
    {state, _result} = accept_worker_runtime_info(state, issue_id, runtime_info)
    notify_dashboard()
    {:noreply, state}
  end

  def handle_info(
        {:worker_model_resolution, issue_id, resolution_info, worker_pid, acknowledgment_ref},
        state
      )
      when is_pid(worker_pid) and is_reference(acknowledgment_ref) do
    {state, result} = accept_worker_model_resolution(state, issue_id, resolution_info)
    send(worker_pid, {:worker_model_resolution_ack, acknowledgment_ref, result})
    notify_dashboard()
    {:noreply, state}
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
        case due_retry_attempt_state(state, issue_id, retry_token) do
          {:ok, _attempt, _metadata, state} -> {:noreply, queue_poll_cycle(state)}
          :missing -> {:noreply, state}
        end
      end

    notify_dashboard()
    result
  end

  def handle_info({:retry_issue, _issue_id}, state), do: {:noreply, state}

  def handle_info(:start_pending_workspace_cleanups, state) do
    state = retry_pending_workspace_cleanups(state)
    notify_dashboard()
    {:noreply, state}
  end

  def handle_info({ref, result}, state) when is_reference(ref) do
    case cleanup_task_for_ref(state.cleanup_pending, ref) do
      {issue_id, task_entry} ->
        Process.demonitor(ref, [:flush])

        state =
          state
          |> clear_workspace_cleanup_task(issue_id, task_entry)
          |> apply_workspace_cleanup_result(issue_id, result)
          |> retry_pending_workspace_cleanups()

        notify_dashboard()
        {:noreply, state}

      nil ->
        Logger.debug("Orchestrator ignored message: #{inspect({ref, result})}")
        {:noreply, state}
    end
  end

  def handle_info(
        {:workspace_cleanup_timeout, issue_id, timeout_token},
        %{cleanup_pending: cleanup_pending} = state
      ) do
    case get_in(cleanup_pending, [issue_id, :cleanup_task]) do
      %{timeout_token: ^timeout_token} = task_entry ->
        _result = Task.Supervisor.terminate_child(SymphonyElixir.TaskSupervisor, task_entry.pid)
        Process.demonitor(task_entry.ref, [:flush])

        Logger.warning("Workspace cleanup timed out issue_id=#{issue_id}")

        state =
          state
          |> clear_workspace_cleanup_task(issue_id, task_entry)
          |> apply_workspace_cleanup_result(
            issue_id,
            {:error, :workspace_preservation_required, ""}
          )
          |> retry_pending_workspace_cleanups()

        notify_dashboard()
        {:noreply, state}

      _other ->
        {:noreply, state}
    end
  end

  def handle_info(msg, state) do
    Logger.debug("Orchestrator ignored message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp queue_poll_cycle(%State{poll_task: %{} = _poll_task} = state) do
    %{refresh_runtime_config(state) | poll_dirty: true, poll_check_in_progress: true}
  end

  defp queue_poll_cycle(%State{poll_check_in_progress: true} = state),
    do: refresh_runtime_config(state)

  defp queue_poll_cycle(%State{} = state) do
    state =
      state
      |> refresh_runtime_config()
      |> Map.put(:poll_check_in_progress, true)
      |> Map.put(:next_poll_due_at_ms, nil)

    notify_dashboard()
    :ok = schedule_poll_cycle_start()
    state
  end

  defp start_poll_task(%State{} = state) do
    state =
      state
      |> refresh_runtime_config()
      |> prepare_poll_state()
      |> Map.put(:poll_check_in_progress, true)
      |> Map.put(:next_poll_due_at_ms, nil)

    generation = state.poll_generation + 1
    request = poll_request(state)
    work_fn = state.poll_work_fn || (&collect_tracker_poll/1)

    case start_poll_guard(state, generation, request, work_fn) do
      {:ok, guard_pid} ->
        guard_ref = Process.monitor(guard_pid)
        timeout_token = make_ref()

        timeout_ref =
          Process.send_after(
            self(),
            {:poll_task_timeout, generation, guard_pid, timeout_token},
            poll_task_timeout_ms(state)
          )

        %{
          state
          | poll_generation: generation,
            poll_task: %{
              pid: nil,
              guard_pid: guard_pid,
              guard_ref: guard_ref,
              generation: generation,
              timeout_ref: timeout_ref,
              timeout_token: timeout_token
            }
        }

      {:error, reason} ->
        Logger.warning("Unable to start supervised tracker poll guard: #{inspect(reason)}")
        finish_poll_cycle(state, {:error, {:guard_start_failed, reason}})
    end
  end

  defp start_poll_guard(state, generation, request, work_fn) do
    PollTaskGuard.start(
      self(),
      state.poll_owner_key || __MODULE__,
      generation,
      request,
      work_fn
    )
  catch
    :exit, reason -> {:error, {:supervisor_exit, reason}}
  end

  defp prepare_poll_state(%State{} = state) do
    state
    |> retry_pending_terminal_transitions()
    |> retry_pending_durable_retries()
    |> retry_pending_operator_outcomes()
    |> retry_pending_workspace_cleanups()
    |> reconcile_stalled_running_issues()
    |> ensure_operator_cursors_for_poll()
  end

  defp pin_operator_authority_generation(
         %State{
           operator_commands: %OperatorCommandState{operator_user_ids_generation: nil}
         } = state,
         _configured,
         _authority_generation,
         _tracker_authority_generation
       ),
       do: state

  defp pin_operator_authority_generation(
         %State{
           operator_commands: %OperatorCommandState{
             operator_user_ids_generation: generation,
             operator_authority_generation: pinned_authority_generation,
             operator_authority_invalidated: operator_authority_invalidated,
             tracker_authority_generation: pinned_tracker_authority_generation,
             tracker_authority_invalidated: tracker_authority_invalidated
           }
         } = state,
         configured,
         authority_generation,
         tracker_authority_generation
       ) do
    operator_authority_invalidated =
      operator_authority_invalidated?(
        operator_authority_invalidated,
        generation,
        configured,
        pinned_authority_generation,
        authority_generation
      )

    tracker_authority_invalidated =
      tracker_authority_invalidated?(
        tracker_authority_invalidated,
        pinned_tracker_authority_generation,
        tracker_authority_generation
      )

    log_authority_transition(
      operator_authority_invalidated,
      state.operator_commands.operator_authority_invalidated,
      "Operator command authority changed after startup; commands remain disabled until restart"
    )

    log_authority_transition(
      tracker_authority_invalidated,
      state.operator_commands.tracker_authority_invalidated,
      "Tracker authority changed after startup; all tracker polling remains disabled until restart"
    )

    operator_commands = %{
      state.operator_commands
      | operator_authority_invalidated: operator_authority_invalidated,
        tracker_authority_invalidated: tracker_authority_invalidated
    }

    %{state | operator_commands: operator_commands}
  end

  defp operator_authority_invalidated?(
         already_invalidated,
         pinned_user_ids,
         configured_user_ids,
         pinned_generation,
         current_generation
       ) do
    already_invalidated or
      Enum.sort(configured_user_ids) != Enum.sort(pinned_user_ids) or
      not authority_generation_matches?(pinned_generation, current_generation)
  end

  defp tracker_authority_invalidated?(already_invalidated, pinned_generation, current_generation) do
    already_invalidated or not authority_generation_matches?(pinned_generation, current_generation)
  end

  defp authority_generation_matches?(nil, _current_generation), do: true
  defp authority_generation_matches?(pinned_generation, current_generation), do: pinned_generation == current_generation

  defp log_authority_transition(true, false, message), do: Logger.warning(message)
  defp log_authority_transition(_invalidated, _was_invalidated, _message), do: :ok

  defp ensure_operator_cursors_for_poll(%State{} = state) do
    case operator_user_ids(state) do
      [] ->
        state

      _operator_user_ids ->
        state
        |> operator_command_issue_ids()
        |> Enum.reduce(state, fn issue_id, state_acc ->
          ensure_operator_cursor(state_acc, issue_id)
        end)
    end
  end

  defp poll_request(%State{
         operator_commands: %OperatorCommandState{tracker_authority_invalidated: true}
       }) do
    blocked_tracker_poll_request()
  end

  defp poll_request(
         %State{
           operator_commands: %OperatorCommandState{tracker_context: context}
         } = state
       )
       when is_struct(context, Tracker.PollContext) do
    operator_user_ids = operator_user_ids(state)

    running_ids =
      state.running
      |> Enum.reject(fn {_issue_id, running_entry} ->
        Map.has_key?(running_entry, :terminal_pending)
      end)
      |> Enum.map(&elem(&1, 0))

    retry_issue_ids =
      state.retry_attempts
      |> Enum.flat_map(fn
        {issue_id, %{status: :dispatching}} -> [issue_id]
        _retry -> []
      end)

    %{
      running_ids: running_ids,
      admission_ids: Map.keys(tracker_admissions(state)),
      parked_ids: Map.keys(state.parked),
      retry_issue_ids: retry_issue_ids,
      comment_requests: operator_comment_requests(state, operator_user_ids),
      operator_user_ids: operator_user_ids,
      dispatch_paused: state.dispatch_paused,
      tracker_authority_valid: true,
      tracker_context: context
    }
  end

  defp poll_request(%State{}), do: blocked_tracker_poll_request()

  defp blocked_tracker_poll_request do
    %{
      running_ids: [],
      admission_ids: [],
      parked_ids: [],
      retry_issue_ids: [],
      comment_requests: [],
      operator_user_ids: [],
      dispatch_paused: true,
      tracker_authority_valid: false
    }
  end

  defp tracker_admissions(%State{
         operator_commands: %OperatorCommandState{tracker_admissions: admissions}
       }),
       do: admissions

  defp delete_tracker_admission(
         %State{operator_commands: %OperatorCommandState{} = operator_commands} = state,
         issue_id
       ) do
    operator_commands = %{
      operator_commands
      | tracker_admissions: Map.delete(operator_commands.tracker_admissions, issue_id)
    }

    %{state | operator_commands: operator_commands}
  end

  defp operator_comment_requests(_state, []), do: []

  defp operator_comment_requests(%State{} = state, _operator_user_ids) do
    state
    |> operator_command_issue_ids()
    |> Enum.flat_map(&operator_comment_request(state, &1))
  end

  defp operator_comment_request(state, issue_id) do
    case Map.get(state.operator_comment_cursors, issue_id) do
      %{created_at: %DateTime{} = cursor} -> [{issue_id, cursor}]
      _cursor -> []
    end
  end

  defp collect_tracker_poll(%{tracker_authority_valid: false} = request) do
    %{
      request: request,
      running: {:skip, :tracker_authority_invalidated},
      admissions: {:skip, :tracker_authority_invalidated},
      parked: {:skip, :tracker_authority_invalidated},
      comments: %{},
      dispatch: {:skip, :tracker_authority_invalidated}
    }
  end

  defp collect_tracker_poll(%{tracker_context: tracker_context} = request) do
    if Tracker.authority_valid?(tracker_context) do
      collect_authorized_tracker_poll(request, tracker_context)
    else
      skipped_tracker_poll(request, :tracker_authority_invalidated)
    end
  end

  defp collect_authorized_tracker_poll(request, tracker_context) do
    %{
      request: request,
      running: fetch_issue_states(request.running_ids, tracker_context),
      admissions: fetch_issue_states(Map.get(request, :admission_ids, []), tracker_context),
      parked: fetch_issue_states(request.parked_ids, tracker_context),
      comments: fetch_operator_comments(request.comment_requests, tracker_context),
      dispatch: fetch_dispatch_candidates(request.dispatch_paused, tracker_context)
    }
  end

  defp skipped_tracker_poll(request, reason) do
    %{
      request: request,
      running: {:skip, reason},
      admissions: {:skip, reason},
      parked: {:skip, reason},
      comments: %{},
      dispatch: {:skip, reason}
    }
  end

  defp fetch_issue_states([], _tracker_context), do: {:ok, []}

  defp fetch_issue_states(issue_ids, tracker_context),
    do: Tracker.fetch_issue_states_by_ids(issue_ids, tracker_context)

  defp fetch_operator_comments(comment_requests, tracker_context) do
    Map.new(comment_requests, fn {issue_id, cursor} ->
      {issue_id, Tracker.fetch_comments_since(issue_id, cursor, tracker_context)}
    end)
  end

  defp fetch_dispatch_candidates(true, _tracker_context), do: {:skip, :paused}

  defp fetch_dispatch_candidates(false, tracker_context) do
    with :ok <- Config.validate!(),
         :ok <- Config.validate_runtime_capabilities(),
         {:ok, issues} <- Tracker.fetch_candidate_issues(tracker_context) do
      revalidate_poll_candidates(issues, tracker_context)
    end
  end

  defp revalidate_poll_candidates(issues, tracker_context) when is_list(issues) do
    issue_ids =
      Enum.flat_map(issues, fn
        %Issue{id: issue_id} when is_binary(issue_id) -> [issue_id]
        _issue -> []
      end)

    fetch_issue_states(issue_ids, tracker_context)
  end

  defp revalidate_poll_candidates(_issues, _tracker_context),
    do: {:error, :invalid_candidate_collection}

  defp apply_poll_result(
         %State{} = state,
         %{request: %{tracker_authority_valid: false}}
       ),
       do: state

  defp apply_poll_result(%State{} = state, %{request: request} = result) when is_map(request) do
    state = refresh_runtime_config(state)

    if state.operator_commands.tracker_authority_invalidated do
      state
    else
      effective_operator_user_ids = operator_user_ids(state)

      state
      |> apply_tracker_admission_poll_result(
        request,
        Map.get(result, :admissions)
      )
      |> apply_running_poll_result(
        request.running_ids,
        Map.get(result, :running),
        request.tracker_context.active_states,
        request.tracker_context.terminal_states
      )
      |> apply_parked_poll_result(Map.get(result, :parked))
      |> apply_operator_comment_results(
        effective_operator_user_ids,
        Map.get(result, :comments, %{})
      )
      |> apply_dispatch_poll_result(request, Map.get(result, :dispatch))
    end
  end

  defp apply_poll_result(%State{} = state, _invalid_result), do: state

  defp apply_tracker_admission_poll_result(
         %State{} = state,
         %{tracker_context: %Tracker.PollContext{} = tracker_context},
         {:ok, issues}
       )
       when is_list(issues) do
    Enum.reduce(Map.keys(tracker_admissions(state)), state, fn issue_id, state_acc ->
      admission = Map.get(tracker_admissions(state_acc), issue_id)
      issue = find_issue_by_id(issues, issue_id)

      reconcile_recovered_tracker_admission(state_acc, issue, admission, tracker_context)
    end)
  end

  defp apply_tracker_admission_poll_result(
         %State{} = state,
         _request,
         {:error, reason}
       ) do
    Enum.reduce(tracker_admissions(state), state, fn {_issue_id, admission}, state_acc ->
      park_recovered_tracker_admission(
        state_acc,
        recovered_admission_issue(admission),
        admission,
        tracker_admission_parked_reason(reason)
      )
    end)
  end

  defp apply_tracker_admission_poll_result(%State{} = state, _request, _result), do: state

  defp reconcile_recovered_tracker_admission(
         %State{} = state,
         %Issue{} = issue,
         admission,
         tracker_context
       )
       when is_map(admission) do
    case TrackerAdmission.verify_recovery_evidence(issue, tracker_context, admission) do
      :ok ->
        reconcile_recovered_tracker_state(state, issue, admission, tracker_context)

      {:error, reason} ->
        park_recovered_tracker_admission(
          state,
          issue,
          admission,
          tracker_admission_parked_reason(reason)
        )
    end
  end

  defp reconcile_recovered_tracker_admission(
         %State{} = state,
         _missing_issue,
         admission,
         _tracker_context
       )
       when is_map(admission) do
    park_recovered_tracker_admission(
      state,
      recovered_admission_issue(admission),
      admission,
      "tracker_admission_conflict"
    )
  end

  defp reconcile_recovered_tracker_admission(state, _issue, _admission, _tracker_context),
    do: state

  defp reconcile_recovered_tracker_state(state, issue, admission, tracker_context) do
    cond do
      issue.state == admission.target_state ->
        complete_and_start_recovered_admission(state, issue, admission, tracker_context)

      issue.state == admission.source_state and admission.status == "io_started" ->
        retry_recovered_admission_mutation(state, issue, admission, tracker_context)

      true ->
        park_recovered_tracker_admission(
          state,
          issue,
          admission,
          "tracker_admission_conflict"
        )
    end
  end

  defp retry_recovered_admission_mutation(state, issue, admission, tracker_context) do
    with :ok <-
           tracker_update_issue_state(
             state,
             issue.id,
             admission.target_state,
             tracker_context
           ),
         {:ok, issues} <- tracker_fetch_issues_by_ids(state, [issue.id], tracker_context),
         {:ok, admitted_issue} <-
           TrackerAdmission.verify_readback(
             issues,
             issue.id,
             %{sha256: admission.issue_snapshot_sha256},
             target_state: admission.target_state
           ),
         :ok <-
           TrackerAdmission.verify_recovery_evidence(
             admitted_issue,
             tracker_context,
             admission
           ) do
      complete_and_start_recovered_admission(
        state,
        admitted_issue,
        admission,
        tracker_context
      )
    else
      {:error, reason} ->
        park_recovered_tracker_admission(
          state,
          issue,
          admission,
          tracker_admission_parked_reason(reason)
        )
    end
  end

  defp complete_and_start_recovered_admission(state, issue, admission, tracker_context) do
    with :ok <- maybe_complete_recovered_admission(state, issue, admission),
         {:ok, prepared_workspace} <- prepare_recovered_admission_workspace(issue, admission) do
      start_issue_task(
        state,
        issue,
        admission.attempt,
        self(),
        admission.worker_host,
        prepared_workspace,
        %{
          run_id: admission.run_id,
          normalized_attempt: admission.attempt,
          tracker_context: tracker_context,
          admission: recovered_admission_packet(admission)
        }
      )
    else
      {:error, reason} ->
        park_recovered_tracker_admission(
          state,
          issue,
          admission,
          tracker_admission_parked_reason(reason)
        )
    end
  end

  defp maybe_complete_recovered_admission(_state, _issue, %{status: "completed"}), do: :ok

  defp maybe_complete_recovered_admission(state, issue, %{status: "io_started"} = admission) do
    event =
      admission
      |> recovered_admission_packet()
      |> Map.merge(%{
        transition: "tracker_admission_completed",
        stage: "admission",
        run_id: admission.run_id,
        issue_id: issue.id,
        issue_identifier: issue.identifier,
        attempt: admission.attempt,
        worker_host: admission.worker_host,
        workspace_path: admission.workspace_path,
        workspace_root: admission.workspace_root
      })

    append_run_event(state, event)
  end

  defp maybe_complete_recovered_admission(_state, _issue, _admission),
    do: {:error, :invalid_recovered_admission_status}

  defp prepare_recovered_admission_workspace(issue, admission) do
    Workspace.prepare_for_issue(issue, admission.worker_host,
      expected_workspace_path: admission.workspace_path,
      expected_workspace_root: admission.workspace_root,
      expected_worker_host: admission.worker_host
    )
  end

  defp recovered_admission_packet(admission) do
    Map.take(admission, [
      :admission_id,
      :issue_snapshot_bytes,
      :issue_snapshot_schema,
      :issue_snapshot_sha256,
      :source_state,
      :target_state,
      :tracker_authority_digest
    ])
  end

  defp recovered_admission_issue(admission) do
    %Issue{
      id: admission.issue_id,
      identifier: admission.identifier,
      title: admission.identifier,
      state: admission.source_state,
      assigned_to_worker: true
    }
  end

  defp park_recovered_tracker_admission(state, issue, admission, reason) do
    park_claimed_issue(
      state,
      issue,
      admission.worker_host,
      %{path: admission.workspace_path, root: admission.workspace_root},
      admission.run_id,
      admission.attempt,
      reason
    )
  end

  defp apply_running_poll_result(
         state,
         running_ids,
         {:ok, issues},
         active_states,
         terminal_states
       )
       when is_list(issues) do
    issues
    |> reconcile_running_issue_states(
      state,
      active_state_set(active_states),
      terminal_state_set(terminal_states)
    )
    |> reconcile_missing_running_issue_ids(running_ids, issues)
  end

  defp apply_running_poll_result(state, _running_ids, {:error, reason}, _active_states, _terminal_states) do
    Logger.debug("Failed to refresh running issue states: #{inspect(reason)}; keeping active workers")
    state
  end

  defp apply_running_poll_result(
         state,
         _running_ids,
         _invalid_result,
         _active_states,
         _terminal_states
       ),
       do: state

  defp apply_parked_poll_result(state, {:ok, issues}) when is_list(issues),
    do: Enum.reduce(issues, state, &reconcile_parked_issue/2)

  defp apply_parked_poll_result(state, {:error, reason}) do
    Logger.debug("Failed to refresh parked issue states: #{inspect(reason)}; keeping operator waits")
    state
  end

  defp apply_parked_poll_result(state, _invalid_result), do: state

  defp apply_operator_comment_results(state, [], _comment_results), do: state

  defp apply_operator_comment_results(state, operator_user_ids, comment_results)
       when is_list(operator_user_ids) and is_map(comment_results) do
    Enum.reduce(comment_results, state, fn
      {issue_id, {:ok, comments}}, state_acc when is_list(comments) ->
        comments
        |> Enum.sort_by(&operator_comment_sort_key/1)
        |> Enum.reduce(state_acc, fn comment, comment_state ->
          process_operator_comment(comment_state, issue_id, comment, operator_user_ids)
        end)

      {issue_id, {:error, reason}}, state_acc ->
        Logger.debug("Failed to fetch operator comments issue_id=#{issue_id}: #{inspect(reason)}")
        state_acc

      {_issue_id, _invalid_result}, state_acc ->
        state_acc
    end)
  end

  defp apply_operator_comment_results(state, _operator_user_ids, _comment_results), do: state

  defp apply_dispatch_poll_result(state, _request, {:skip, :paused}), do: state

  defp apply_dispatch_poll_result(%State{dispatch_paused: true} = state, request, _result) do
    Enum.reduce(request.retry_issue_ids, state, fn issue_id, state_acc ->
      defer_pending_dispatch(state_acc, issue_id, :dispatch_paused)
    end)
  end

  defp apply_dispatch_poll_result(state, request, {:ok, issues}) when is_list(issues) do
    state = apply_due_retry_candidates(state, request.retry_issue_ids, issues, request.tracker_context)

    if available_slots(state) > 0 do
      choose_issues(issues, state, request.tracker_context)
    else
      state
    end
  end

  defp apply_dispatch_poll_result(state, request, {:error, reason}) do
    log_dispatch_poll_error(reason)

    Enum.reduce(request.retry_issue_ids, state, fn issue_id, state_acc ->
      defer_pending_dispatch(state_acc, issue_id, {:retry_poll_failed, reason})
    end)
  end

  defp apply_dispatch_poll_result(state, _request, _invalid_result), do: state

  defp apply_due_retry_candidates(state, retry_issue_ids, issues, tracker_context) do
    Enum.reduce(retry_issue_ids, state, fn issue_id, state_acc ->
      case Map.get(state_acc.retry_attempts, issue_id) do
        %{status: :dispatching, attempt: attempt} = retry_entry ->
          metadata = retry_metadata(retry_entry)
          issue = find_issue_by_id(issues, issue_id)

          {:noreply, next_state} =
            handle_retry_issue_lookup(
              issue,
              state_acc,
              issue_id,
              attempt,
              metadata,
              tracker_context
            )

          next_state

        _retry ->
          state_acc
      end
    end)
  end

  defp retry_metadata(retry_entry) do
    %{
      identifier: Map.get(retry_entry, :identifier),
      error: Map.get(retry_entry, :error),
      previous_run_id: Map.get(retry_entry, :previous_run_id),
      previous_attempt: Map.get(retry_entry, :previous_attempt),
      next_action: Map.get(retry_entry, :next_action),
      worker_host: Map.get(retry_entry, :worker_host),
      workspace_path: Map.get(retry_entry, :workspace_path),
      workspace_root: Map.get(retry_entry, :workspace_root)
    }
  end

  defp log_dispatch_poll_error(:missing_linear_api_token),
    do: Logger.error("Linear API token missing in WORKFLOW.md")

  defp log_dispatch_poll_error(:missing_linear_project_slug),
    do: Logger.error("Linear project slug missing in WORKFLOW.md")

  defp log_dispatch_poll_error(:missing_tracker_kind),
    do: Logger.error("Tracker kind missing in WORKFLOW.md")

  defp log_dispatch_poll_error({:unsupported_tracker_kind, kind}),
    do: Logger.error("Unsupported tracker kind in WORKFLOW.md: #{inspect(kind)}")

  defp log_dispatch_poll_error({:invalid_workflow_config, message}),
    do: Logger.error("Invalid WORKFLOW.md config: #{message}")

  defp log_dispatch_poll_error({:missing_required_dynamic_tools, tools}),
    do: Logger.error("Runtime capability preflight blocked dispatch: missing_required_dynamic_tools=#{Enum.join(tools, ",")}")

  defp log_dispatch_poll_error({:missing_workflow_file, path, reason}),
    do: Logger.error("Missing WORKFLOW.md at #{path}: #{inspect(reason)}")

  defp log_dispatch_poll_error(:workflow_front_matter_not_a_map),
    do: Logger.error("Failed to parse WORKFLOW.md: workflow front matter must decode to a map")

  defp log_dispatch_poll_error({:workflow_parse_error, reason}),
    do: Logger.error("Failed to parse WORKFLOW.md: #{inspect(reason)}")

  defp log_dispatch_poll_error(reason),
    do: Logger.error("Failed to fetch from tracker: #{inspect(reason)}")

  defp reconcile_parked_issue(%Issue{} = issue, %State{} = state) do
    wait = Map.get(state.parked, issue.id)

    cond do
      is_map(wait) and Map.get(wait, :reason) == "operator_stopped" ->
        update_in(state.parked[issue.id], fn
          nil -> nil
          parked_wait -> %{parked_wait | tracker_state: issue.state, identifier: issue.identifier}
        end)

      terminal_issue_state?(issue.state, terminal_state_set()) ->
        release_terminal_parked_issue(state, issue.id)

      !issue_routable_to_worker?(issue) ->
        release_parked_issue(state, issue.id, "worker_route_removed")

      true ->
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
    issues
    |> reconcile_running_issue_states(state, active_state_set(), terminal_state_set())
    |> retry_pending_workspace_cleanups_sync()
  end

  def reconcile_issue_states_for_test(issues, state) when is_list(issues) do
    issues
    |> reconcile_running_issue_states(state, active_state_set(), terminal_state_set())
    |> retry_pending_workspace_cleanups_sync()
  end

  @doc false
  @spec reconcile_parked_issue_for_test(Issue.t(), term()) :: term()
  def reconcile_parked_issue_for_test(%Issue{} = issue, %State{} = state) do
    issue
    |> reconcile_parked_issue(state)
    |> retry_pending_workspace_cleanups_sync()
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
  @spec revalidate_poll_candidates_for_test(term(), Tracker.PollContext.t()) ::
          {:ok, [term()]} | {:error, term()}
  def revalidate_poll_candidates_for_test(issues, %Tracker.PollContext{} = tracker_context) do
    revalidate_poll_candidates(issues, tracker_context)
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
    state
    |> retry_pending_terminal_transitions()
    |> retry_pending_workspace_cleanups_sync()
  end

  @doc false
  @spec retry_pending_durable_retries_for_test(term()) :: term()
  def retry_pending_durable_retries_for_test(%State{} = state) do
    retry_pending_durable_retries(state)
  end

  @doc false
  @spec retry_pending_operator_outcomes_for_test(term()) :: term()
  def retry_pending_operator_outcomes_for_test(%State{} = state) do
    retry_pending_operator_outcomes(state)
  end

  @doc false
  @spec claim_and_start_issue_for_test(
          term(),
          Issue.t(),
          non_neg_integer(),
          String.t() | nil,
          Path.t() | nil,
          Path.t() | nil,
          String.t() | nil,
          Tracker.PollContext.t()
        ) :: term()
  def claim_and_start_issue_for_test(
        %State{} = state,
        %Issue{} = issue,
        attempt,
        worker_host,
        expected_workspace_path,
        expected_workspace_root,
        expected_worker_host,
        %Tracker.PollContext{} = tracker_context
      ) do
    claim_and_start_issue(
      state,
      issue,
      attempt,
      self(),
      worker_host,
      expected_workspace_path,
      expected_workspace_root,
      %{
        expected_worker_host: expected_worker_host,
        tracker_context: tracker_context
      }
    )
  end

  @doc false
  @spec due_retry_attempt_state_for_test(term(), String.t(), reference()) :: term()
  def due_retry_attempt_state_for_test(%State{} = state, issue_id, retry_token)
      when is_binary(issue_id) and is_reference(retry_token) do
    due_retry_attempt_state(state, issue_id, retry_token)
  end

  @doc false
  @spec run_poll_cycle_for_test(term(), Tracker.PollContext.t()) :: term()
  def run_poll_cycle_for_test(%State{} = state, %Tracker.PollContext{} = tracker_context) do
    state = %{
      state
      | operator_commands: %{
          state.operator_commands
          | tracker_context: tracker_context
        }
    }

    run_test_poll_cycle(state)
  end

  @doc false
  @spec reconcile_tracker_admissions_for_test(
          term(),
          {:ok, [Issue.t()]} | {:error, term()},
          Tracker.PollContext.t()
        ) :: term()
  def reconcile_tracker_admissions_for_test(
        %State{} = state,
        result,
        %Tracker.PollContext{} = tracker_context
      ) do
    apply_tracker_admission_poll_result(state, %{tracker_context: tracker_context}, result)
  end

  @doc false
  @spec run_poll_cycle_without_tracker_context_for_test(term()) :: term()
  def run_poll_cycle_without_tracker_context_for_test(%State{} = state) do
    state = %{
      state
      | operator_commands: %{
          state.operator_commands
          | tracker_context: nil
        }
    }

    run_test_poll_cycle(state)
  end

  defp run_test_poll_cycle(%State{} = state) do
    state =
      state
      |> refresh_runtime_config()
      |> prepare_poll_state()
      |> Map.put(:poll_check_in_progress, true)

    request = poll_request(state)

    state
    |> apply_poll_result(collect_tracker_poll(request))
    |> finish_poll_cycle(:ok)
  end

  @doc false
  @spec apply_poll_result_for_test(term(), map()) :: term()
  def apply_poll_result_for_test(%State{} = state, result) when is_map(result),
    do: apply_poll_result(state, result)

  @doc false
  @spec collect_tracker_poll_for_test(map()) :: map()
  def collect_tracker_poll_for_test(request) when is_map(request),
    do: collect_tracker_poll(request)

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
                 workspace_path: Map.get(running_entry, :workspace_path),
                 workspace_root: Map.get(running_entry, :workspace_root)
               }),
             park_event =
               wait
               |> operator_wait_event("run_parked")
               |> maybe_put_operator_command_context(
                 Keyword.get(opts, :operator_comment),
                 Keyword.get(opts, :operator_action)
               ),
             :ok <- append_run_event(state, park_event) do
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

  defp release_terminal_parked_issue(%State{} = state, issue_id) do
    case persist_parked_release(state, issue_id, "tracker_terminal") do
      {:ok, released_state, wait} ->
        cleanup_metadata = %{
          run_id: wait.run_id,
          attempt: wait.attempt,
          identifier: wait.identifier,
          worker_host: Map.get(wait, :worker_host),
          workspace_path: Map.get(wait, :workspace_path),
          workspace_root: Map.get(wait, :workspace_root)
        }

        request_workspace_cleanup(released_state, issue_id, cleanup_metadata, "tracker_terminal")

      {:error, state} ->
        state
    end
  end

  defp release_parked_issue(%State{} = state, issue_id, release_reason) do
    case persist_parked_release(state, issue_id, release_reason) do
      {:ok, released_state, _wait} -> released_state
      {:error, state} -> state
    end
  end

  defp persist_parked_release(%State{} = state, issue_id, release_reason) do
    case Map.get(state.parked, issue_id) do
      nil ->
        {:error, state}

      wait ->
        event =
          wait
          |> operator_wait_event("wait_released")
          |> Map.put(:release_reason, release_reason)

        case append_run_event(state, event) do
          :ok ->
            {:ok, %{state | parked: Map.delete(state.parked, issue_id)}, wait}

          {:error, reason} ->
            Logger.error("Failed to release parked issue_id=#{issue_id}: #{inspect(reason)}")
            {:error, state}
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
            workspace_path: Map.get(running_entry, :workspace_path),
            workspace_root: Map.get(running_entry, :workspace_root)
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

  defp choose_issues(issues, state, tracker_context) do
    active_states = active_state_set(tracker_context.active_states)
    terminal_states = terminal_state_set(tracker_context.terminal_states)

    issues
    |> sort_issues_for_dispatch()
    |> Enum.reduce(state, fn issue, state_acc ->
      if should_dispatch_issue?(issue, state_acc, active_states, terminal_states) do
        dispatch = dispatch_context(state_acc, issue.id)

        do_dispatch_issue(
          state_acc,
          issue,
          dispatch.attempt,
          dispatch.worker_host,
          dispatch.workspace_path,
          dispatch.workspace_root,
          dispatch.affinity_required,
          tracker_context
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
      issue_available_for_dispatch?(state, issue.id, claimed, running, parked) and
      dispatch_capacity_available?(state, issue, running)
  end

  defp should_dispatch_issue?(_issue, _state, _active_states, _terminal_states), do: false

  defp issue_available_for_dispatch?(state, issue_id, claimed, running, parked) do
    !MapSet.member?(claimed, issue_id) and
      !Map.has_key?(running, issue_id) and
      !Map.has_key?(parked, issue_id) and
      !Map.has_key?(state.retry_attempts, issue_id)
  end

  defp dispatch_capacity_available?(state, issue, running) do
    available_slots(state) > 0 and
      state_slots_available?(issue, running) and
      worker_slots_available?(state)
  end

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
    terminal_state_set(Config.settings!().tracker.terminal_states)
  end

  defp terminal_state_set(terminal_states) when is_list(terminal_states) do
    terminal_states
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> Enum.reject(&(OperatorWait.reason_for_tracker_state(&1) != nil))
    |> MapSet.new()
  end

  defp active_state_set do
    active_state_set(Config.settings!().tracker.active_states)
  end

  defp active_state_set(active_states) when is_list(active_states) do
    active_states
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> MapSet.new()
  end

  defp do_dispatch_issue(
         %State{} = state,
         issue,
         attempt,
         preferred_worker_host,
         expected_workspace_path,
         expected_workspace_root,
         affinity_required,
         tracker_context
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
            expected_workspace_path,
            expected_workspace_root,
            %{
              expected_worker_host: preferred_worker_host,
              tracker_context: tracker_context
            }
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
         expected_workspace_path,
         expected_workspace_root,
         dispatch_context
       ) do
    case Config.validate_runtime_capabilities() do
      :ok ->
        claim_and_start_issue(
          state,
          issue,
          attempt,
          recipient,
          worker_host,
          expected_workspace_path,
          expected_workspace_root,
          dispatch_context
        )

      {:error, {:missing_required_dynamic_tools, tools}} ->
        Logger.error("Runtime capability preflight blocked claim for #{issue_context(issue)}: missing_required_dynamic_tools=#{Enum.join(tools, ",")}")
        defer_pending_dispatch(state, issue.id, :missing_required_dynamic_tools)

      {:error, reason} ->
        Logger.error("Runtime capability preflight failed for #{issue_context(issue)}: #{inspect(reason)}")
        defer_pending_dispatch(state, issue.id, reason)
    end
  end

  defp claim_and_start_issue(
         %State{} = state,
         issue,
         attempt,
         recipient,
         worker_host,
         expected_workspace_path,
         expected_workspace_root,
         dispatch_context
       ) do
    expected_worker_host = dispatch_context.expected_worker_host

    case Workspace.prepare_for_issue(issue, worker_host,
           expected_workspace_path: expected_workspace_path,
           expected_workspace_root: expected_workspace_root,
           expected_worker_host: expected_worker_host
         ) do
      {:ok, prepared_workspace} ->
        claim_prepared_issue(
          state,
          issue,
          attempt,
          recipient,
          worker_host,
          prepared_workspace,
          dispatch_context.tracker_context
        )

      {:error, reason} ->
        Logger.error("Workspace preparation blocked claim for #{issue_context(issue)}: #{inspect(reason)}")
        defer_pending_dispatch(state, issue.id, reason)
    end
  end

  defp claim_prepared_issue(
         state,
         issue,
         attempt,
         recipient,
         worker_host,
         prepared_workspace,
         tracker_context
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
      workspace_path: prepared_workspace.path,
      workspace_root: prepared_workspace.root
    }

    case append_run_event(state, claim_event) do
      :ok ->
        state = consume_dispatch_queue(state, issue.id)

        run_context = %{
          run_id: run_id,
          normalized_attempt: normalized_attempt,
          tracker_context: tracker_context,
          admission: nil
        }

        if tracker_admission_required?(issue) do
          admit_and_start_issue(
            state,
            issue,
            attempt,
            recipient,
            worker_host,
            prepared_workspace,
            run_context
          )
        else
          start_issue_task(
            state,
            issue,
            attempt,
            recipient,
            worker_host,
            prepared_workspace,
            run_context
          )
        end

      {:error, reason} ->
        Logger.error("Unable to record durable claim for #{issue_context(issue)}: #{inspect(reason)}")
        defer_pending_dispatch(state, issue.id, reason)
    end
  end

  defp tracker_admission_required?(%Issue{state: state}),
    do: normalize_issue_state(state) == "agent ready"

  defp admit_and_start_issue(
         state,
         issue,
         attempt,
         recipient,
         worker_host,
         prepared_workspace,
         %{run_id: run_id, normalized_attempt: normalized_attempt, tracker_context: tracker_context} =
           run_context
       ) do
    admission_id = RunLedger.new_id("admission")
    target_state = "Agent Running"

    with {:ok, admission, snapshot} <-
           TrackerAdmission.packet(issue, tracker_context, admission_id, target_state),
         admission_event =
           tracker_admission_event(
             issue,
             worker_host,
             prepared_workspace,
             run_id,
             normalized_attempt,
             admission
           ),
         :ok <-
           append_run_event(
             state,
             Map.put(admission_event, :transition, "tracker_admission_io_started")
           ),
         :ok <- tracker_update_issue_state(state, issue.id, target_state, tracker_context),
         {:ok, issues} <- tracker_fetch_issues_by_ids(state, [issue.id], tracker_context),
         {:ok, admitted_issue} <-
           TrackerAdmission.verify_readback(issues, issue.id, snapshot, target_state: target_state),
         :ok <-
           append_run_event(
             state,
             Map.put(admission_event, :transition, "tracker_admission_completed")
           ) do
      start_issue_task(
        state,
        admitted_issue,
        attempt,
        recipient,
        worker_host,
        prepared_workspace,
        %{run_context | admission: admission}
      )
    else
      {:error, reason} ->
        parked_reason = tracker_admission_parked_reason(reason)

        Logger.warning("Tracker admission blocked model start for #{issue_context(issue)} reason=#{parked_reason} error_code=#{ObservabilitySanitizer.error_code(reason, "tracker_admission_failed")}")

        park_claimed_issue(
          state,
          issue,
          worker_host,
          prepared_workspace,
          run_id,
          normalized_attempt,
          parked_reason
        )
    end
  end

  defp tracker_admission_event(
         issue,
         worker_host,
         prepared_workspace,
         run_id,
         attempt,
         admission
       ) do
    admission
    |> Map.merge(%{
      stage: "admission",
      run_id: run_id,
      issue_id: issue.id,
      issue_identifier: issue.identifier,
      attempt: attempt,
      worker_host: worker_host,
      workspace_path: prepared_workspace.path,
      workspace_root: prepared_workspace.root
    })
  end

  defp tracker_update_issue_state(
         %State{
           operator_commands: %OperatorCommandState{tracker_update_state_fn: update_fn}
         },
         issue_id,
         target_state,
         tracker_context
       )
       when is_function(update_fn, 3),
       do: update_fn.(issue_id, target_state, tracker_context)

  defp tracker_update_issue_state(_state, issue_id, target_state, tracker_context),
    do: Tracker.update_issue_state(issue_id, target_state, tracker_context)

  defp tracker_fetch_issues_by_ids(
         %State{
           operator_commands: %OperatorCommandState{tracker_fetch_by_ids_fn: fetch_fn}
         },
         issue_ids,
         tracker_context
       )
       when is_function(fetch_fn, 2),
       do: fetch_fn.(issue_ids, tracker_context)

  defp tracker_fetch_issues_by_ids(_state, issue_ids, tracker_context),
    do: Tracker.fetch_issue_states_by_ids(issue_ids, tracker_context)

  defp tracker_admission_parked_reason(reason)
       when reason in [
              :issue_not_found,
              :issue_snapshot_conflict,
              :target_state_not_observed
            ],
       do: "tracker_admission_conflict"

  defp tracker_admission_parked_reason(_reason), do: "tracker_admission_failed"

  defp park_claimed_issue(
         state,
         issue,
         worker_host,
         prepared_workspace,
         run_id,
         attempt,
         reason
       ) do
    with {:ok, wait} <-
           OperatorWait.new(reason, %{
             issue_id: issue.id,
             identifier: issue.identifier,
             run_id: run_id,
             attempt: attempt,
             tracker_state: issue.state,
             worker_host: worker_host,
             workspace_path: prepared_workspace.path,
             workspace_root: prepared_workspace.root
           }),
         :ok <- append_run_event(state, operator_wait_event(wait, "run_parked")) do
      %{
        state
        | parked: Map.put(state.parked, issue.id, wait),
          claimed: MapSet.delete(state.claimed, issue.id),
          retry_attempts: Map.delete(state.retry_attempts, issue.id)
      }
      |> delete_tracker_admission(issue.id)
    else
      {:error, error} ->
        Logger.error("Failed to durably park claimed issue_id=#{issue.id} reason=#{reason}: #{inspect(error)}")

        %{state | claimed: MapSet.put(state.claimed, issue.id)}
    end
  end

  defp start_issue_task(
         state,
         issue,
         attempt,
         recipient,
         worker_host,
         prepared_workspace,
         %{
           run_id: run_id,
           normalized_attempt: normalized_attempt,
           tracker_context: tracker_context,
           admission: admission
         }
       ) do
    run_budget = RunBudget.from_agent_config(Config.settings!().agent)

    task = fn ->
      AgentRunner.run(issue, recipient,
        attempt: attempt,
        worker_host: worker_host,
        expected_workspace_path: prepared_workspace.path,
        prepared_workspace: prepared_workspace,
        runtime_ack_required: true,
        run_id: run_id,
        runner_generation: state.runner_generation,
        stage: "running",
        max_turns: run_budget.max_turns,
        admission: admission,
        tracker_context: tracker_context
      )
    end

    case start_agent_task(state, task) do
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
          workspace_path: prepared_workspace.path,
          workspace_root: prepared_workspace.root,
          session_id: nil,
          session_title: nil,
          admission: admission,
          resolved_model: nil,
          reasoning_effort: nil,
          model_catalog_source: nil,
          model_catalog: nil,
          last_codex_message: nil,
          last_codex_timestamp: nil,
          last_codex_event: nil,
          codex_app_server_pid: nil,
          codex_input_tokens: 0,
          codex_cached_input_tokens: 0,
          codex_uncached_input_tokens: 0,
          codex_output_tokens: 0,
          codex_total_tokens: 0,
          codex_last_reported_input_tokens: 0,
          codex_last_reported_cached_input_tokens: 0,
          codex_last_reported_uncached_input_tokens: 0,
          codex_last_reported_output_tokens: 0,
          codex_last_reported_total_tokens: 0,
          codex_token_accounting: new_token_accounting(),
          codex_token_telemetry_observed: false,
          codex_token_telemetry_integrity: :unobserved,
          codex_token_telemetry_failure: nil,
          codex_token_telemetry_epoch: 0,
          codex_uncached_input_telemetry_observed: false,
          codex_uncached_input_telemetry_integrity: :unobserved,
          codex_uncached_input_telemetry_failure: nil,
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
            |> delete_tracker_admission(issue.id)
            |> initialize_operator_cursor(issue.id, running_entry.started_at)

          {:error, reason} ->
            Logger.error("Unable to record durable run start for #{issue_context(issue)}: #{inspect(reason)}")
            terminate_task(pid)
            Process.demonitor(ref, [:flush])
            fail_claimed_start(state, issue.id, running_entry, reason)
        end

      {:error, reason} ->
        Logger.error("Unable to spawn agent for #{issue_context(issue)}: #{inspect(reason)}")
        next_attempt = normalized_attempt + 1

        failed_entry = %{
          run_id: run_id,
          pid: nil,
          ref: nil,
          identifier: issue.identifier,
          issue: issue,
          worker_host: worker_host,
          workspace_path: prepared_workspace.path,
          workspace_root: prepared_workspace.root,
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
               workspace_path: prepared_workspace.path,
               workspace_root: prepared_workspace.root
             }}
        }

        state = %{state | claimed: MapSet.put(state.claimed, issue.id)}
        persist_or_block_terminal(state, issue.id, failed_entry, pending)
    end
  end

  defp start_agent_task(%State{task_start_fn: task_start_fn}, task) when is_function(task_start_fn, 1),
    do: task_start_fn.(task)

  defp start_agent_task(_state, task),
    do: Task.Supervisor.start_child(SymphonyElixir.TaskSupervisor, task)

  defp fail_claimed_start(state, issue_id, running_entry, reason) do
    attempt = Map.get(running_entry, :retry_attempt, 0)

    pending = %{
      transition: "run_failed",
      terminal_reason: "spawn_failed",
      action:
        {:retry, attempt + 1,
         %{
           identifier: running_entry.identifier,
           error: "failed to persist run start: #{inspect(reason)}",
           previous_run_id: running_entry.run_id,
           previous_attempt: attempt,
           worker_host: running_entry.worker_host,
           workspace_path: running_entry.workspace_path,
           workspace_root: running_entry.workspace_root
         }}
    }

    state
    |> Map.put(:claimed, MapSet.put(state.claimed, issue_id))
    |> persist_or_block_terminal(issue_id, running_entry, pending)
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
    schedule = build_retry_schedule(issue_id, next_attempt, previous_retry, metadata)

    case append_run_event(state, schedule.event) do
      :ok -> finalize_retry_schedule(state, issue_id, schedule)
      {:error, reason} -> retain_pending_retry(state, issue_id, schedule, reason)
    end
  end

  defp build_retry_schedule(issue_id, next_attempt, previous_retry, metadata) do
    delay_ms = retry_delay(next_attempt, metadata)
    identifier = pick_retry_identifier(issue_id, previous_retry, metadata)
    error = sanitized_retry_error(previous_retry, metadata)
    worker_host = pick_retry_worker_host(previous_retry, metadata)
    workspace_path = pick_retry_workspace_path(previous_retry, metadata)
    workspace_root = pick_retry_workspace_root(previous_retry, metadata)
    previous_run_id = metadata[:previous_run_id] || Map.get(previous_retry, :previous_run_id)
    previous_attempt = Map.get(metadata, :previous_attempt, Map.get(previous_retry, :previous_attempt, 0))

    event = %{
      transition: "retry_scheduled",
      stage: "retry_queued",
      run_id: previous_run_id,
      issue_id: issue_id,
      issue_identifier: identifier,
      attempt: previous_attempt,
      next_action: metadata[:next_action] || Map.get(previous_retry, :next_action),
      next_attempt: next_attempt,
      worker_host: worker_host,
      workspace_path: workspace_path,
      workspace_root: workspace_root
    }

    entry = %{
      attempt: next_attempt,
      timer_ref: nil,
      retry_token: nil,
      due_at_ms: nil,
      identifier: identifier,
      error: error,
      previous_run_id: previous_run_id,
      previous_attempt: previous_attempt,
      next_action: event.next_action,
      worker_host: worker_host,
      workspace_path: workspace_path,
      workspace_root: workspace_root,
      pending_event: event,
      status: :durability_pending
    }

    %{
      delay_ms: delay_ms,
      due_at_ms: System.monotonic_time(:millisecond) + delay_ms,
      entry: entry,
      event: event,
      old_timer: Map.get(previous_retry, :timer_ref),
      retry_token: make_ref()
    }
  end

  defp sanitized_retry_error(previous_retry, metadata) do
    case pick_retry_error(previous_retry, metadata) do
      nil -> nil
      value -> ObservabilitySanitizer.retry_error_code(value)
    end
  end

  defp finalize_retry_schedule(state, issue_id, schedule) do
    if is_reference(schedule.old_timer), do: Process.cancel_timer(schedule.old_timer)

    timer_ref =
      Process.send_after(self(), {:retry_issue, issue_id, schedule.retry_token}, schedule.delay_ms)

    error_suffix = if is_binary(schedule.entry.error), do: " error=#{schedule.entry.error}", else: ""

    Logger.warning("Retrying issue_id=#{issue_id} issue_identifier=#{schedule.entry.identifier} in #{schedule.delay_ms}ms (attempt #{schedule.entry.attempt})#{error_suffix}")

    scheduled_entry = %{
      schedule.entry
      | timer_ref: timer_ref,
        retry_token: schedule.retry_token,
        due_at_ms: schedule.due_at_ms,
        pending_event: nil,
        status: :scheduled
    }

    {:ok, put_retry_entry(state, issue_id, scheduled_entry)}
  end

  defp retain_pending_retry(state, issue_id, schedule, reason) do
    if is_reference(schedule.old_timer), do: Process.cancel_timer(schedule.old_timer)

    Logger.error("Failed to append Symphony retry ledger event issue_id=#{issue_id}: #{inspect(reason)}; retaining pending durable retry and claim")

    pending_entry = Map.put(schedule.entry, :persistence_error, reason)
    {:error, put_retry_entry(state, issue_id, pending_entry)}
  end

  defp put_retry_entry(state, issue_id, retry_entry) do
    %{
      state
      | retry_attempts: Map.put(state.retry_attempts, issue_id, retry_entry),
        claimed: MapSet.put(state.claimed, issue_id)
    }
  end

  defp retry_schedule_state({:ok, state}), do: state
  defp retry_schedule_state({:error, state}), do: state

  defp retry_pending_durable_retries(%State{} = state) do
    Enum.reduce(state.retry_attempts, state, fn
      {issue_id, %{status: :durability_pending} = retry}, state_acc ->
        metadata = %{
          identifier: retry.identifier,
          error: retry.error,
          previous_run_id: retry.previous_run_id,
          previous_attempt: retry.previous_attempt,
          next_action: retry.next_action,
          worker_host: retry.worker_host,
          workspace_path: retry.workspace_path,
          workspace_root: retry.workspace_root
        }

        state_acc
        |> schedule_issue_retry(issue_id, retry.attempt, metadata)
        |> retry_schedule_state()

      {_issue_id, _retry}, state_acc ->
        state_acc
    end)
  end

  defp due_retry_attempt_state(%State{} = state, issue_id, retry_token) when is_reference(retry_token) do
    case Map.get(state.retry_attempts, issue_id) do
      %{attempt: attempt, retry_token: ^retry_token} = retry_entry ->
        metadata = %{
          identifier: Map.get(retry_entry, :identifier),
          error: Map.get(retry_entry, :error),
          previous_run_id: Map.get(retry_entry, :previous_run_id),
          previous_attempt: Map.get(retry_entry, :previous_attempt),
          next_action: Map.get(retry_entry, :next_action),
          worker_host: Map.get(retry_entry, :worker_host),
          workspace_path: Map.get(retry_entry, :workspace_path),
          workspace_root: Map.get(retry_entry, :workspace_root)
        }

        dispatching_entry = %{
          retry_entry
          | timer_ref: nil,
            retry_token: nil,
            due_at_ms: nil,
            status: :dispatching
        }

        {:ok, attempt, metadata, %{state | retry_attempts: Map.put(state.retry_attempts, issue_id, dispatching_entry)}}

      _ ->
        :missing
    end
  end

  defp defer_pending_dispatch(%State{} = state, issue_id, reason) do
    case Map.get(state.retry_attempts, issue_id) do
      nil ->
        state

      retry ->
        if is_reference(Map.get(retry, :timer_ref)), do: Process.cancel_timer(retry.timer_ref)

        delay_ms = failure_retry_delay(max(Map.get(retry, :attempt, 1), 1))
        retry_token = make_ref()

        updated_retry =
          retry
          |> Map.put(:timer_ref, Process.send_after(self(), {:retry_issue, issue_id, retry_token}, delay_ms))
          |> Map.put(:retry_token, retry_token)
          |> Map.put(:due_at_ms, System.monotonic_time(:millisecond) + delay_ms)
          |> Map.put(:status, :scheduled)
          |> Map.put(:error, ObservabilitySanitizer.error_code(reason, "dispatch_failed"))

        %{state | retry_attempts: Map.put(state.retry_attempts, issue_id, updated_retry)}
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
        if Map.get(retry, :status, :scheduled) == :scheduled do
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
        else
          {issue_id, retry}
        end
      end)

    %{state | retry_attempts: retry_attempts}
  end

  defp handle_retry_issue_lookup(
         %Issue{} = issue,
         state,
         issue_id,
         attempt,
         metadata,
         tracker_context
       ) do
    active_states = active_state_set(tracker_context.active_states)
    terminal_states = terminal_state_set(tracker_context.terminal_states)

    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info(
          "Issue state is terminal while retry is pending: issue_id=#{issue_id} issue_identifier=#{issue.identifier} state=#{issue.state}; preserving recorded workspace and bounded dispatch visibility"
        )

        {:noreply, defer_pending_dispatch(state, issue_id, :retry_issue_terminal)}

      retry_candidate_issue?(issue, active_states, terminal_states) ->
        handle_active_retry(
          state,
          issue,
          attempt,
          metadata,
          active_states,
          terminal_states,
          tracker_context
        )

      true ->
        Logger.debug("Issue left active states while retry was pending issue_id=#{issue_id} issue_identifier=#{issue.identifier}; keeping bounded dispatch visibility")

        {:noreply, defer_pending_dispatch(state, issue_id, :retry_issue_not_active)}
    end
  end

  defp handle_retry_issue_lookup(nil, state, issue_id, _attempt, _metadata, _tracker_context) do
    Logger.debug("Issue no longer visible while retry was pending issue_id=#{issue_id}; keeping bounded dispatch visibility")
    {:noreply, defer_pending_dispatch(state, issue_id, :retry_issue_not_visible)}
  end

  defp notify_dashboard do
    StatusDashboard.notify_update()
  end

  defp handle_active_retry(
         state,
         issue,
         attempt,
         metadata,
         active_states,
         terminal_states,
         tracker_context
       ) do
    affinity_required = valid_expected_workspace_path?(metadata[:workspace_path])

    if retry_candidate_issue?(issue, active_states, terminal_states) and
         dispatch_slots_available?(issue, state) and
         worker_slots_available?(state, metadata[:worker_host], affinity_required) do
      {:noreply,
       do_dispatch_issue(
         state,
         issue,
         attempt,
         metadata[:worker_host],
         metadata[:workspace_path],
         metadata[:workspace_root],
         affinity_required,
         tracker_context
       )}
    else
      Logger.debug("No available slots for retrying #{issue_context(issue)}; retrying again")

      next_state =
        state
        |> schedule_issue_retry(
          issue.id,
          attempt,
          Map.merge(metadata, %{
            identifier: issue.identifier,
            error: "no available orchestrator slots"
          })
        )
        |> retry_schedule_state()

      {:noreply, next_state}
    end
  end

  defp release_issue_claim(%State{} = state, issue_id) do
    %{state | claimed: MapSet.delete(state.claimed, issue_id)}
  end

  defp request_workspace_cleanup(state, issue_id, metadata, terminal_reason) do
    cleanup_entry = %{
      issue_id: issue_id,
      run_id: metadata[:previous_run_id] || metadata[:run_id],
      identifier: metadata[:identifier],
      attempt: metadata[:previous_attempt] || metadata[:attempt] || 0,
      worker_host: metadata[:worker_host],
      workspace_path: metadata[:workspace_path],
      workspace_root: metadata[:workspace_root],
      terminal_reason: terminal_reason,
      status: :request_pending
    }

    state = put_cleanup_pending(state, issue_id, cleanup_entry)

    case append_workspace_cleanup_request(state, cleanup_entry) do
      :ok ->
        state
        |> consume_pending_retry(issue_id)
        |> put_cleanup_pending(issue_id, %{cleanup_entry | status: :cleanup_pending})
        |> retry_pending_workspace_cleanups()

      {:error, reason} ->
        Logger.error("Failed to persist workspace cleanup request issue_id=#{issue_id}: #{inspect(reason)}")
        put_cleanup_pending(state, issue_id, Map.put(cleanup_entry, :persistence_error, reason))
    end
  end

  defp append_workspace_cleanup_request(state, entry) do
    append_run_event(state, %{
      transition: "workspace_cleanup_requested",
      stage: "cleanup",
      terminal_reason: entry.terminal_reason,
      run_id: entry.run_id,
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      attempt: entry.attempt,
      worker_host: entry.worker_host,
      workspace_path: entry.workspace_path,
      workspace_root: entry.workspace_root
    })
  end

  defp put_cleanup_pending(state, issue_id, entry) do
    %{
      state
      | cleanup_pending: Map.put(state.cleanup_pending, issue_id, entry),
        claimed: MapSet.put(state.claimed, issue_id)
    }
  end

  defp consume_pending_retry(state, issue_id) do
    %{state | retry_attempts: Map.delete(state.retry_attempts, issue_id)}
  end

  defp retry_pending_workspace_cleanups(%State{} = state) do
    Enum.reduce(state.cleanup_pending, state, &retry_pending_workspace_cleanup/2)
  end

  defp initialize_workspace_cleanup_recovery(state) do
    if orchestrator_server_process?(state) do
      send(self(), :start_pending_workspace_cleanups)
      state
    else
      retry_pending_workspace_cleanups_sync(state)
    end
  end

  defp retry_pending_workspace_cleanup({issue_id, %{status: :request_pending} = entry}, state) do
    case append_workspace_cleanup_request(state, entry) do
      :ok ->
        state
        |> put_cleanup_pending(issue_id, %{entry | status: :cleanup_pending})
        |> maybe_start_workspace_cleanup(issue_id)

      {:error, _reason} ->
        state
    end
  end

  defp retry_pending_workspace_cleanup({issue_id, %{status: :cleanup_pending}}, state) do
    maybe_start_workspace_cleanup(state, issue_id)
  end

  defp retry_pending_workspace_cleanup(
         {issue_id, %{status: :io_completion_pending} = entry},
         state
       ) do
    case append_workspace_cleanup_io_completed(state, entry) do
      :ok ->
        state
        |> put_cleanup_pending(issue_id, cleanup_completion_pending_entry(entry))
        |> retry_pending_workspace_cleanup_completion(issue_id)

      {:error, _reason} ->
        state
    end
  end

  defp retry_pending_workspace_cleanup({issue_id, %{status: :completion_pending} = entry}, state) do
    retry_pending_workspace_cleanup_completion(state, issue_id, entry)
  end

  defp retry_pending_workspace_cleanup({_issue_id, %{status: :operator_required}}, state),
    do: state

  defp retry_pending_workspace_cleanup({_issue_id, %{status: :cleanup_running}}, state),
    do: state

  defp maybe_start_workspace_cleanup(state, issue_id) do
    cond do
      not orchestrator_server_process?(state) ->
        state

      active_workspace_cleanup_count(state.cleanup_pending) >= @max_workspace_cleanup_tasks ->
        state

      get_in(state.cleanup_pending, [issue_id, :cleanup_task]) != nil ->
        state

      true ->
        entry = Map.fetch!(state.cleanup_pending, issue_id)

        case append_workspace_cleanup_io_started(state, entry) do
          :ok ->
            state
            |> put_cleanup_pending(issue_id, cleanup_running_entry(entry))
            |> start_workspace_cleanup_task(issue_id)

          {:error, reason} ->
            Logger.error("Failed to persist workspace cleanup I/O start issue_id=#{issue_id}: #{inspect(reason)}")
            put_cleanup_pending(state, issue_id, Map.put(entry, :persistence_error, reason))
        end
    end
  end

  defp start_workspace_cleanup_task(state, issue_id) do
    entry = Map.fetch!(state.cleanup_pending, issue_id)
    do_start_workspace_cleanup_task(state, issue_id, entry)
  end

  defp do_start_workspace_cleanup_task(state, issue_id, entry) do
    timeout_ms = Config.settings!().hooks.timeout_ms

    task =
      Task.Supervisor.async_nolink(SymphonyElixir.TaskSupervisor, fn ->
        perform_workspace_cleanup_io(entry)
      end)

    timeout_token = make_ref()

    timeout_ref =
      Process.send_after(
        self(),
        {:workspace_cleanup_timeout, issue_id, timeout_token},
        timeout_ms
      )

    task_entry = %{
      pid: task.pid,
      ref: task.ref,
      timeout_ref: timeout_ref,
      timeout_token: timeout_token
    }

    update_in(state.cleanup_pending[issue_id], &Map.put(&1, :cleanup_task, task_entry))
  catch
    :exit, reason ->
      Logger.warning("Unable to start workspace cleanup task issue_id=#{issue_id} reason=#{inspect(reason)}")
      require_workspace_cleanup_operator(state, issue_id, entry, :workspace_preservation_required)
  end

  defp perform_workspace_cleanup_io(entry) do
    if valid_expected_workspace_path?(entry.workspace_path) and
         valid_expected_workspace_path?(entry.workspace_root) do
      Workspace.remove_exact_if_durable(
        entry.workspace_path,
        entry.workspace_root,
        entry.worker_host
      )
    else
      {:error, :workspace_affinity_missing, ""}
    end
  end

  defp apply_workspace_cleanup_result(state, issue_id, {:ok, _removed}) do
    entry = Map.fetch!(state.cleanup_pending, issue_id)

    case append_workspace_cleanup_io_completed(state, entry) do
      :ok ->
        put_cleanup_pending(state, issue_id, cleanup_completion_pending_entry(entry))

      {:error, reason} ->
        Logger.error("Workspace cleanup I/O completion remains pending issue_id=#{issue_id}: #{inspect(reason)}")

        put_cleanup_pending(
          state,
          issue_id,
          entry
          |> Map.put(:status, :io_completion_pending)
          |> Map.put(:persistence_error, reason)
        )
    end
  end

  defp apply_workspace_cleanup_result(state, issue_id, {:error, reason, _output}) do
    Logger.error("Workspace cleanup requires operator recovery issue_id=#{issue_id}: #{inspect(reason)}")
    entry = Map.fetch!(state.cleanup_pending, issue_id)
    require_workspace_cleanup_operator(state, issue_id, entry, reason)
  end

  defp apply_workspace_cleanup_result(state, issue_id, {:error, reason}) do
    apply_workspace_cleanup_result(state, issue_id, {:error, reason, ""})
  end

  defp apply_workspace_cleanup_result(state, issue_id, _invalid_result) do
    apply_workspace_cleanup_result(
      state,
      issue_id,
      {:error, :workspace_preservation_required, ""}
    )
  end

  defp complete_workspace_cleanup(state, issue_id) do
    %{
      state
      | cleanup_pending: Map.delete(state.cleanup_pending, issue_id),
        claimed: MapSet.delete(state.claimed, issue_id)
    }
  end

  defp retry_pending_workspace_cleanup_completion(state, issue_id) do
    entry = Map.fetch!(state.cleanup_pending, issue_id)
    retry_pending_workspace_cleanup_completion(state, issue_id, entry)
  end

  defp retry_pending_workspace_cleanup_completion(state, issue_id, entry) do
    case append_workspace_cleanup_completed(state, entry) do
      :ok -> complete_workspace_cleanup(state, issue_id)
      {:error, _reason} -> state
    end
  end

  defp cleanup_running_entry(entry) do
    entry
    |> Map.put(:status, :cleanup_running)
    |> Map.delete(:cleanup_error)
    |> Map.delete(:persistence_error)
  end

  defp cleanup_completion_pending_entry(entry) do
    entry
    |> Map.put(:status, :completion_pending)
    |> Map.delete(:cleanup_task)
    |> Map.delete(:cleanup_error)
    |> Map.delete(:persistence_error)
  end

  defp clear_workspace_cleanup_task(state, issue_id, task_entry) do
    if is_reference(task_entry.timeout_ref), do: Process.cancel_timer(task_entry.timeout_ref)

    update_in(state.cleanup_pending[issue_id], fn
      nil -> nil
      entry -> Map.delete(entry, :cleanup_task)
    end)
  end

  defp active_workspace_cleanup_count(cleanup_pending) do
    Enum.count(cleanup_pending, fn {_issue_id, entry} -> Map.has_key?(entry, :cleanup_task) end)
  end

  defp cleanup_task_for_ref(cleanup_pending, ref) do
    Enum.find_value(cleanup_pending, fn {issue_id, entry} ->
      case Map.get(entry, :cleanup_task) do
        %{ref: ^ref} = task_entry -> {issue_id, task_entry}
        _other -> nil
      end
    end)
  end

  defp handle_workspace_cleanup_down(ref, reason, state) do
    case cleanup_task_for_ref(state.cleanup_pending, ref) do
      {issue_id, task_entry} ->
        Logger.warning("Workspace cleanup task exited issue_id=#{issue_id} reason=#{inspect(reason)}")

        state =
          state
          |> clear_workspace_cleanup_task(issue_id, task_entry)
          |> apply_workspace_cleanup_result(
            issue_id,
            {:error, :workspace_preservation_required, ""}
          )
          |> retry_pending_workspace_cleanups()

        notify_dashboard()
        {:noreply, state}

      nil ->
        {:noreply, state}
    end
  end

  defp orchestrator_server_process?(state) do
    case GenServer.whereis(state.poll_owner_key || __MODULE__) do
      pid when pid == self() -> true
      _other -> false
    end
  catch
    :exit, _reason -> false
  end

  defp retry_pending_workspace_cleanups_sync(%State{} = state) do
    Enum.reduce(state.cleanup_pending, state, fn
      {issue_id, %{status: :request_pending} = entry}, state_acc ->
        case append_workspace_cleanup_request(state_acc, entry) do
          :ok ->
            state_acc
            |> put_cleanup_pending(issue_id, %{entry | status: :cleanup_pending})
            |> start_workspace_cleanup_sync(issue_id)

          {:error, _reason} ->
            state_acc
        end

      {issue_id, %{status: :cleanup_pending}}, state_acc ->
        start_workspace_cleanup_sync(state_acc, issue_id)

      {issue_id, %{status: :io_completion_pending} = entry}, state_acc ->
        case append_workspace_cleanup_io_completed(state_acc, entry) do
          :ok ->
            state_acc
            |> put_cleanup_pending(issue_id, cleanup_completion_pending_entry(entry))
            |> retry_pending_workspace_cleanup_completion(issue_id)

          {:error, _reason} ->
            state_acc
        end

      {issue_id, %{status: :completion_pending} = entry}, state_acc ->
        case append_workspace_cleanup_completed(state_acc, entry) do
          :ok -> complete_workspace_cleanup(state_acc, issue_id)
          {:error, _reason} -> state_acc
        end

      {_issue_id, _entry}, state_acc ->
        state_acc
    end)
  end

  defp start_workspace_cleanup_sync(state, issue_id) do
    entry = Map.fetch!(state.cleanup_pending, issue_id)

    case append_workspace_cleanup_io_started(state, entry) do
      :ok ->
        state
        |> put_cleanup_pending(issue_id, cleanup_running_entry(entry))
        |> perform_workspace_cleanup_sync(issue_id)

      {:error, reason} ->
        put_cleanup_pending(state, issue_id, Map.put(entry, :persistence_error, reason))
    end
  end

  defp perform_workspace_cleanup_sync(state, issue_id) do
    entry = Map.fetch!(state.cleanup_pending, issue_id)

    with true <- valid_expected_workspace_path?(entry.workspace_path),
         true <- valid_expected_workspace_path?(entry.workspace_root),
         {:ok, _removed} <-
           Workspace.remove_exact_if_durable(
             entry.workspace_path,
             entry.workspace_root,
             entry.worker_host
           ) do
      persist_synchronous_workspace_cleanup_completion(state, issue_id, entry)
    else
      false ->
        Logger.error("Workspace cleanup pending because exact affinity is missing issue_id=#{issue_id}")
        require_workspace_cleanup_operator(state, issue_id, entry, :workspace_affinity_missing)

      {:error, reason, _output} ->
        Logger.error("Workspace cleanup remains pending issue_id=#{issue_id}: #{inspect(reason)}")
        require_workspace_cleanup_operator(state, issue_id, entry, reason)
    end
  end

  defp persist_synchronous_workspace_cleanup_completion(state, issue_id, entry) do
    case append_workspace_cleanup_io_completed(state, entry) do
      :ok ->
        state
        |> put_cleanup_pending(issue_id, cleanup_completion_pending_entry(entry))
        |> retry_pending_workspace_cleanup_completion(issue_id)

      {:error, reason} ->
        put_cleanup_pending(
          state,
          issue_id,
          entry
          |> Map.put(:status, :io_completion_pending)
          |> Map.put(:persistence_error, reason)
        )
    end
  end

  defp append_workspace_cleanup_io_started(state, entry) do
    append_workspace_cleanup_lifecycle_event(state, entry, "workspace_cleanup_io_started")
  end

  defp append_workspace_cleanup_io_completed(state, entry) do
    append_workspace_cleanup_lifecycle_event(state, entry, "workspace_cleanup_io_completed")
  end

  defp append_workspace_cleanup_operator_required(state, entry, cleanup_error) do
    state
    |> append_workspace_cleanup_lifecycle_event(
      entry,
      "workspace_cleanup_operator_required",
      %{cleanup_error: cleanup_error}
    )
  end

  defp append_workspace_cleanup_retry_requested(state, entry) do
    append_workspace_cleanup_lifecycle_event(state, entry, "workspace_cleanup_retry_requested")
  end

  defp append_workspace_cleanup_lifecycle_event(state, entry, transition, extra \\ %{}) do
    append_run_event(
      state,
      Map.merge(
        %{
          transition: transition,
          stage: "cleanup",
          run_id: entry.run_id,
          issue_id: entry.issue_id,
          issue_identifier: entry.identifier,
          attempt: entry.attempt,
          worker_host: entry.worker_host,
          workspace_path: entry.workspace_path,
          workspace_root: entry.workspace_root
        },
        extra
      )
    )
  end

  defp require_workspace_cleanup_operator(state, issue_id, entry, reason) do
    cleanup_error = persisted_cleanup_error(reason)

    entry =
      entry
      |> Map.put(:status, :operator_required)
      |> Map.put(:cleanup_error, restored_cleanup_error(cleanup_error))
      |> Map.delete(:cleanup_task)

    case append_workspace_cleanup_operator_required(state, entry, cleanup_error) do
      :ok ->
        put_cleanup_pending(state, issue_id, Map.delete(entry, :persistence_error))

      {:error, persistence_error} ->
        put_cleanup_pending(state, issue_id, Map.put(entry, :persistence_error, persistence_error))
    end
  end

  defp persisted_cleanup_error(:workspace_affinity_missing), do: "workspace_affinity_missing"

  defp persisted_cleanup_error(:workspace_preservation_required),
    do: "workspace_preservation_required"

  defp persisted_cleanup_error(_reason), do: "workspace_cleanup_failed"

  defp restored_cleanup_error("workspace_affinity_missing"), do: :workspace_affinity_missing

  defp restored_cleanup_error("workspace_preservation_required"),
    do: :workspace_preservation_required

  defp restored_cleanup_error(nil), do: :workspace_preservation_required
  defp restored_cleanup_error(_reason), do: :workspace_cleanup_failed

  defp append_workspace_cleanup_completed(state, entry) do
    append_run_event(state, %{
      transition: "workspace_cleanup_completed",
      stage: "cleanup",
      run_id: entry.run_id,
      issue_id: entry.issue_id,
      issue_identifier: entry.identifier,
      attempt: entry.attempt,
      worker_host: entry.worker_host,
      workspace_path: entry.workspace_path,
      workspace_root: entry.workspace_root
    })
  end

  defp cleanup_entry_from_running(issue_id, running_entry) do
    %{
      issue_id: issue_id,
      run_id: running_entry.run_id,
      identifier: running_entry.identifier,
      attempt: Map.get(running_entry, :retry_attempt, 0),
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path),
      workspace_root: Map.get(running_entry, :workspace_root),
      terminal_reason: "tracker_terminal",
      status: :request_pending
    }
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
        workspace_root: Map.get(dispatch, :workspace_root),
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
      attempt when is_integer(attempt) and attempt >= 0 -> attempt + 1
      _ -> 1
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

  defp pick_retry_workspace_root(previous_retry, metadata) do
    metadata[:workspace_root] || Map.get(previous_retry, :workspace_root)
  end

  defp maybe_put_runtime_value(running_entry, _key, nil), do: running_entry

  defp maybe_put_runtime_value(running_entry, key, value) when is_map(running_entry) do
    Map.put(running_entry, key, value)
  end

  defp accept_worker_runtime_info(%{running: running} = state, issue_id, runtime_info)
       when is_binary(issue_id) and is_map(runtime_info) do
    case Map.get(running, issue_id) do
      nil ->
        {state, {:error, :run_not_active}}

      running_entry ->
        accept_worker_runtime_info_for_run(state, issue_id, running_entry, runtime_info)
    end
  end

  defp accept_worker_runtime_info(state, _issue_id, _runtime_info),
    do: {state, {:error, :invalid_runtime_info}}

  defp accept_worker_runtime_info_for_run(state, issue_id, running_entry, runtime_info) do
    if runtime_affinity_matches_run?(runtime_info, running_entry) do
      persist_worker_runtime_info(state, issue_id, running_entry, runtime_info)
    else
      Logger.warning("Rejecting mismatched worker runtime info issue_id=#{issue_id} run_id=#{inspect(runtime_info[:run_id])}")
      {state, {:error, :workspace_affinity_mismatch}}
    end
  end

  defp persist_worker_runtime_info(state, issue_id, running_entry, runtime_info) do
    updated_running_entry =
      running_entry
      |> maybe_put_runtime_value(:worker_host, runtime_info[:worker_host])
      |> maybe_put_runtime_value(:workspace_path, runtime_info[:workspace_path])
      |> maybe_put_runtime_value(:workspace_root, runtime_info[:workspace_root])
      |> maybe_put_runtime_value(:session_title, runtime_info[:session_title])

    event = run_event(updated_running_entry, "run_runtime_ready", "running")

    case append_run_event(state, event) do
      :ok ->
        updated_running = Map.put(state.running, issue_id, updated_running_entry)
        {%{state | running: updated_running}, :ok}

      {:error, reason} ->
        Logger.error("Failed to durably acknowledge worker runtime issue_id=#{issue_id}: #{inspect(reason)}")
        {state, {:error, {:ledger_write_failed, reason}}}
    end
  end

  defp runtime_affinity_matches_run?(runtime_info, running_entry) do
    runtime_info[:run_id] == running_entry[:run_id] and
      runtime_info[:worker_host] == running_entry[:worker_host] and
      runtime_info[:workspace_path] == running_entry[:workspace_path] and
      runtime_info[:workspace_root] == running_entry[:workspace_root]
  end

  defp runtime_info_matches_run?(runtime_info, running_entry) do
    incoming_run_id = Map.get(runtime_info, :run_id)
    active_run_id = Map.get(running_entry, :run_id)
    is_nil(incoming_run_id) or is_nil(active_run_id) or incoming_run_id == active_run_id
  end

  defp accept_worker_model_resolution(
         %{running: running} = state,
         issue_id,
         resolution_info
       )
       when is_binary(issue_id) and is_map(resolution_info) do
    case Map.get(running, issue_id) do
      nil ->
        {state, {:error, :run_not_active}}

      running_entry ->
        persist_worker_model_resolution(state, issue_id, running_entry, resolution_info)
    end
  end

  defp accept_worker_model_resolution(state, _issue_id, _resolution_info),
    do: {state, {:error, :invalid_model_resolution}}

  defp persist_worker_model_resolution(state, issue_id, running_entry, resolution_info) do
    cond do
      not model_resolution_matches_run?(state, running_entry, resolution_info) ->
        Logger.warning("Rejecting mismatched worker model resolution issue_id=#{issue_id} run_id=#{inspect(resolution_info[:run_id])}")
        {state, {:error, :run_identity_mismatch}}

      is_nil(resolution_info[:resolved_model]) ->
        {state, :ok}

      not is_binary(resolution_info[:resolved_model]) ->
        {state, {:error, :invalid_model_resolution}}

      is_binary(Map.get(running_entry, :resolved_model)) ->
        accept_existing_model_resolution(state, running_entry, resolution_info)

      true ->
        append_worker_model_resolution(state, issue_id, running_entry, resolution_info)
    end
  end

  defp accept_existing_model_resolution(state, running_entry, resolution_info) do
    if running_entry.resolved_model == resolution_info.resolved_model do
      {state, :ok}
    else
      {state, {:error, :model_resolution_mismatch}}
    end
  end

  defp append_worker_model_resolution(state, issue_id, running_entry, resolution_info) do
    updated_running_entry =
      running_entry
      |> maybe_put_runtime_value(:resolved_model, resolution_info[:resolved_model])
      |> maybe_put_runtime_value(:reasoning_effort, resolution_info[:reasoning_effort])
      |> maybe_put_runtime_value(:model_catalog_source, resolution_info[:model_catalog_source])
      |> maybe_put_runtime_value(:model_catalog, resolution_info[:model_catalog])

    event = run_event(updated_running_entry, "model_resolved", "running")

    case append_run_event(state, event) do
      :ok ->
        updated_running = Map.put(state.running, issue_id, updated_running_entry)
        {%{state | running: updated_running}, :ok}

      {:error, reason} ->
        Logger.error("Failed to durably acknowledge worker model resolution issue_id=#{issue_id}: #{inspect(reason)}")
        {state, {:error, {:ledger_write_failed, reason}}}
    end
  end

  defp model_resolution_matches_run?(state, running_entry, resolution_info) do
    resolution_info[:run_id] == running_entry[:run_id] and
      resolution_info[:runner_generation] == state.runner_generation
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
    if GenServer.whereis(server) do
      try do
        GenServer.call(server, :request_refresh, @refresh_call_timeout_ms)
      catch
        :exit, _reason -> :unavailable
      end
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

  @spec retry_workspace_cleanup(String.t()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def retry_workspace_cleanup(issue_id),
    do: retry_workspace_cleanup(__MODULE__, issue_id)

  @spec retry_workspace_cleanup(GenServer.server(), String.t()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def retry_workspace_cleanup(server, issue_id) when is_binary(issue_id) do
    if GenServer.whereis(server) do
      GenServer.call(server, {:retry_workspace_cleanup, issue_id})
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

  def handle_call({:retry_workspace_cleanup, issue_id}, _from, state) do
    case request_workspace_cleanup_retry(state, issue_id) do
      {:ok, payload, updated_state} -> {:reply, {:ok, payload}, updated_state}
      {:error, reason, unchanged_state} -> {:reply, {:error, reason}, unchanged_state}
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
          codex_cached_input_tokens: Map.get(metadata, :codex_cached_input_tokens, 0),
          codex_uncached_input_tokens: Map.get(metadata, :codex_uncached_input_tokens, 0),
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

    admitting =
      Enum.map(tracker_admissions(state), fn {issue_id, admission} ->
        %{
          issue_id: issue_id,
          run_id: admission.run_id,
          attempt: admission.attempt,
          stage: "tracker_admission",
          status: admission.status,
          identifier: admission.identifier,
          admission_id: admission.admission_id,
          source_state: admission.source_state,
          target_state: admission.target_state,
          issue_snapshot_schema: admission.issue_snapshot_schema,
          issue_snapshot_bytes: admission.issue_snapshot_bytes,
          issue_snapshot_sha256: admission.issue_snapshot_sha256,
          tracker_authority_digest: admission.tracker_authority_digest,
          worker_host: admission.worker_host,
          workspace_path: admission.workspace_path
        }
      end)

    retrying =
      state.retry_attempts
      |> Enum.map(fn {issue_id, %{attempt: attempt} = retry} ->
        pending? = Map.get(retry, :status) == :durability_pending
        due_at_ms = Map.get(retry, :due_at_ms)

        %{
          issue_id: issue_id,
          run_id: Map.get(retry, :previous_run_id),
          attempt: attempt,
          stage: if(pending?, do: "retry_persistence_pending", else: "retry_queued"),
          due_in_ms: if(is_integer(due_at_ms), do: max(0, due_at_ms - now_ms), else: nil),
          identifier: Map.get(retry, :identifier),
          error: if(pending?, do: "retry_ledger_write_failed", else: Map.get(retry, :error)),
          worker_host: Map.get(retry, :worker_host),
          workspace_path: Map.get(retry, :workspace_path),
          workspace_root: Map.get(retry, :workspace_root)
        }
      end)
      |> Enum.concat(recovered_dispatch_snapshot_rows(state.recovered_dispatches))
      |> Enum.concat(queued_resume_snapshot_rows(state.queued_resumes))
      |> Enum.concat(cleanup_pending_snapshot_rows(state.cleanup_pending))

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
       admitting: admitting,
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
    in_flight? = is_map(state.poll_task)
    coalesced = state.poll_check_in_progress == true or already_due?

    state =
      cond do
        in_flight? -> %{state | poll_dirty: true}
        coalesced -> state
        true -> schedule_tick(state, 0)
      end

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
    accounting = token_delta.accounting
    codex_app_server_pid = Map.get(running_entry, :codex_app_server_pid)
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
        codex_input_tokens: accounting.input.lifetime,
        codex_cached_input_tokens: accounting.cached_input.lifetime,
        codex_uncached_input_tokens: accounting.uncached_input.lifetime,
        codex_output_tokens: accounting.output.lifetime,
        codex_total_tokens: accounting.total.lifetime,
        codex_last_reported_input_tokens: accounting.input.last_raw || 0,
        codex_last_reported_cached_input_tokens: accounting.cached_input.last_raw || 0,
        codex_last_reported_uncached_input_tokens: accounting.uncached_input.last_raw || 0,
        codex_last_reported_output_tokens: accounting.output.last_raw || 0,
        codex_last_reported_total_tokens: accounting.total.last_raw || 0,
        codex_token_accounting: accounting,
        codex_token_telemetry_observed: accounting.integrity == :valid,
        codex_token_telemetry_integrity: accounting.integrity,
        codex_token_telemetry_failure: accounting.failure,
        codex_token_telemetry_epoch: accounting.total.epoch,
        codex_uncached_input_telemetry_observed: accounting.uncached_integrity == :valid,
        codex_uncached_input_telemetry_integrity: accounting.uncached_integrity,
        codex_uncached_input_telemetry_failure: accounting.uncached_failure,
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
       when event in [
              :app_server_error,
              :terminal_protocol_error,
              :turn_failed,
              :turn_ended_with_error,
              :startup_failed
            ] do
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

  defp clear_poll_task(%State{poll_task: %{timeout_ref: timeout_ref, guard_ref: guard_ref}} = state) do
    if is_reference(timeout_ref), do: Process.cancel_timer(timeout_ref)
    if is_reference(guard_ref), do: Process.demonitor(guard_ref, [:flush])
    %{state | poll_task: nil}
  end

  defp clear_poll_task(%State{} = state), do: %{state | poll_task: nil}

  defp finish_poll_cycle(%State{} = state, :ok) do
    delay_ms = if state.poll_dirty, do: 0, else: max(state.poll_interval_ms || 1_000, 1)

    state
    |> Map.put(:poll_check_in_progress, false)
    |> Map.put(:poll_dirty, false)
    |> Map.put(:poll_failure_count, 0)
    |> schedule_tick(delay_ms)
  end

  defp finish_poll_cycle(%State{} = state, {:error, _reason}) do
    failure_count = state.poll_failure_count + 1

    state
    |> Map.put(:poll_check_in_progress, false)
    |> Map.put(:poll_dirty, false)
    |> Map.put(:poll_failure_count, failure_count)
    |> schedule_tick(poll_failure_backoff_ms(failure_count))
  end

  defp poll_failure_backoff_ms(failure_count) when is_integer(failure_count) and failure_count > 0 do
    exponent = min(failure_count - 1, 4)
    min(@poll_failure_backoff_base_ms * (1 <<< exponent), @poll_failure_backoff_max_ms)
  end

  defp poll_task_timeout_ms(%State{poll_task_timeout_ms: timeout_ms})
       when is_integer(timeout_ms) and timeout_ms > 0,
       do: timeout_ms

  defp poll_task_timeout_ms(_state), do: @poll_task_timeout_ms

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
           workspace_path: Map.get(running_entry, :workspace_path),
           workspace_root: Map.get(running_entry, :workspace_root)
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
           workspace_path: Map.get(running_entry, :workspace_path),
           workspace_root: Map.get(running_entry, :workspace_root)
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
        [terminal_reason: pending.terminal_reason] ++ terminal_intent(pending.action)
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
        |> schedule_issue_retry(issue_id, attempt, Map.put(metadata, :next_action, "continuation"))
        |> retry_schedule_state()

      {:retry, attempt, metadata} ->
        state
        |> schedule_issue_retry(issue_id, attempt, Map.put(metadata, :next_action, "retry"))
        |> retry_schedule_state()

      {:stop, cleanup_workspace, retry} ->
        apply_terminal_stop(state, issue_id, running_entry, cleanup_workspace, retry)
    end
  end

  defp apply_terminal_stop(state, issue_id, _running_entry, _cleanup_workspace, %{
         attempt: attempt,
         metadata: metadata
       }) do
    state
    |> schedule_issue_retry(issue_id, attempt, Map.put(metadata, :next_action, "retry"))
    |> retry_schedule_state()
  end

  defp apply_terminal_stop(state, issue_id, running_entry, cleanup_workspace, _retry) do
    state = %{state | retry_attempts: Map.delete(state.retry_attempts, issue_id)}
    finish_terminal_stop(state, issue_id, running_entry, cleanup_workspace)
  end

  defp finish_terminal_stop(state, issue_id, running_entry, true) do
    cleanup_entry = cleanup_entry_from_running(issue_id, running_entry)

    state
    |> put_cleanup_pending(issue_id, cleanup_entry)
    |> retry_pending_workspace_cleanups()
  end

  defp finish_terminal_stop(state, issue_id, _running_entry, false),
    do: release_issue_claim(state, issue_id)

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
      workspace_root: Map.get(running_entry, :workspace_root),
      resolved_model: Map.get(running_entry, :resolved_model),
      reasoning_effort: Map.get(running_entry, :reasoning_effort),
      model_catalog_source: Map.get(running_entry, :model_catalog_source),
      terminal_reason: Keyword.get(extra, :terminal_reason),
      next_action: Keyword.get(extra, :next_action),
      next_attempt: Keyword.get(extra, :next_attempt)
    }
  end

  defp terminal_intent({:continuation, attempt, _metadata}),
    do: [next_action: "continuation", next_attempt: attempt]

  defp terminal_intent({:retry, attempt, _metadata}),
    do: [next_action: "retry", next_attempt: attempt]

  defp terminal_intent({:stop, _cleanup_workspace, %{attempt: attempt}}),
    do: [next_action: "retry", next_attempt: attempt]

  defp terminal_intent({:stop, _cleanup_workspace, _retry}), do: [next_action: "none"]

  defp append_run_event(%State{run_ledger_path: nil}, _event), do: :ok

  defp append_run_event(%State{} = state, event) when is_map(event) do
    event = Map.put(event, :runner_generation, state.runner_generation)
    append_fn = state.run_ledger_append_fn || (&RunLedger.append/2)
    append_fn.(state.run_ledger_path, event)
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
      workspace_path: Map.get(wait, :workspace_path),
      workspace_root: Map.get(wait, :workspace_root)
    }
  end

  defp resolve_operator_wait(state, wait, action) do
    case apply_operator_wait_action(state, wait, action) do
      {:ok, payload, state} -> {:reply, {:ok, payload}, state}
      {:error, reason, state} -> {:reply, {:error, reason}, state}
    end
  end

  defp apply_operator_wait_action(state, wait, action, opts \\ []) do
    if OperatorWait.action_allowed?(wait, action) do
      apply_allowed_operator_wait_action(state, wait, action, opts)
    else
      {:error, :action_not_allowed, state}
    end
  end

  defp apply_allowed_operator_wait_action(state, wait, action, opts) do
    case guard_merge_approval(state, wait, action) do
      :ok -> persist_allowed_operator_wait_action(state, wait, action, opts)
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp persist_allowed_operator_wait_action(state, wait, action, opts) do
    transition = if action == "reject", do: "wait_rejected", else: "resume_queued"
    next_attempt = max(wait.attempt + 1, 1)

    event =
      wait
      |> operator_wait_event(transition)
      |> maybe_mark_resume_queued(action)
      |> maybe_put_resumed_attempt(action, next_attempt)
      |> maybe_put_operator_command_context(Keyword.get(opts, :operator_comment), action)

    persist_operator_wait_action(state, wait, action, next_attempt, event)
  end

  defp persist_operator_wait_action(state, wait, action, next_attempt, event) do
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
  end

  defp guard_merge_approval(
         %State{
           run_ledger_path: run_ledger_path,
           runner_generation: runner_generation,
           operator_commands: %OperatorCommandState{workflow_generation: workflow_generation}
         },
         %{reason: "waiting_owner", tracker_state: tracker_state} = wait,
         "approve"
       ) do
    if OperatorWait.human_review_state?(tracker_state) do
      if is_binary(run_ledger_path) and is_binary(runner_generation) and
           is_binary(workflow_generation) do
        run_ledger_path
        |> MergeLane.default_path()
        |> MergeLane.guard_runner_approval(
          wait.issue_id,
          wait.wait_id,
          runner_generation,
          workflow_generation
        )
      else
        {:error, :runner_merge_identity_unavailable}
      end
    else
      :ok
    end
  end

  defp guard_merge_approval(_state, _wait, _action), do: :ok

  defp maybe_put_resumed_attempt(event, "reject", _next_attempt), do: event
  defp maybe_put_resumed_attempt(event, _action, next_attempt), do: Map.put(event, :attempt, next_attempt)

  defp maybe_mark_resume_queued(event, "reject"), do: event
  defp maybe_mark_resume_queued(event, _action), do: Map.put(event, :stage, "resume_queued")

  defp maybe_put_operator_command_context(
         event,
         %{id: comment_id, created_at: %DateTime{} = created_at},
         action
       )
       when is_binary(comment_id) and is_binary(action) do
    Map.merge(event, %{
      comment_id: comment_id,
      comment_created_at: DateTime.to_iso8601(created_at),
      operator_command: action
    })
  end

  defp maybe_put_operator_command_context(event, _comment, _action), do: event

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
      workspace_root: Map.get(wait, :workspace_root),
      queued_at: DateTime.utc_now()
    }
  end

  defp operator_command_issue_ids(%State{} = state) do
    state.running
    |> Map.keys()
    |> Enum.concat(Map.keys(state.parked))
    |> Enum.concat(Map.keys(state.cleanup_pending))
    |> Enum.concat(Map.keys(state.retry_attempts))
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
      case maybe_apply_operator_comment(state, issue_id, comment, operator_user_ids) do
        {:ignored, updated_state} ->
          persist_operator_cursor(
            updated_state,
            "operator_cursor_advanced",
            issue_id,
            created_at,
            comment_id
          )

        {:processed, updated_state} ->
          advance_operator_cursor_in_memory(updated_state, issue_id, created_at, comment_id)

        {:pending, updated_state} ->
          updated_state

        {:command, updated_state} ->
          updated_state
      end
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
    cond do
      Map.has_key?(state.operator_commands.pending_outcomes, comment.id) ->
        log_pending_operator_comment_identity(state, issue_id, comment)
        {:pending, state}

      MapSet.member?(state.operator_commands.processed_comment_ids, comment.id) ->
        {:processed, state}

      true ->
        parse_and_apply_operator_comment(state, issue_id, comment, operator_user_ids)
    end
  end

  defp log_pending_operator_comment_identity(state, issue_id, comment) do
    pending = state.operator_commands.pending_outcomes[comment.id]

    unless pending_operator_comment_identity_matches?(pending, issue_id, comment.created_at) do
      Logger.warning("Suppressing refetched pending operator comment with mismatched identity issue_id=#{issue_id} comment_id=#{comment.id}")
    end
  end

  defp pending_operator_comment_identity_matches?(pending, issue_id, %DateTime{} = created_at)
       when is_map(pending) do
    pending.issue_id == issue_id and
      DateTime.compare(pending.comment_created_at, created_at) == :eq
  end

  defp pending_operator_comment_identity_matches?(_pending, _issue_id, _created_at), do: false

  defp parse_and_apply_operator_comment(state, issue_id, comment, operator_user_ids) do
    case OperatorCommand.parse_comment(comment, operator_user_ids) do
      {:ok, action} -> {:command, apply_operator_comment(state, issue_id, comment, action)}
      :ignore -> {:ignored, state}
    end
  end

  defp operator_user_ids(%State{
         operator_commands: %OperatorCommandState{operator_authority_invalidated: true}
       }),
       do: []

  defp operator_user_ids(%State{
         operator_commands: %OperatorCommandState{operator_user_ids_generation: nil}
       }) do
    Config.settings!().tracker.operator_user_ids || []
  end

  defp operator_user_ids(%State{
         operator_commands: %OperatorCommandState{operator_user_ids_generation: generation}
       }),
       do: generation

  defp apply_operator_comment(state, issue_id, comment, "stop") do
    case Map.get(state.running, issue_id) do
      %{issue: %Issue{} = issue} ->
        updated_state =
          park_running_issue(state, issue, "operator_stopped",
            terminal_reason: "operator_stop",
            operator_comment: comment,
            operator_action: "stop"
          )

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
        apply_operator_retry_stop(state, issue_id, comment)
    end
  end

  defp apply_operator_comment(state, issue_id, comment, "retry") do
    case Map.get(state.cleanup_pending, issue_id) do
      %{status: :operator_required} ->
        case request_workspace_cleanup_retry(state, issue_id) do
          {:ok, _payload, updated_state} ->
            record_operator_command_outcome(
              updated_state,
              issue_id,
              comment,
              "retry",
              "operator_command_applied"
            )

          {:error, _reason, unchanged_state} ->
            record_operator_command_outcome(
              unchanged_state,
              issue_id,
              comment,
              "retry",
              "operator_command_rejected"
            )
        end

      _cleanup ->
        apply_operator_wait_comment(state, issue_id, comment, "retry")
    end
  end

  defp apply_operator_comment(state, issue_id, comment, action) do
    apply_operator_wait_comment(state, issue_id, comment, action)
  end

  defp apply_operator_retry_stop(state, issue_id, comment) do
    updated_state = park_retry_attempt(state, issue_id, comment)

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
  end

  defp park_retry_attempt(%State{} = state, issue_id, comment) do
    case Map.get(state.retry_attempts, issue_id) do
      %{previous_run_id: run_id, identifier: identifier} = retry
      when is_binary(run_id) and is_binary(identifier) ->
        with {:ok, wait} <-
               OperatorWait.new("operator_stopped", %{
                 issue_id: issue_id,
                 identifier: identifier,
                 run_id: run_id,
                 attempt: retry.attempt,
                 terminal_reason: "operator_stop",
                 worker_host: Map.get(retry, :worker_host),
                 workspace_path: Map.get(retry, :workspace_path),
                 workspace_root: Map.get(retry, :workspace_root)
               }),
             park_event =
               wait
               |> operator_wait_event("retry_parked")
               |> maybe_put_operator_command_context(comment, "stop"),
             :ok <- append_run_event(state, park_event) do
          if is_reference(retry.timer_ref), do: Process.cancel_timer(retry.timer_ref)

          Logger.info("Queued retry parked for operator action: issue_id=#{issue_id} issue_identifier=#{identifier} reason=operator_stopped wait_id=#{wait.wait_id}")

          %{
            state
            | parked: Map.put(state.parked, issue_id, wait),
              claimed: MapSet.delete(state.claimed, issue_id),
              retry_attempts: Map.delete(state.retry_attempts, issue_id)
          }
        else
          {:error, error} ->
            Logger.error("Failed to durably park queued retry issue_id=#{issue_id}: #{inspect(error)}")
            state
        end

      _retry ->
        state
    end
  end

  defp apply_operator_wait_comment(state, issue_id, comment, action) do
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
        case apply_operator_wait_action(state, wait, action, operator_comment: comment) do
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

  defp request_workspace_cleanup_retry(state, issue_id) do
    case Map.get(state.cleanup_pending, issue_id) do
      %{status: :operator_required} = entry ->
        case append_workspace_cleanup_retry_requested(state, entry) do
          :ok ->
            updated_entry =
              entry
              |> Map.put(:status, :cleanup_pending)
              |> Map.delete(:cleanup_error)
              |> Map.delete(:persistence_error)

            updated_state =
              state
              |> put_cleanup_pending(issue_id, updated_entry)
              |> retry_pending_workspace_cleanups()

            {:ok,
             %{
               issue_id: issue_id,
               run_id: entry.run_id,
               retry_requested: true,
               requested_at: DateTime.utc_now()
             }, updated_state}

          {:error, reason} ->
            {:error, {:ledger_write_failed, reason}, state}
        end

      nil ->
        {:error, :cleanup_not_found, state}

      _entry ->
        {:error, :cleanup_retry_not_allowed, state}
    end
  end

  defp record_operator_command_outcome(
         %State{} = state,
         issue_id,
         comment,
         action,
         transition
       ) do
    outcome = operator_command_outcome(issue_id, comment, action, transition)

    state =
      if transition == "operator_command_applied" do
        put_pending_operator_outcome(state, outcome)
      else
        state
      end

    persist_operator_command_outcome(state, outcome)
  end

  defp operator_command_outcome(issue_id, comment, action, transition) do
    %{
      transition: transition,
      issue_id: issue_id,
      comment_id: comment.id,
      comment_created_at: comment.created_at,
      operator_command: action
    }
  end

  defp put_pending_operator_outcome(%State{} = state, outcome) do
    operator_commands = %{
      state.operator_commands
      | pending_outcomes: Map.put(state.operator_commands.pending_outcomes, outcome.comment_id, outcome)
    }

    %{
      state
      | operator_commands: operator_commands
    }
  end

  defp persist_operator_command_outcome(%State{} = state, outcome) do
    case validate_pending_operator_outcome(state, outcome) do
      :ok -> do_persist_operator_command_outcome(state, outcome)
      {:error, reason} -> reject_mismatched_operator_outcome(state, outcome, reason)
    end
  end

  defp do_persist_operator_command_outcome(%State{} = state, outcome) do
    event = operator_command_outcome_event(outcome)

    case append_run_event(state, event) do
      :ok ->
        Logger.info("Operator command #{outcome.operator_command} #{operator_outcome_label(outcome.transition)} issue_id=#{outcome.issue_id} comment_id=#{outcome.comment_id}")

        operator_commands = %{
          state.operator_commands
          | processed_comment_ids: MapSet.put(state.operator_commands.processed_comment_ids, outcome.comment_id),
            pending_outcomes: Map.delete(state.operator_commands.pending_outcomes, outcome.comment_id)
        }

        state = %{state | operator_commands: operator_commands}

        advance_operator_cursor_in_memory(
          state,
          outcome.issue_id,
          outcome.comment_created_at,
          outcome.comment_id
        )

      {:error, reason} ->
        Logger.error("Failed to record operator command issue_id=#{outcome.issue_id} comment_id=#{outcome.comment_id}: #{inspect(reason)}")

        state
    end
  end

  defp operator_command_outcome_event(outcome) do
    %{
      transition: outcome.transition,
      stage: "operator",
      issue_id: outcome.issue_id,
      comment_id: outcome.comment_id,
      comment_created_at: DateTime.to_iso8601(outcome.comment_created_at),
      operator_command: outcome.operator_command
    }
  end

  defp validate_pending_operator_outcome(state, outcome) do
    case Map.get(state.operator_commands.pending_outcomes, outcome.comment_id) do
      nil ->
        :ok

      pending ->
        if pending_operator_outcomes_match?(pending, outcome),
          do: :ok,
          else: {:error, :pending_operator_outcome_mismatch}
    end
  end

  defp pending_operator_outcomes_match?(pending, outcome) do
    pending.transition == outcome.transition and
      pending.issue_id == outcome.issue_id and
      pending.operator_command == outcome.operator_command and
      DateTime.compare(pending.comment_created_at, outcome.comment_created_at) == :eq
  end

  defp reject_mismatched_operator_outcome(state, outcome, reason) do
    Logger.error("Refusing mismatched operator outcome issue_id=#{outcome.issue_id} comment_id=#{outcome.comment_id}: #{inspect(reason)}")
    state
  end

  defp retry_pending_operator_outcomes(%State{} = state) do
    state.operator_commands.pending_outcomes
    |> Map.values()
    |> Enum.sort_by(fn outcome ->
      {DateTime.to_unix(outcome.comment_created_at, :microsecond), outcome.comment_id}
    end)
    |> Enum.reduce(state, fn outcome, state_acc ->
      persist_operator_command_outcome(state_acc, outcome)
    end)
  end

  defp operator_outcome_label("operator_command_applied"), do: "applied"
  defp operator_outcome_label(_transition), do: "rejected"

  defp advance_operator_cursor_in_memory(state, issue_id, created_at, comment_id) do
    cursor = advance_operator_cursor(state, issue_id, created_at, comment_id)

    %{
      state
      | operator_comment_cursors: Map.put(state.operator_comment_cursors, issue_id, cursor)
    }
  end

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

  defp restore_pending_operator_outcomes(events) when is_map(events) do
    Enum.reduce(events, %{}, fn {comment_id, event}, pending ->
      with true <- is_binary(comment_id),
           {:ok, created_at, _offset} <- DateTime.from_iso8601(event["comment_created_at"]),
           issue_id when is_binary(issue_id) <- event["issue_id"],
           operator_command when is_binary(operator_command) <- event["operator_command"] do
        Map.put(pending, comment_id, %{
          transition: "operator_command_applied",
          issue_id: issue_id,
          comment_id: comment_id,
          comment_created_at: created_at,
          operator_command: operator_command
        })
      else
        _invalid -> pending
      end
    end)
  end

  defp restore_pending_operator_outcomes(_events), do: %{}

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

  defp restore_cleanup_pending(events) when is_map(events) do
    Map.new(events, fn {issue_id, event} ->
      status = restored_cleanup_status(event["cleanup_status"])

      {issue_id,
       maybe_restore_cleanup_error(
         %{
           issue_id: issue_id,
           run_id: event["run_id"],
           identifier: event["issue_identifier"],
           attempt: event["attempt"],
           worker_host: event["worker_host"],
           workspace_path: event["workspace_path"],
           workspace_root: event["workspace_root"],
           terminal_reason: event["terminal_reason"] || "tracker_terminal",
           status: status
         },
         status,
         event["cleanup_error"]
       )}
    end)
  end

  defp restore_cleanup_pending(_events), do: %{}

  defp restored_cleanup_status("cleanup_pending"), do: :cleanup_pending
  defp restored_cleanup_status("operator_required"), do: :operator_required
  defp restored_cleanup_status("completion_pending"), do: :completion_pending
  defp restored_cleanup_status(_legacy), do: :request_pending

  defp maybe_restore_cleanup_error(entry, :operator_required, cleanup_error) do
    Map.put(entry, :cleanup_error, restored_cleanup_error(cleanup_error))
  end

  defp maybe_restore_cleanup_error(entry, _status, _cleanup_error), do: entry

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
           workspace_root: event["workspace_root"],
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
        workspace_root: Map.get(queued, :workspace_root),
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
        workspace_path: Map.get(dispatch, :workspace_path),
        workspace_root: Map.get(dispatch, :workspace_root)
      }
    end)
  end

  defp cleanup_pending_snapshot_rows(cleanup_pending) do
    Enum.map(cleanup_pending, fn {issue_id, cleanup} ->
      %{
        issue_id: issue_id,
        run_id: Map.get(cleanup, :run_id),
        attempt: Map.get(cleanup, :attempt),
        stage: "cleanup_pending",
        due_in_ms: nil,
        identifier: Map.get(cleanup, :identifier),
        error: cleanup_pending_error(cleanup),
        worker_host: Map.get(cleanup, :worker_host),
        workspace_path: Map.get(cleanup, :workspace_path),
        workspace_root: Map.get(cleanup, :workspace_root)
      }
    end)
  end

  defp cleanup_pending_error(%{cleanup_error: :workspace_affinity_missing}),
    do: "workspace_affinity_missing"

  defp cleanup_pending_error(%{cleanup_error: :workspace_preservation_required}),
    do: "workspace_preservation_required"

  defp cleanup_pending_error(%{cleanup_error: _reason}), do: "workspace_cleanup_failed"
  defp cleanup_pending_error(_cleanup), do: "workspace_cleanup_pending"

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

  defp record_session_completion_totals(state, running_entry) do
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

  defp refresh_runtime_config(%State{} = state) do
    {config, authority_generation, tracker_authority_generation} =
      Config.settings_with_authority!()

    state
    |> Map.put(:poll_interval_ms, config.polling.interval_ms)
    |> Map.put(:max_concurrent_agents, config.agent.max_concurrent_agents)
    |> refresh_tracker_behavior_context(config.tracker)
    |> pin_operator_authority_generation(
      config.tracker.operator_user_ids || [],
      authority_generation,
      tracker_authority_generation
    )
  end

  defp refresh_tracker_behavior_context(
         %State{
           operator_commands:
             %OperatorCommandState{
               tracker_context: %Tracker.PollContext{} = context
             } = operator_commands
         } = state,
         tracker
       ) do
    tracker_context = %{
      context
      | assignee: tracker.assignee,
        active_states: tracker.active_states,
        terminal_states: tracker.terminal_states
    }

    %{state | operator_commands: %{operator_commands | tracker_context: tracker_context}}
  end

  defp refresh_tracker_behavior_context(%State{} = state, _tracker), do: state

  defp retry_candidate_issue?(%Issue{} = issue, terminal_states) do
    retry_candidate_issue?(issue, active_state_set(), terminal_states)
  end

  defp retry_candidate_issue?(%Issue{} = issue, active_states, terminal_states) do
    candidate_issue?(issue, active_states, terminal_states) and
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
    accounting = token_accounting(running_entry)

    uncached_guard_enabled? =
      is_integer(get_in(running_entry, [:run_budget, :max_uncached_input_tokens]))

    case canonical_token_usage(extract_token_usage(update)) do
      {:ok, usage} ->
        token_delta_from_canonical_usage(accounting, usage, uncached_guard_enabled?)

      {:error, reason} ->
        failed_token_delta(accounting, reason)
    end
  end

  defp token_delta_from_canonical_usage(
         %{integrity: :failed} = accounting,
         _usage,
         _uncached_guard_enabled?
       ),
       do: empty_token_delta(accounting)

  defp token_delta_from_canonical_usage(accounting, usage, uncached_guard_enabled?) do
    with {:ok, input, input_delta} <-
           advance_token_component(accounting.input, get_token_usage(usage, :input)),
         {:ok, output, output_delta} <-
           advance_token_component(accounting.output, get_token_usage(usage, :output)),
         {:ok, total, total_delta} <-
           advance_token_component(accounting.total, get_token_usage(usage, :total)) do
      integrity =
        if accounting.integrity == :valid or is_integer(get_token_usage(usage, :total)),
          do: :valid,
          else: :unobserved

      {cached_input, uncached_input, uncached_integrity, uncached_failure} =
        advance_uncached_input_accounting(
          accounting,
          usage,
          uncached_guard_enabled?
        )

      updated_accounting = %{
        integrity: integrity,
        failure: nil,
        input: input,
        cached_input: cached_input,
        uncached_input: uncached_input,
        uncached_integrity: uncached_integrity,
        uncached_failure: uncached_failure,
        output: output,
        total: total
      }

      %{
        input_tokens: input_delta,
        cached_input_tokens: cached_input.lifetime,
        uncached_input_tokens: uncached_input.lifetime,
        output_tokens: output_delta,
        total_tokens: total_delta,
        accounting: updated_accounting
      }
    else
      {:error, reason} -> failed_token_delta(accounting, reason)
    end
  end

  defp empty_token_delta(accounting) do
    %{
      input_tokens: 0,
      cached_input_tokens: 0,
      uncached_input_tokens: 0,
      output_tokens: 0,
      total_tokens: 0,
      accounting: accounting
    }
  end

  defp failed_token_delta(%{integrity: :failed} = accounting, _reason),
    do: empty_token_delta(accounting)

  defp failed_token_delta(accounting, reason) do
    accounting = %{accounting | integrity: :failed, failure: reason}
    empty_token_delta(accounting)
  end

  defp canonical_token_usage(usage) when is_map(usage) do
    with {:ok, input} <- normalized_token_component(usage, token_fields(:input)),
         {:ok, output} <- normalized_token_component(usage, token_fields(:output)),
         {:ok, explicit_total} <- normalized_token_component(usage, token_fields(:total)),
         {:ok, derived_total} <- checked_token_sum(input, output) do
      {:ok,
       %{
         input: input,
         cached_input: optional_token_component(usage, token_fields(:cached_input)),
         output: output,
         total: canonical_total(explicit_total, derived_total)
       }}
    else
      :invalid -> {:error, :malformed_counter}
    end
  end

  defp canonical_token_usage(_usage), do: {:error, :malformed_counter}

  defp normalized_token_component(usage, fields) do
    values =
      fields
      |> Enum.filter(&Map.has_key?(usage, &1))
      |> Enum.map(&integer_like(Map.get(usage, &1)))

    cond do
      values == [] -> {:ok, nil}
      Enum.any?(values, &is_nil/1) -> :invalid
      true -> {:ok, Enum.max(values)}
    end
  end

  defp optional_token_component(usage, fields) do
    case normalized_token_component(usage, fields) do
      {:ok, value} -> value
      :invalid -> :invalid
    end
  end

  defp checked_token_sum(input, output) when is_integer(input) and is_integer(output) do
    if input <= @max_cumulative_token_count - output do
      {:ok, input + output}
    else
      :invalid
    end
  end

  defp checked_token_sum(_input, _output), do: {:ok, nil}

  defp canonical_total(nil, nil), do: nil
  defp canonical_total(total, nil), do: total
  defp canonical_total(nil, derived), do: derived
  defp canonical_total(total, derived), do: max(total, derived)

  defp advance_uncached_input_accounting(
         %{uncached_integrity: :failed} = accounting,
         _usage,
         _guard_enabled?
       ) do
    {
      accounting.cached_input,
      accounting.uncached_input,
      accounting.uncached_integrity,
      accounting.uncached_failure
    }
  end

  defp advance_uncached_input_accounting(accounting, usage, guard_enabled?) do
    input = get_token_usage(usage, :input)
    cached_input = get_token_usage(usage, :cached_input)

    token_usage_observed? =
      Enum.any?(
        [input, get_token_usage(usage, :output), get_token_usage(usage, :total)],
        &is_integer/1
      )

    case classify_uncached_input(
           input,
           cached_input,
           token_usage_observed?,
           guard_enabled?
         ) do
      {:counters, cached_raw, uncached_raw} ->
        advance_uncached_counters(accounting, cached_raw, uncached_raw)

      {:error, reason} ->
        failed_uncached_accounting(accounting, reason)

      :unavailable ->
        unchanged_uncached_accounting(accounting)
    end
  end

  defp classify_uncached_input(input, cached_input, _observed?, _guard_enabled?)
       when is_integer(input) and is_integer(cached_input) and cached_input <= input,
       do: {:counters, cached_input, input - cached_input}

  defp classify_uncached_input(_input, :invalid, _observed?, _guard_enabled?),
    do: {:error, :malformed_cached_input_counter}

  defp classify_uncached_input(input, cached_input, _observed?, _guard_enabled?)
       when is_integer(cached_input) and not is_integer(input),
       do: {:error, :malformed_cached_input_counter}

  defp classify_uncached_input(input, cached_input, _observed?, _guard_enabled?)
       when is_integer(input) and is_integer(cached_input) and cached_input > input,
       do: {:error, :cached_input_exceeds_input}

  defp classify_uncached_input(_input, _cached_input, true, true),
    do: {:error, :missing_cached_input_counter}

  defp classify_uncached_input(_input, _cached_input, _observed?, _guard_enabled?),
    do: :unavailable

  defp advance_uncached_counters(accounting, cached_raw, uncached_raw) do
    with {:ok, cached_component, _cached_delta} <-
           advance_token_component(accounting.cached_input, cached_raw),
         {:ok, uncached_component, _uncached_delta} <-
           advance_token_component(accounting.uncached_input, uncached_raw) do
      {cached_component, uncached_component, :valid, nil}
    else
      {:error, reason} -> failed_uncached_accounting(accounting, reason)
    end
  end

  defp unchanged_uncached_accounting(accounting) do
    {
      accounting.cached_input,
      accounting.uncached_input,
      accounting.uncached_integrity,
      accounting.uncached_failure
    }
  end

  defp failed_uncached_accounting(accounting, reason) do
    {accounting.cached_input, accounting.uncached_input, :failed, reason}
  end

  defp advance_token_component(component, nil), do: {:ok, component, 0}

  defp advance_token_component(%{last_raw: nil} = component, next_raw)
       when is_integer(next_raw) do
    with {:ok, lifetime} <- checked_token_add(component.lifetime, next_raw) do
      {:ok, %{component | last_raw: next_raw, lifetime: lifetime}, next_raw}
    end
  end

  defp advance_token_component(%{last_raw: previous_raw} = component, 0)
       when is_integer(previous_raw) and previous_raw > 0 do
    {:ok, %{component | last_raw: 0, epoch: component.epoch + 1}, 0}
  end

  defp advance_token_component(%{last_raw: previous_raw} = component, next_raw)
       when is_integer(previous_raw) and is_integer(next_raw) and next_raw >= previous_raw do
    delta = next_raw - previous_raw

    with {:ok, lifetime} <- checked_token_add(component.lifetime, delta) do
      {:ok, %{component | last_raw: next_raw, lifetime: lifetime}, delta}
    end
  end

  defp advance_token_component(%{last_raw: previous_raw}, next_raw)
       when is_integer(previous_raw) and is_integer(next_raw) and next_raw > 0 and
              next_raw < previous_raw,
       do: {:error, :ambiguous_counter_decrease}

  defp advance_token_component(_component, _next_raw),
    do: {:error, :malformed_counter}

  defp checked_token_add(left, right)
       when is_integer(left) and left >= 0 and is_integer(right) and right >= 0 do
    if left <= @max_cumulative_token_count - right do
      {:ok, left + right}
    else
      {:error, :counter_overflow}
    end
  end

  defp token_accounting(%{codex_token_accounting: accounting} = running_entry)
       when is_map(accounting),
       do: normalize_token_accounting(accounting, running_entry)

  defp token_accounting(running_entry) do
    observed? = Map.get(running_entry, :codex_token_telemetry_observed, false) == true

    %{
      integrity: if(observed?, do: :valid, else: :unobserved),
      failure: Map.get(running_entry, :codex_token_telemetry_failure),
      input: legacy_token_component(running_entry, :input),
      cached_input: legacy_token_component(running_entry, :cached_input),
      uncached_input: legacy_token_component(running_entry, :uncached_input),
      uncached_integrity: Map.get(running_entry, :codex_uncached_input_telemetry_integrity, :unobserved),
      uncached_failure: Map.get(running_entry, :codex_uncached_input_telemetry_failure),
      output: legacy_token_component(running_entry, :output),
      total: legacy_token_component(running_entry, :total)
    }
  end

  defp legacy_token_component(running_entry, token_key) do
    lifetime = Map.get(running_entry, token_lifetime_key(token_key), 0)
    last_raw = Map.get(running_entry, token_last_reported_key(token_key), 0)

    %{
      last_raw: if(last_raw == 0 and lifetime == 0, do: nil, else: last_raw),
      lifetime: lifetime,
      epoch: 0
    }
  end

  defp normalize_token_accounting(accounting, running_entry) do
    accounting
    |> Map.put_new(:cached_input, legacy_token_component(running_entry, :cached_input))
    |> Map.put_new(:uncached_input, legacy_token_component(running_entry, :uncached_input))
    |> Map.put_new(
      :uncached_integrity,
      Map.get(running_entry, :codex_uncached_input_telemetry_integrity, :unobserved)
    )
    |> Map.put_new(
      :uncached_failure,
      Map.get(running_entry, :codex_uncached_input_telemetry_failure)
    )
  end

  defp token_lifetime_key(:input), do: :codex_input_tokens
  defp token_lifetime_key(:cached_input), do: :codex_cached_input_tokens
  defp token_lifetime_key(:uncached_input), do: :codex_uncached_input_tokens
  defp token_lifetime_key(:output), do: :codex_output_tokens
  defp token_lifetime_key(:total), do: :codex_total_tokens

  defp token_last_reported_key(:input), do: :codex_last_reported_input_tokens
  defp token_last_reported_key(:cached_input), do: :codex_last_reported_cached_input_tokens
  defp token_last_reported_key(:uncached_input), do: :codex_last_reported_uncached_input_tokens
  defp token_last_reported_key(:output), do: :codex_last_reported_output_tokens
  defp token_last_reported_key(:total), do: :codex_last_reported_total_tokens

  defp new_token_accounting do
    %{
      integrity: :unobserved,
      failure: nil,
      input: new_token_component(),
      cached_input: new_token_component(),
      uncached_input: new_token_component(),
      uncached_integrity: :unobserved,
      uncached_failure: nil,
      output: new_token_component(),
      total: new_token_component()
    }
  end

  defp new_token_component, do: %{last_raw: nil, lifetime: 0, epoch: 0}

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

      if is_map(direct) and token_counter_map?(direct), do: direct
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

      if is_map(value) and token_counter_map?(value), do: value
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

  defp token_counter_map?(payload) do
    token_fields =
      token_fields(:input) ++
        token_fields(:cached_input) ++ token_fields(:output) ++ token_fields(:total)

    Enum.any?(token_fields, &Map.has_key?(payload, &1))
  end

  defp get_token_usage(usage, :input),
    do: payload_get(usage, token_fields(:input))

  defp get_token_usage(usage, :cached_input), do: Map.get(usage, :cached_input)

  defp get_token_usage(usage, :output),
    do: payload_get(usage, token_fields(:output))

  defp get_token_usage(usage, :total),
    do: payload_get(usage, token_fields(:total))

  defp token_fields(:input) do
    [
      "input_tokens",
      "prompt_tokens",
      :input_tokens,
      :prompt_tokens,
      :input,
      "promptTokens",
      :promptTokens,
      "inputTokens",
      :inputTokens
    ]
  end

  defp token_fields(:cached_input) do
    [
      "cached_input_tokens",
      :cached_input_tokens,
      "cachedInputTokens",
      :cachedInputTokens,
      "cached_prompt_tokens",
      :cached_prompt_tokens
    ]
  end

  defp token_fields(:output) do
    [
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
    ]
  end

  defp token_fields(:total) do
    [
      "total_tokens",
      "total",
      :total_tokens,
      :total,
      "totalTokens",
      :totalTokens
    ]
  end

  defp payload_get(payload, fields) when is_list(fields) do
    Enum.find_value(fields, fn field -> map_integer_value(payload, field) end)
  end

  defp map_integer_value(payload, field) do
    payload
    |> Map.get(field)
    |> integer_like()
  end

  defp run_budget_snapshot(running_entry, now) do
    RunBudget.snapshot(
      Map.get(running_entry, :run_budget, disabled_run_budget()),
      run_budget_metrics(running_entry, now)
    )
  end

  defp run_budget_metrics(running_entry, now) do
    telemetry_observed? = Map.get(running_entry, :codex_token_telemetry_observed, false)

    %{
      turns: Map.get(running_entry, :turn_count, 0),
      tokens: Map.get(running_entry, :codex_total_tokens, 0),
      token_telemetry_observed: telemetry_observed?,
      token_telemetry_integrity:
        Map.get(
          running_entry,
          :codex_token_telemetry_integrity,
          if(telemetry_observed?, do: :valid, else: :unobserved)
        ),
      token_telemetry_failure: Map.get(running_entry, :codex_token_telemetry_failure),
      uncached_input_tokens: Map.get(running_entry, :codex_uncached_input_tokens, 0),
      uncached_input_telemetry_observed: Map.get(running_entry, :codex_uncached_input_telemetry_observed, false),
      uncached_input_telemetry_integrity: Map.get(running_entry, :codex_uncached_input_telemetry_integrity, :unobserved),
      uncached_input_telemetry_failure: Map.get(running_entry, :codex_uncached_input_telemetry_failure),
      seconds: running_seconds(Map.get(running_entry, :started_at), now)
    }
  end

  defp disabled_run_budget do
    %{
      max_turns: Config.settings!().agent.max_turns,
      max_tokens: nil,
      max_uncached_input_tokens: nil,
      max_seconds: nil
    }
  end

  defp running_seconds(%DateTime{} = started_at, %DateTime{} = now) do
    max(0, DateTime.diff(now, started_at, :second))
  end

  defp running_seconds(_started_at, _now), do: 0

  defp integer_like(value)
       when is_integer(value) and value >= 0 and value <= @max_cumulative_token_count,
       do: value

  defp integer_like(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {num, ""} when num >= 0 and num <= @max_cumulative_token_count -> num
      _ -> nil
    end
  end

  defp integer_like(_value), do: nil
end
