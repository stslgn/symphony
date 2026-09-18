defmodule SymphonyElixir.ParkedRetryRegressionTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{Orchestrator, RunLedger, Tracker, Workflow}
  alias SymphonyElixir.Linear.Issue

  # No real client, application supervisor, worker, or managed ledger is started.
  defmodule FakeLinearClient do
    @spec fetch_candidate_issues(term()) :: {:ok, list()}
    def fetch_candidate_issues(context) do
      send(self(), :candidate_lookup)
      issues = Application.fetch_env!(:symphony_elixir, :parked_retry_fixture)
      {:ok, Application.get_env(:symphony_elixir, :parked_retry_candidates, Enum.filter(issues, &(&1.state in context.active_states)))}
    end

    @spec fetch_issue_states_by_ids(list(), term()) :: {:ok, list()}
    def fetch_issue_states_by_ids(ids, _context) do
      send(self(), {:exact_lookup, ids})
      issues = Application.fetch_env!(:symphony_elixir, :parked_retry_fixture)
      Application.get_env(:symphony_elixir, :parked_retry_lookup, {:ok, Enum.filter(issues, &(&1.id in ids))})
    end
  end

  @tag :tmp_dir
  setup %{tmp_dir: root} do
    workflow = Path.join(root, "WORKFLOW.md")
    workspace = Path.join(root, "saved-work")
    File.mkdir_p!(workspace)
    File.write!(Path.join(workspace, "preserved.txt"), "synthetic preserved work\n")

    File.write!(workflow, """
    ---
    tracker:
      kind: linear
      api_key: synthetic-fixture-only
      project_slug: synthetic-project
      active_states: [Agent Ready, Agent Running, Automated Review, Rework]
      terminal_states: [Done, Canceled]
    workspace:
      root: #{root}
    ---
    ## Symphony Runtime Prompt

    Synthetic fixture; no worker dispatch permitted.
    """)

    keys = [:workflow_file_path, :linear_client_module, :parked_retry_fixture, :parked_retry_candidates, :parked_retry_lookup]
    previous = Map.new(keys, &{&1, Application.fetch_env(:symphony_elixir, &1)})
    Workflow.set_workflow_file_path(workflow)
    Application.put_env(:symphony_elixir, :linear_client_module, FakeLinearClient)
    issue = %Issue{id: "fixture-177", identifier: "FIX-177", state: "Human Review"}
    Application.put_env(:symphony_elixir, :parked_retry_fixture, [issue])

    on_exit(fn ->
      Enum.each(previous, fn
        {key, {:ok, value}} -> Application.put_env(:symphony_elixir, key, value)
        {key, :error} -> Application.delete_env(:symphony_elixir, key)
      end)
    end)

    ledger = Path.join(root, "run-ledger.jsonl")

    base = %{
      run_id: "fixture-run",
      issue_id: issue.id,
      issue_identifier: issue.identifier,
      attempt: 0,
      workspace_path: workspace,
      workspace_root: root
    }

    for event <- [
          %{transition: "run_claimed", stage: "claimed"},
          %{transition: "run_started", stage: "running"},
          %{
            transition: "run_completed",
            stage: "released",
            terminal_reason: "worker_completed",
            next_action: "continuation",
            next_attempt: 1
          },
          %{
            transition: "retry_scheduled",
            stage: "retry_queued",
            next_action: "continuation",
            next_attempt: 1
          }
        ] do
      assert :ok = RunLedger.append(ledger, Map.merge(base, event))
    end

    context = Tracker.current_poll_context()

    retry = %{
      status: :dispatching,
      attempt: 1,
      identifier: issue.identifier,
      timer_ref: nil,
      retry_token: nil,
      due_at_ms: nil,
      previous_run_id: base.run_id,
      previous_attempt: 0,
      next_action: "continuation",
      worker_host: nil,
      workspace_path: workspace,
      workspace_root: root
    }

    state = %Orchestrator.State{
      max_concurrent_agents: 1,
      run_ledger_path: ledger,
      running: %{},
      retry_attempts: %{issue.id => retry},
      claimed: MapSet.new([issue.id]),
      task_start_fn: fn _ -> flunk("must not launch a worker") end,
      operator_commands: %Orchestrator.OperatorCommandState{
        tracker_context: context,
        operator_user_ids_generation: [],
        tracker_authority_generation: context.authority_generation
      }
    }

    request = %{
      running_ids: [],
      admission_ids: [],
      parked_ids: [],
      retry_issue_ids: [issue.id],
      retry_reconciliation: %{issue.id => Map.take(retry, [:previous_run_id, :previous_attempt, :attempt, :identifier, :worker_host, :workspace_path, :workspace_root])},
      comment_requests: [],
      operator_user_ids: [],
      dispatch_paused: false,
      tracker_authority_valid: true,
      tracker_context: context
    }

    {:ok,
     %{
       issue: issue,
       state: state,
       request: request,
       ledger: ledger,
       workspace: workspace,
       base: base
     }}
  end

  @tag :tmp_dir
  test "parked continuation is reconciled by exact ID and retired durably", fixture do
    result = Orchestrator.collect_tracker_poll_for_test(fixture.request)
    assert result.dispatch == {:ok, []}
    assert_received :candidate_lookup
    updated = Orchestrator.apply_poll_result_for_test(fixture.state, result)

    # Cancel only the synthetic timer in this test VM, including on RED.
    case updated.retry_attempts[fixture.issue.id] do
      %{timer_ref: ref} when is_reference(ref) -> Process.cancel_timer(ref)
      _ -> :ok
    end

    assert updated.running == %{}
    assert updated.cleanup_pending == %{}

    assert File.read!(Path.join(fixture.workspace, "preserved.txt")) ==
             "synthetic preserved work\n"

    assert_received {:exact_lookup, ["fixture-177"]}
    refute Map.has_key?(updated.retry_attempts, fixture.issue.id)
    refute MapSet.member?(updated.claimed, fixture.issue.id)
    assert {:ok, recovery} = RunLedger.reconcile_startup(fixture.ledger, "fixture-restart")
    refute Map.has_key?(recovery.recovered_dispatches, fixture.issue.id)
    assert recovery.cleanup_pending == %{}
  end

  @tag :tmp_dir
  test "baseline continuation is recovered on restart without another worker", fixture do
    assert {:ok, recovery} = RunLedger.reconcile_startup(fixture.ledger, "fixture-restart")
    assert Map.has_key?(recovery.recovered_dispatches, fixture.issue.id)
    assert recovery.cleanup_pending == %{}
    assert {:ok, events} = RunLedger.read_events(fixture.ledger)
    assert Enum.count(events, &(&1["transition"] == "run_started")) == 1
  end

  @tag :tmp_dir
  test "even a known Human Review result must not retain runnable retry ownership", fixture do
    result = Orchestrator.collect_tracker_poll_for_test(fixture.request)
    result = %{result | dispatch: {:ok, [fixture.issue]}}
    updated = Orchestrator.apply_poll_result_for_test(fixture.state, result)

    case updated.retry_attempts[fixture.issue.id] do
      %{timer_ref: ref} when is_reference(ref) -> Process.cancel_timer(ref)
      _ -> :ok
    end

    assert updated.running == %{}
    assert updated.cleanup_pending == %{}
    refute Map.has_key?(updated.retry_attempts, fixture.issue.id)
    refute MapSet.member?(updated.claimed, fixture.issue.id)
  end

  @tag :tmp_dir
  test "reusing run_stopped after completion makes replay invalid", fixture do
    event =
      Map.merge(fixture.base, %{
        transition: "run_stopped",
        stage: "released",
        terminal_reason: "tracker_non_active"
      })

    assert :ok = RunLedger.append(fixture.ledger, event)

    assert {:error, {:invalid_ledger_record, 5, {:invalid_transition_sequence, "run_stopped", :illegal_predecessor}}} =
             RunLedger.reconcile_startup(fixture.ledger, "fixture-restart")
  end

  for parked_state <- ["Human Clarification", "Deploy Ready", "Blocked"] do
    @tag :tmp_dir
    test "retires verified #{parked_state} without creating a typed wait", fixture do
      issue = %{fixture.issue | state: unquote(parked_state)}
      Application.put_env(:symphony_elixir, :parked_retry_fixture, [issue])
      updated = poll(fixture)
      assert updated.retry_attempts == %{}
      assert updated.parked == %{}
      assert updated.cleanup_pending == %{}
    end
  end

  @tag :tmp_dir
  test "recovered continuation is reconciled by the real poll request path", fixture do
    assert {:ok, recovery} = RunLedger.reconcile_startup(fixture.ledger, "fixture-restart")
    state = %{fixture.state | retry_attempts: %{}, claimed: MapSet.new(), recovered_dispatches: recovery.recovered_dispatches, recovered_attempts: recovery.recovered_attempts}
    updated = Orchestrator.run_poll_cycle_for_test(state, fixture.request.tracker_context)
    cancel_timers(updated)
    assert_received {:exact_lookup, ["fixture-177"]}
    assert updated.recovered_dispatches == %{}
    assert updated.recovered_attempts == %{}
    assert updated.running == %{}
    assert updated.cleanup_pending == %{}
    assert {:ok, replay} = RunLedger.reconcile_startup(fixture.ledger, "fixture-restart-again")
    assert replay.recovered_dispatches == %{}
  end

  for result <- [{:ok, []}, {:error, :unavailable}] do
    @tag :tmp_dir
    test "#{inspect(result)} exact lookup retains retry and affinity", fixture do
      Application.put_env(:symphony_elixir, :parked_retry_lookup, unquote(Macro.escape(result)))
      before = File.read!(fixture.ledger)
      updated = poll(fixture)
      assert updated.retry_attempts[fixture.issue.id].workspace_path == fixture.workspace
      assert updated.retry_attempts[fixture.issue.id].attempt == 1
      assert MapSet.member?(updated.claimed, fixture.issue.id)
      assert updated.cleanup_pending == %{}
      assert File.read!(fixture.ledger) == before
    end
  end

  for tracker_state <- ["Agent Running", "Done", "Unknown"] do
    @tag :tmp_dir
    test "exact #{tracker_state} is not pickup or retirement authority", fixture do
      Application.put_env(:symphony_elixir, :parked_retry_fixture, [%{fixture.issue | state: unquote(tracker_state)}])
      Application.put_env(:symphony_elixir, :parked_retry_candidates, [])
      updated = poll(fixture)
      assert Map.has_key?(updated.retry_attempts, fixture.issue.id)
      assert updated.running == %{}
      assert updated.cleanup_pending == %{}
    end
  end

  @tag :tmp_dir
  test "append failure retains ownership even if candidate list is stale active", fixture do
    result = Orchestrator.collect_tracker_poll_for_test(fixture.request)
    result = %{result | dispatch: {:ok, [%{fixture.issue | state: "Agent Running"}]}}
    state = %{fixture.state | run_ledger_append_fn: fn _, _ -> {:error, :eacces} end}
    before = File.read!(fixture.ledger)
    updated = Orchestrator.apply_poll_result_for_test(state, result)
    cancel_timers(updated)
    assert Map.has_key?(updated.retry_attempts, fixture.issue.id)
    assert MapSet.member?(updated.claimed, fixture.issue.id)
    assert updated.running == %{}
    assert updated.cleanup_pending == %{}
    assert File.read!(fixture.ledger) == before
  end

  @tag :tmp_dir
  test "stale poll cannot retire a replacement retry", fixture do
    result = Orchestrator.collect_tracker_poll_for_test(fixture.request)
    replacement = %{fixture.state.retry_attempts[fixture.issue.id] | previous_run_id: "other-run"}
    state = %{fixture.state | retry_attempts: %{fixture.issue.id => replacement}}
    updated = Orchestrator.apply_poll_result_for_test(state, result)
    cancel_timers(updated)
    assert updated.retry_attempts[fixture.issue.id].previous_run_id == "other-run"
    assert {:ok, events} = RunLedger.read_events(fixture.ledger)
    refute Enum.any?(events, &(&1["transition"] == "retry_retired"))
  end

  @tag :tmp_dir
  test "authority drift after collection prevents retirement", fixture do
    result = Orchestrator.collect_tracker_poll_for_test(fixture.request)
    path = Workflow.workflow_file_path()
    File.write!(path, String.replace(File.read!(path), "synthetic-project", "other-project"))
    before = File.read!(fixture.ledger)
    updated = Orchestrator.apply_poll_result_for_test(fixture.state, result)
    assert updated.retry_attempts == fixture.state.retry_attempts
    assert updated.operator_commands.tracker_authority_invalidated
    assert File.read!(fixture.ledger) == before
  end

  @tag :tmp_dir
  test "pause and existing same-issue owners prevent retirement", fixture do
    result = Orchestrator.collect_tracker_poll_for_test(fixture.request)
    # These maps model ownership only; poll application must not touch their I/O.
    for field <- [:running, :parked, :queued_resumes, :cleanup_pending] do
      owner = %{fixture.issue.id => %{marker: :preserve}}
      state = Map.put(fixture.state, field, owner)
      updated = Orchestrator.apply_poll_result_for_test(state, result)
      cancel_timers(updated)
      assert Map.get(updated, field) == owner
      assert Map.has_key?(updated.retry_attempts, fixture.issue.id)
    end

    paused = %{fixture.state | dispatch_paused: true}
    updated = Orchestrator.apply_poll_result_for_test(paused, result)
    cancel_timers(updated)
    assert Map.has_key?(updated.retry_attempts, fixture.issue.id)
    assert {:ok, events} = RunLedger.read_events(fixture.ledger)
    refute Enum.any?(events, &(&1["transition"] == "retry_retired"))
  end

  @tag :tmp_dir
  test "unrelated parked waits and cleanup entries are unchanged", fixture do
    state = %{fixture.state | parked: %{"other-wait" => %{marker: :preserve}}, cleanup_pending: %{"other-cleanup" => %{marker: :preserve}}, queued_resumes: %{"other-resume" => %{marker: :preserve}}}
    updated = poll(%{fixture | state: state})
    assert updated.parked == state.parked
    assert updated.cleanup_pending == state.cleanup_pending
    assert updated.queued_resumes == state.queued_resumes
    assert updated.retry_attempts == %{}
  end

  @tag :tmp_dir
  test "retirement requires exact durable identity and rejects duplicate append", fixture do
    event = retirement_event(fixture)
    before = File.read!(fixture.ledger)

    for delta <- [
          %{next_attempt: 2},
          %{attempt: 1},
          %{run_id: "missing"},
          %{issue_id: "other"},
          %{workspace_path: fixture.workspace <> "-other"},
          %{tracker_state: "Done"},
          %{release_reason: "tracker_terminal"},
          %{cleanup_error: "workspace_cleanup_failed"},
          %{wait_id: "invented"}
        ] do
      assert {:error, _} = RunLedger.append(fixture.ledger, Map.merge(event, delta))
      assert File.read!(fixture.ledger) == before
    end

    assert :ok = RunLedger.append(fixture.ledger, event)
    retired = File.read!(fixture.ledger)
    assert {:error, _} = RunLedger.append(fixture.ledger, event)
    assert File.read!(fixture.ledger) == retired
    assert {:ok, replay} = RunLedger.reconcile_startup(fixture.ledger, "fixture-replay")
    assert replay.recovered_dispatches == %{}
    assert replay.cleanup_pending == %{}
  end

  @tag :tmp_dir
  test "recovered retirement append failure is safe to retry without a worker", fixture do
    assert {:ok, recovery} = RunLedger.reconcile_startup(fixture.ledger, "fixture-restart")

    state = %{
      fixture.state
      | retry_attempts: %{},
        recovered_dispatches: recovery.recovered_dispatches,
        recovered_attempts: recovery.recovered_attempts,
        run_ledger_append_fn: fn _, _ -> {:error, :eacces} end
    }

    updated = Orchestrator.run_poll_cycle_for_test(state, fixture.request.tracker_context)
    cancel_timers(updated)
    assert updated.recovered_dispatches == state.recovered_dispatches
    assert MapSet.member?(updated.claimed, fixture.issue.id)
    assert updated.running == %{}
    assert updated.cleanup_pending == %{}
    updated = %{updated | run_ledger_append_fn: nil}
    retried = Orchestrator.run_poll_cycle_for_test(updated, fixture.request.tracker_context)
    cancel_timers(retried)
    assert retried.recovered_dispatches == %{}
    refute MapSet.member?(retried.claimed, fixture.issue.id)
  end

  @tag :tmp_dir
  test "stale retry event cannot resurrect a retired dispatch on replay", fixture do
    assert :ok = RunLedger.append(fixture.ledger, retirement_event(fixture))

    assert :ok =
             RunLedger.append(
               fixture.ledger,
               Map.merge(fixture.base, %{
                 transition: "retry_scheduled",
                 stage: "retry_queued",
                 next_action: "continuation",
                 next_attempt: 1
               })
             )

    assert {:error, _} = RunLedger.reconcile_startup(fixture.ledger, "fixture-restart")
  end

  @tag :tmp_dir
  test "retirement never falls back to cleanup for a legacy parked terminal overlap", fixture do
    path = Workflow.workflow_file_path()
    File.write!(path, String.replace(File.read!(path), "[Done, Canceled]", "[Done, Canceled, Human Review]"))
    context = Tracker.current_poll_context()
    request = %{fixture.request | tracker_context: context}

    state = %{
      fixture.state
      | operator_commands: %Orchestrator.OperatorCommandState{
          tracker_context: context,
          operator_user_ids_generation: [],
          tracker_authority_generation: context.authority_generation
        }
    }

    updated = poll(%{fixture | state: state, request: request})
    assert updated.retry_attempts == %{}
    assert updated.cleanup_pending == %{}
    assert File.exists?(Path.join(fixture.workspace, "preserved.txt"))
  end

  defp retirement_event(fixture) do
    Map.merge(fixture.base, %{transition: "retry_retired", stage: "released", next_attempt: 1, release_reason: "tracker_parked", tracker_state: "Human Review"})
  end

  defp poll(fixture) do
    result = Orchestrator.collect_tracker_poll_for_test(fixture.request)
    state = Orchestrator.apply_poll_result_for_test(fixture.state, result)
    cancel_timers(state)
    state
  end

  defp cancel_timers(state) do
    for {_id, retry} <- state.retry_attempts, is_reference(retry.timer_ref), do: Process.cancel_timer(retry.timer_ref)
    if is_reference(state.tick_timer_ref), do: Process.cancel_timer(state.tick_timer_ref)
  end
end
