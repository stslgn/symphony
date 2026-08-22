defmodule SymphonyElixir.ScenarioHarnessTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{RunLedger, ScenarioHarness, TrackerAdmission}

  @moduletag :scenario

  setup do
    previous_discovery =
      Application.get_env(:symphony_elixir, :codex_model_discovery_enabled)

    Application.put_env(:symphony_elixir, :codex_model_discovery_enabled, true)

    on_exit(fn ->
      if is_nil(previous_discovery) do
        Application.delete_env(:symphony_elixir, :codex_model_discovery_enabled)
      else
        Application.put_env(
          :symphony_elixir,
          :codex_model_discovery_enabled,
          previous_discovery
        )
      end
    end)

    :ok
  end

  test "crash after admission mutation reconciles once before the first model start" do
    root = scenario_root("pre-model-admission-restart")
    on_exit(fn -> File.rm_rf(root) end)

    raw_workspace_root = Path.join(root, "workspaces")
    File.mkdir_p!(raw_workspace_root)
    {:ok, workspace_root} = SymphonyElixir.PathSafety.canonicalize(raw_workspace_root)
    ledger_path = Path.join(root, "run-ledger.jsonl")
    fake = ScenarioHarness.write_fake_codex!(root, :live_ok)
    ready_issue = issue("issue-admission-restart", "SCN-ADMISSION-RESTART")
    running_issue = %{ready_issue | state: "Agent Running"}
    workspace_path = Path.join(workspace_root, ready_issue.identifier)
    File.mkdir_p!(workspace_path)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      tracker_active_states: ["Agent Ready", "Agent Running"],
      workspace_root: workspace_root,
      poll_interval_ms: 60_000,
      max_concurrent_agents: 1,
      max_turns: 1,
      codex_command: "#{fake.binary} app-server",
      prompt: "Admission={{ run.admission.id }} Run={{ run.id }}"
    )

    tracker_context = Tracker.current_poll_context()

    assert {:ok, admission, _snapshot} =
             TrackerAdmission.packet(
               ready_issue,
               tracker_context,
               "admission-scenario-restart",
               "Agent Running"
             )

    base = %{
      run_id: "run-admission-scenario-restart",
      issue_id: ready_issue.id,
      issue_identifier: ready_issue.identifier,
      attempt: 0,
      workspace_path: workspace_path,
      workspace_root: workspace_root
    }

    assert :ok =
             RunLedger.append(
               ledger_path,
               Map.merge(base, %{transition: "run_claimed", stage: "claimed"})
             )

    assert :ok =
             RunLedger.append(
               ledger_path,
               base
               |> Map.merge(admission)
               |> Map.merge(%{transition: "tracker_admission_io_started", stage: "admission"})
             )

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [running_issue])
    Application.put_env(:symphony_elixir, :memory_tracker_recipient, self())

    harness =
      ScenarioHarness.start!(
        Module.concat(__MODULE__, :PreModelAdmissionRestartRunner),
        ledger_path
      )

    try do
      snapshot =
        ScenarioHarness.await_snapshot(harness, fn snapshot ->
          length(snapshot.parked) == 1
        end)

      expected_issue_id = ready_issue.id
      refute_received {:memory_tracker_state_update, ^expected_issue_id, "Agent Running"}
      assert snapshot.running == []
      assert [%{reason: "run_budget_exhausted"}] = snapshot.parked

      events = ScenarioHarness.events(harness)
      assert count_transition(events, "run_claimed") == 1
      assert count_transition(events, "tracker_admission_io_started") == 1
      assert count_transition(events, "tracker_admission_completed") == 1
      assert count_transition(events, "run_started") == 1
      assert count_transition(events, "model_resolved") == 1

      trace = File.read!(fake.trace)
      assert trace =~ "Admission=admission-scenario-restart"
      assert :ok = ScenarioHarness.assert_consistent!(harness)
    after
      ScenarioHarness.stop(harness)
      Application.delete_env(:symphony_elixir, :memory_tracker_recipient)
    end
  end

  test "duplicate wake-ups stay paused, dispatch once, and restore the parked run after restart" do
    root = scenario_root("wake-pause-restart")
    on_exit(fn -> File.rm_rf(root) end)

    ledger_path = Path.join(root, "run-ledger.jsonl")
    fake = ScenarioHarness.write_fake_codex!(root, :live_ok)

    issue = issue("issue-scenario-wake", "SCN-1")

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      tracker_active_states: ["Agent Ready", "Agent Running"],
      workspace_root: Path.join(root, "workspaces"),
      poll_interval_ms: 60_000,
      max_concurrent_agents: 1,
      max_turns: 1,
      codex_command: "#{fake.binary} app-server",
      prompt: "Attempt={{ run.attempt }} Run={{ run.id }}"
    )

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    harness = ScenarioHarness.start!(Module.concat(__MODULE__, :WakeRunner), ledger_path)

    try do
      ScenarioHarness.await_poll_idle(harness)

      assert {:ok, %{dispatch_paused: true, changed: true}} =
               Orchestrator.set_dispatch_paused(harness.name, true)

      Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue])

      assert %{queued: true, coalesced: false} =
               Orchestrator.request_refresh(harness.name)

      assert %{queued: true, coalesced: true} =
               Orchestrator.request_refresh(harness.name)

      paused_snapshot = ScenarioHarness.await_poll_idle(harness)
      assert paused_snapshot.control.dispatch_paused
      assert paused_snapshot.running == []
      assert count_transition(ScenarioHarness.events(harness), "run_claimed") == 0

      assert {:ok, %{dispatch_paused: false, changed: true}} =
               Orchestrator.set_dispatch_paused(harness.name, false)

      snapshot =
        ScenarioHarness.await_snapshot(harness, fn snapshot ->
          length(snapshot.parked) == 1
        end)

      assert snapshot.running == []
      assert snapshot.retrying == []
      assert [wait] = snapshot.parked
      assert wait.reason == "run_budget_exhausted"
      assert wait.terminal_reason == "turn_budget_exhausted"
      first_run_id = wait.run_id

      events = ScenarioHarness.events(harness)
      assert count_transition(events, "run_claimed") == 1
      assert count_transition(events, "model_resolved") == 1
      assert count_transition(events, "run_parked") == 1

      assert Enum.any?(events, fn event ->
               event["transition"] == "model_resolved" and
                 event["resolved_model"] == "gpt-live" and
                 event["reasoning_effort"] == "low" and
                 event["model_catalog_source"] == "live"
             end)

      trace = File.read!(fake.trace)
      assert trace =~ ~s("method":"model/list")
      assert trace =~ ~s("method":"thread/start")
      assert trace =~ ~s("method":"turn/start")
      refute trace =~ ~s("method":"config/read")

      assert {:ok, %{resumed: true}} =
               Orchestrator.resolve_wait(harness.name, issue.id, wait.wait_id, "retry")

      resumed_snapshot =
        ScenarioHarness.await_snapshot(harness, fn snapshot ->
          case snapshot.parked do
            [%{run_id: run_id, attempt: 1}] when run_id != first_run_id -> true
            _other -> false
          end
        end)

      assert [%{run_id: resumed_run_id, attempt: 1}] = resumed_snapshot.parked
      refute resumed_run_id == first_run_id

      resumed_events = ScenarioHarness.events(harness)
      assert count_transition(resumed_events, "run_claimed") == 2

      assert Enum.any?(resumed_events, fn event ->
               event["transition"] == "resume_queued" and event["attempt"] == 1
             end)

      assert Enum.any?(resumed_events, fn event ->
               event["transition"] == "run_started" and
                 event["run_id"] == resumed_run_id and event["attempt"] == 1
             end)

      resumed_trace = File.read!(fake.trace)
      assert resumed_trace =~ "Attempt=1 Run=#{resumed_run_id}"

      assert :ok = ScenarioHarness.assert_consistent!(harness)
    after
      ScenarioHarness.stop(harness)
    end

    restarted = ScenarioHarness.start!(Module.concat(__MODULE__, :RestartedWakeRunner), ledger_path)

    try do
      snapshot =
        ScenarioHarness.await_snapshot(restarted, fn snapshot ->
          length(snapshot.parked) == 1
        end)

      assert snapshot.running == []
      assert snapshot.retrying == []
      assert [wait] = snapshot.parked
      assert wait.reason == "run_budget_exhausted"
      assert wait.terminal_reason == "turn_budget_exhausted"
      assert wait.attempt == 1
      assert count_transition(ScenarioHarness.events(restarted), "run_claimed") == 2
      assert :ok = ScenarioHarness.assert_consistent!(restarted)
    after
      ScenarioHarness.stop(restarted)
    end
  end

  test "batched token telemetry parks once at the first observed crossing" do
    root = scenario_root("token-overshoot")
    on_exit(fn -> File.rm_rf(root) end)

    ledger_path = Path.join(root, "run-ledger.jsonl")
    issue = issue("issue-scenario-token", "SCN-2")

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      tracker_active_states: ["Agent Ready", "Agent Running"],
      workspace_root: Path.join(root, "workspaces"),
      poll_interval_ms: 60_000,
      max_run_tokens: 150_000
    )

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    harness = ScenarioHarness.start!(Module.concat(__MODULE__, :TokenRunner), ledger_path)

    try do
      ScenarioHarness.await_poll_idle(harness)

      assert {:ok, %{dispatch_paused: true}} =
               Orchestrator.set_dispatch_paused(harness.name, true)

      Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue])
      ScenarioHarness.seed_running!(harness, issue, run_id: "run-token", max_tokens: 150_000)

      assert :ok = ScenarioHarness.report_tokens(harness, issue, "run-token", 135_693, 1_335)

      below_limit =
        ScenarioHarness.await_snapshot(harness, fn snapshot ->
          case snapshot.running do
            [%{codex_total_tokens: 135_693}] -> true
            _ -> false
          end
        end)

      assert below_limit.parked == []

      assert :ok = ScenarioHarness.report_tokens(harness, issue, "run-token", 171_836, 1_472)

      crossed =
        ScenarioHarness.await_snapshot(harness, fn snapshot ->
          length(snapshot.parked) == 1
        end)

      assert crossed.running == []
      assert crossed.retrying == []
      assert crossed.codex_totals.total_tokens == 171_836
      assert [wait] = crossed.parked
      assert wait.reason == "run_budget_exhausted"
      assert wait.terminal_reason == "token_budget_exhausted"

      assert :ok = ScenarioHarness.report_tokens(harness, issue, "run-token", 171_836, 1_472)
      _snapshot_after_duplicate = Orchestrator.snapshot(harness.name, 1_000)

      events = ScenarioHarness.events(harness)
      assert count_transition(events, "run_parked") == 1
      assert count_transition(events, "run_failed") == 0
      assert count_transition(events, "run_completed") == 0
      assert :ok = ScenarioHarness.assert_consistent!(harness)
    after
      ScenarioHarness.stop(harness)
    end
  end

  test "cached input does not consume the uncached-input scenario budget" do
    root = scenario_root("uncached-token-budget")
    on_exit(fn -> File.rm_rf(root) end)

    ledger_path = Path.join(root, "run-ledger.jsonl")
    issue = %{issue("issue-scenario-uncached", "SCN-UNCACHED") | state: "Agent Running"}

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      tracker_active_states: ["Agent Ready", "Agent Running"],
      workspace_root: Path.join(root, "workspaces"),
      poll_interval_ms: 60_000,
      max_run_uncached_input_tokens: 100_000
    )

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    harness = ScenarioHarness.start!(Module.concat(__MODULE__, :UncachedTokenRunner), ledger_path)

    try do
      ScenarioHarness.await_poll_idle(harness)

      assert {:ok, %{dispatch_paused: true}} =
               Orchestrator.set_dispatch_paused(harness.name, true)

      ScenarioHarness.seed_running!(harness, issue,
        run_id: "run-uncached-token",
        max_uncached_input_tokens: 100_000
      )

      assert :ok =
               ScenarioHarness.report_token_usage(harness, issue, "run-uncached-token", %{
                 "input_tokens" => 200_000,
                 "cached_input_tokens" => 125_000,
                 "output_tokens" => 10_000,
                 "total_tokens" => 210_000
               })

      below =
        ScenarioHarness.await_snapshot(harness, fn snapshot ->
          case snapshot.running do
            [%{codex_uncached_input_tokens: 75_000}] -> true
            _other -> false
          end
        end)

      assert below.parked == []

      assert :ok =
               ScenarioHarness.report_token_usage(harness, issue, "run-uncached-token", %{
                 "input_tokens" => 300_000,
                 "cached_input_tokens" => 200_000,
                 "output_tokens" => 15_000,
                 "total_tokens" => 315_000
               })

      crossed =
        ScenarioHarness.await_snapshot(harness, fn snapshot ->
          length(snapshot.parked) == 1
        end)

      assert [%{terminal_reason: "uncached_input_budget_exhausted"}] = crossed.parked
      assert count_transition(ScenarioHarness.events(harness), "run_parked") == 1
      assert :ok = ScenarioHarness.assert_consistent!(harness)
    after
      ScenarioHarness.stop(harness)
    end
  end

  test "budget park followed by human clarification preserves dirty workspace across restart" do
    root = scenario_root("budget-human-clarification")
    on_exit(fn -> File.rm_rf(root) end)

    workspace_root = Path.join(root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-152")
    sentinel = Path.join(workspace, "uncommitted-migration.sql")
    ledger_path = Path.join(root, "run-ledger.jsonl")
    running_issue = %{issue("issue-scenario-preservation", "DUD-152") | state: "Agent Running"}
    clarification_issue = %{running_issue | state: "Human Clarification"}

    File.mkdir_p!(workspace)
    File.write!(sentinel, "create table preserved_work();\n")

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      tracker_active_states: ["Agent Ready", "Agent Running"],
      tracker_terminal_states: ["Done", "Human Clarification"],
      workspace_root: workspace_root,
      poll_interval_ms: 60_000,
      max_run_tokens: 150_000
    )

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    harness = ScenarioHarness.start!(Module.concat(__MODULE__, :PreservationRunner), ledger_path)

    try do
      ScenarioHarness.await_poll_idle(harness)

      assert {:ok, %{dispatch_paused: true}} =
               Orchestrator.set_dispatch_paused(harness.name, true)

      ScenarioHarness.seed_running!(harness, running_issue,
        run_id: "run-preservation",
        max_tokens: 150_000,
        workspace_path: workspace
      )

      assert :ok =
               ScenarioHarness.report_tokens(
                 harness,
                 running_issue,
                 "run-preservation",
                 171_836,
                 1_472
               )

      parked =
        ScenarioHarness.await_snapshot(harness, fn snapshot ->
          length(snapshot.parked) == 1
        end)

      assert [%{reason: "run_budget_exhausted"}] = parked.parked

      Application.put_env(:symphony_elixir, :memory_tracker_issues, [clarification_issue])
      assert %{queued: true} = Orchestrator.request_refresh(harness.name)

      clarified =
        ScenarioHarness.await_snapshot(harness, fn snapshot ->
          case snapshot.parked do
            [%{tracker_state: "Human Clarification"}] -> true
            _other -> false
          end
        end)

      assert clarified.running == []
      assert clarified.retrying == []
      assert [%{reason: "run_budget_exhausted"}] = clarified.parked
      assert File.read!(sentinel) == "create table preserved_work();\n"
      refute Enum.any?(ScenarioHarness.events(harness), &cleanup_transition?/1)
      assert :ok = ScenarioHarness.assert_consistent!(harness)
    after
      ScenarioHarness.stop(harness)
    end

    restarted =
      ScenarioHarness.start!(Module.concat(__MODULE__, :RestartedPreservationRunner), ledger_path)

    try do
      snapshot =
        ScenarioHarness.await_snapshot(restarted, fn snapshot ->
          case snapshot.parked do
            [%{tracker_state: "Human Clarification"}] -> true
            _other -> false
          end
        end)

      assert snapshot.running == []
      assert snapshot.retrying == []
      assert [%{reason: "run_budget_exhausted"}] = snapshot.parked
      assert File.read!(sentinel) == "create table preserved_work();\n"
      refute Enum.any?(ScenarioHarness.events(restarted), &cleanup_transition?/1)
      assert :ok = ScenarioHarness.assert_consistent!(restarted)
    after
      ScenarioHarness.stop(restarted)
    end
  end

  test "live model mismatch blocks prompt delivery while unavailable discovery stays compatible" do
    root = scenario_root("model-boundary")
    on_exit(fn -> File.rm_rf(root) end)

    workspace_root = Path.join(root, "workspaces")
    workspace = Path.join(workspace_root, "SCN-3")
    File.mkdir_p!(workspace)

    mismatch = ScenarioHarness.write_fake_codex!(root, :live_mismatch)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: workspace_root,
      codex_command: "#{mismatch.binary} app-server"
    )

    issue = issue("issue-scenario-model", "SCN-3")

    assert {:error, {:model_not_available, "gpt-missing"}} =
             AppServer.run(workspace, "must not be delivered", issue)

    mismatch_trace = File.read!(mismatch.trace)
    assert mismatch_trace =~ ~s("method":"model/list")
    assert mismatch_trace =~ ~s("method":"thread/start")
    refute mismatch_trace =~ ~s("method":"turn/start")
    refute mismatch_trace =~ "must not be delivered"
    refute mismatch_trace =~ ~s("method":"config/read")

    unavailable = ScenarioHarness.write_fake_codex!(root, :unavailable)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: workspace_root,
      codex_command: "#{unavailable.binary} app-server"
    )

    assert {:ok, result} = AppServer.run(workspace, "compatibility prompt", issue)
    assert result.resolved_model == "gpt-live"
    assert result.reasoning_effort == "low"
    assert result.model_catalog.source == "unavailable"
    assert result.model_catalog.error == "request_failed"

    unavailable_trace = File.read!(unavailable.trace)
    assert unavailable_trace =~ ~s("method":"model/list")
    assert unavailable_trace =~ ~s("method":"turn/start")
    refute unavailable_trace =~ ~s("method":"config/read")
  end

  test "capability preflight blocks claims, tasks, and app-server launch when a required tool is missing" do
    root = scenario_root("capability-preflight")
    on_exit(fn -> File.rm_rf(root) end)

    ledger_path = Path.join(root, "run-ledger.jsonl")
    workspace = Path.join(root, "workspaces/SCN-CAP")
    fake = ScenarioHarness.write_fake_codex!(root, :live_ok)
    required_issue = issue("issue-capability-preflight", "SCN-CAP")

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      workspace_root: Path.join(root, "workspaces"),
      codex_command: "#{fake.binary} app-server",
      codex_dynamic_tool_allowlist: [],
      codex_required_dynamic_tools: ["linear_graphql"]
    )

    File.mkdir_p!(workspace)

    assert {:error, {:missing_required_dynamic_tools, ["linear_graphql"]}} =
             AppServer.start_session(workspace)

    refute File.exists?(fake.trace)

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [required_issue])
    task_children_before = MapSet.new(Task.Supervisor.children(SymphonyElixir.TaskSupervisor))
    harness = ScenarioHarness.start!(Module.concat(__MODULE__, :CapabilityRunner), ledger_path)

    try do
      snapshot = ScenarioHarness.await_poll_idle(harness)
      assert snapshot.running == []
      assert snapshot.retrying == []
      assert count_transition(ScenarioHarness.events(harness), "run_claimed") == 0
      assert MapSet.new(Task.Supervisor.children(SymphonyElixir.TaskSupervisor)) == task_children_before
      refute File.exists?(fake.trace)
    after
      ScenarioHarness.stop(harness)
    end
  end

  test "consistency check rejects duplicate issue ids inside retrying" do
    root = scenario_root("duplicate-retrying")
    on_exit(fn -> File.rm_rf(root) end)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      workspace_root: Path.join(root, "workspaces"),
      poll_interval_ms: 60_000
    )

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])

    harness =
      ScenarioHarness.start!(
        Module.concat(__MODULE__, :DuplicateRetryRunner),
        Path.join(root, "run-ledger.jsonl")
      )

    try do
      ScenarioHarness.await_poll_idle(harness)
      issue_id = "issue-duplicate-retrying"

      :sys.replace_state(harness.pid, fn state ->
        %{
          state
          | retry_attempts: %{
              issue_id => %{
                attempt: 2,
                status: :durability_pending,
                due_at_ms: nil,
                identifier: "SCN-DUP",
                previous_run_id: "run-duplicate-retry"
              }
            },
            queued_resumes: %{
              issue_id => %{
                issue_id: issue_id,
                identifier: "SCN-DUP",
                run_id: "run-duplicate-resume",
                wait_id: "wait-duplicate-resume",
                attempt: 2,
                stage: "resume_queued",
                queued_at: DateTime.utc_now()
              }
            }
        }
      end)

      assert_raise ExUnit.AssertionError, ~r/duplicate issue ids in retrying/, fn ->
        ScenarioHarness.assert_consistent!(harness)
      end
    after
      ScenarioHarness.stop(harness)
    end
  end

  defp scenario_root(name) do
    Path.join(
      System.tmp_dir!(),
      "symphony-scenario-#{name}-#{System.unique_integer([:positive])}"
    )
  end

  defp issue(id, identifier) do
    %Issue{
      id: id,
      identifier: identifier,
      title: "Scenario #{identifier}",
      description: "Synthetic offline scenario",
      state: "Agent Ready",
      url: "https://example.invalid/#{identifier}",
      labels: []
    }
  end

  defp count_transition(events, transition) do
    Enum.count(events, &(&1["transition"] == transition))
  end

  defp cleanup_transition?(event) do
    event["transition"]
    |> to_string()
    |> String.starts_with?("workspace_cleanup_")
  end
end
