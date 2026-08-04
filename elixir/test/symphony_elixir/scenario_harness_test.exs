defmodule SymphonyElixir.ScenarioHarnessTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.ScenarioHarness

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
      codex_command: "#{fake.binary} app-server"
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
      assert count_transition(ScenarioHarness.events(restarted), "run_claimed") == 1
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

  test "capability preflight rejects unknown tools before app-server launch" do
    root = scenario_root("capability-preflight")
    on_exit(fn -> File.rm_rf(root) end)

    fake = ScenarioHarness.write_fake_codex!(root, :live_ok)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      workspace_root: Path.join(root, "workspaces"),
      codex_command: "#{fake.binary} app-server",
      codex_dynamic_tool_allowlist: ["unknown_tool"]
    )

    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.dynamic_tool_allowlist"
    refute File.exists?(fake.trace)
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
end
