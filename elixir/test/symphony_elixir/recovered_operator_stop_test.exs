defmodule SymphonyElixir.RecoveredOperatorStopTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{Linear.Comment, RunLedger}

  test "explicit adoption boundary excludes history across restarts and admits only fresh owner stop" do
    {state, issue, original_ledger, workspace} = restored_retry_fixture()
    state_dir = Path.join(Path.dirname(original_ledger), "managed-adoption")
    File.mkdir_p!(Path.join(state_dir, "logs/log"))
    {:ok, state_dir} = SymphonyElixir.PathSafety.canonicalize(state_dir)
    for dir <- [state_dir, Path.join(state_dir, "logs"), Path.join(state_dir, "logs/log")], do: File.chmod!(dir, 0o700)
    ledger = Path.join(state_dir, "logs/log/run-ledger.jsonl")
    File.cp!(original_ledger, ledger)
    File.chmod!(ledger, 0o600)
    bytes = File.read!(ledger)
    before_status = System.cmd("git", ["status", "--porcelain"], cd: workspace)
    boundary = DateTime.utc_now() |> DateTime.truncate(:millisecond)

    expected = %{
      sha256: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower),
      runner_generation: state.runner_generation,
      issue_ids: [issue.id],
      boundary: DateTime.to_iso8601(boundary)
    }

    assert {:ok, plan} = SymphonyElixir.OperatorCursorMigration.prepare(ledger, expected)
    {:ok, workflow} = SymphonyElixir.PathSafety.canonicalize(Workflow.workflow_file_path())
    assert {:ok, request} = SymphonyElixir.OperatorCursorApply.request(plan, workflow)
    assert {:ok, %{appended: 1}} = SymphonyElixir.OperatorCursorApply.apply(request, request.sha256)
    assert String.starts_with?(File.read!(ledger), bytes)
    historical = stop_comment("historical-before-adoption", DateTime.add(boundary, -1, :second))
    Application.put_env(:symphony_elixir, :memory_tracker_comments, %{issue.id => [historical]})
    first = start_state(ledger) |> poll()
    second = start_state(ledger) |> poll()
    assert first.recovered_dispatches == state.recovered_dispatches
    assert second.recovered_dispatches == state.recovered_dispatches
    assert second.parked == %{}
    assert second.operator_comment_cursors[issue.id].created_at == boundary

    fresh = stop_comment("fresh-after-adoption", DateTime.add(boundary, 1, :second))
    Application.put_env(:symphony_elixir, :memory_tracker_comments, %{issue.id => [historical, fresh]})
    assert poll(second).parked[issue.id].reason == "operator_stopped"
    assert start_state(ledger).parked[issue.id].reason == "operator_stopped"
    assert System.cmd("git", ["status", "--porcelain"], cd: workspace) == before_status
    assert {:ok, recorded} = RunLedger.read_events(ledger)
    assert Enum.filter(recorded, &(&1["transition"] == "operator_command_applied")) |> Enum.map(& &1["comment_id"]) == [fresh.id]
  end

  test "fresh owner stop parks a restored retry across two startups without touching user work" do
    {state, issue, ledger, workspace} = restored_retry_fixture()
    before_status = System.cmd("git", ["status", "--porcelain"], cd: workspace)
    assert {" M tracked.txt\n?? untracked.txt\n", 0} = before_status
    assert state.retry_attempts == %{}
    assert state.recovered_dispatches[issue.id].attempt == 2

    comment = stop_comment("fresh-stop", DateTime.utc_now())
    Application.put_env(:symphony_elixir, :memory_tracker_comments, %{issue.id => [comment]})
    stopped = poll(state)

    assert stopped.parked[issue.id].reason == "operator_stopped"
    assert stopped.parked[issue.id].attempt == 2
    assert stopped.parked[issue.id].workspace_path == workspace
    assert stopped.recovered_dispatches == %{}
    assert stopped.recovered_attempts == %{}
    assert stopped.retry_attempts == %{}
    refute MapSet.member?(stopped.claimed, issue.id)

    # The same comment cannot apply twice, including after a real startup restore.
    poll(stopped)
    restarted = start_state(ledger)
    assert restarted.parked[issue.id].reason == "operator_stopped"
    assert restarted.recovered_dispatches == %{}
    assert restarted.recovered_attempts == %{}
    assert restarted.operator_comment_cursors[issue.id].created_at == comment.created_at
    after_poll = poll(restarted)
    assert after_poll.running == %{}
    assert after_poll.cleanup_pending == %{}
    assert after_poll.parked[issue.id].reason == "operator_stopped"
    assert Application.get_env(:symphony_elixir, :memory_tracker_issues) == [issue]
    assert System.cmd("git", ["status", "--porcelain"], cd: workspace) == before_status
    assert File.read!(Path.join(workspace, "tracked.txt")) == "modified\n"
    assert File.read!(Path.join(workspace, "untracked.txt")) == "untracked\n"

    assert {:ok, events} = RunLedger.read_events(ledger)
    assert Enum.count(events, &(&1["transition"] == "retry_parked")) == 1
    assert Enum.count(events, &(&1["transition"] == "operator_command_applied")) == 1
    assert Enum.count(events, &(&1["transition"] == "run_started")) == 1
    refute Enum.any?(events, &String.starts_with?(&1["transition"], "workspace_cleanup_"))
  end

  for field <- ~w(attempt previous_run_id identifier worker_host workspace_path workspace_root)a do
    test "stop rejects conflicting live and recovered #{field} without dropping either" do
      {state, issue, ledger, _workspace} = restored_retry_fixture()
      retry = Map.put(state.recovered_dispatches[issue.id], :timer_ref, nil)
      value = if unquote(field) == :attempt, do: 3, else: "conflicting-#{unquote(field)}"
      recovered = Map.put(retry, unquote(field), value)
      state = %{state | retry_attempts: %{issue.id => retry}, recovered_dispatches: %{issue.id => recovered}, recovered_attempts: %{issue.id => recovered.attempt}}
      comment = stop_comment("conflicting-stop", DateTime.utc_now())
      Application.put_env(:symphony_elixir, :memory_tracker_comments, %{issue.id => [comment]})

      rejected = poll(state)
      assert rejected.parked == %{}
      assert rejected.retry_attempts == state.retry_attempts
      assert rejected.recovered_dispatches == state.recovered_dispatches
      assert rejected.recovered_attempts == state.recovered_attempts
      assert {:ok, events} = RunLedger.read_events(ledger)
      refute Enum.any?(events, &(&1["transition"] == "retry_parked"))
      assert Enum.any?(events, &(&1["transition"] == "operator_command_rejected"))
    end
  end

  test "stop does not consume a recovered retry when a queued resume also owns the issue" do
    {state, issue, _ledger, _workspace} = restored_retry_fixture()
    state = %{state | queued_resumes: %{issue.id => %{attempt: 4, run_id: "another-run"}}}
    Application.put_env(:symphony_elixir, :memory_tracker_comments, %{issue.id => [stop_comment("resume-conflict", DateTime.utc_now())]})
    rejected = poll(state)
    assert rejected.parked == %{}
    assert rejected.recovered_dispatches == state.recovered_dispatches
    assert rejected.queued_resumes == state.queued_resumes
  end

  test "failed park append retains the recovered queue and workspace across restart" do
    {state, issue, ledger, workspace} = restored_retry_fixture()

    state = %{
      state
      | run_ledger_append_fn: fn path, event ->
          if event.transition == "retry_parked", do: {:error, :enospc}, else: RunLedger.append(path, event)
        end
    }

    Application.put_env(:symphony_elixir, :memory_tracker_comments, %{issue.id => [stop_comment("failed-append", DateTime.utc_now())]})
    failed = poll(state)
    assert failed.parked == %{}
    assert failed.recovered_dispatches == state.recovered_dispatches
    assert failed.recovered_attempts == state.recovered_attempts
    assert failed.claimed == state.claimed
    assert File.read!(Path.join(workspace, "tracked.txt")) == "modified\n"
    recovered = start_state(ledger)
    assert recovered.recovered_dispatches == state.recovered_dispatches
    assert recovered.parked == %{}
    assert {:ok, events} = RunLedger.read_events(ledger)
    refute Enum.any?(events, &(&1["transition"] in ["retry_parked", "operator_command_applied"]))
  end

  test "a recovered issue without a cursor ignores history and runner comments until a fresh owner stop" do
    {state, issue, ledger, _workspace} = restored_retry_fixture(cursor: false)
    historical = stop_comment("historical-stop", DateTime.add(DateTime.utc_now(), -60, :second))
    Application.put_env(:symphony_elixir, :memory_tracker_comments, %{issue.id => [historical]})
    migrated = poll(state)
    assert migrated.parked == %{}
    assert migrated.recovered_dispatches == state.recovered_dispatches
    cursor = migrated.operator_comment_cursors[issue.id].created_at
    assert DateTime.compare(cursor, historical.created_at) == :gt

    wrong_actor = %{stop_comment("wrong-actor", DateTime.add(cursor, 1, :second)) | author_id: "runner"}
    self_authored = %{stop_comment("self-authored", DateTime.add(cursor, 2, :second)) | author_is_me: true}
    Application.put_env(:symphony_elixir, :memory_tracker_comments, %{issue.id => [historical, wrong_actor, self_authored]})
    ignored = poll(migrated)
    assert ignored.parked == %{}
    assert ignored.recovered_dispatches == state.recovered_dispatches

    fresh = stop_comment("fresh-after-migration", DateTime.add(cursor, 3, :second))
    Application.put_env(:symphony_elixir, :memory_tracker_comments, %{issue.id => [historical, wrong_actor, self_authored, fresh]})
    assert poll(ignored).parked[issue.id].reason == "operator_stopped"
    assert {:ok, events} = RunLedger.read_events(ledger)
    assert Enum.count(events, &(&1["transition"] == "operator_cursor_initialized")) == 1

    assert Enum.filter(events, &(&1["transition"] == "operator_command_applied"))
           |> Enum.map(& &1["comment_id"]) == [fresh.id]
  end

  test "matching live and recovered entries park once and cancel the live timer after durable append" do
    {state, issue, ledger, _workspace} = restored_retry_fixture()
    timer = Process.send_after(self(), :must_not_retry, 60_000)
    on_exit(fn -> Process.cancel_timer(timer) end)
    retry = Map.put(state.recovered_dispatches[issue.id], :timer_ref, timer)

    state = %{
      state
      | retry_attempts: %{issue.id => retry},
        claimed: MapSet.new([issue.id]),
        run_ledger_append_fn: fn path, event ->
          if event.transition == "retry_parked", do: assert(is_integer(Process.read_timer(timer)))
          RunLedger.append(path, event)
        end
    }

    Application.put_env(:symphony_elixir, :memory_tracker_comments, %{issue.id => [stop_comment("matching-entries", DateTime.utc_now())]})
    stopped = poll(state)
    assert stopped.parked[issue.id].reason == "operator_stopped"
    assert stopped.retry_attempts == %{}
    assert stopped.recovered_dispatches == %{}
    assert stopped.recovered_attempts == %{}
    assert stopped.claimed == MapSet.new()
    assert Process.read_timer(timer) == false
    assert start_state(ledger).recovered_dispatches == %{}
  end

  defp restored_retry_fixture(opts \\ []) do
    root = Path.dirname(Workflow.workflow_file_path())
    workspace_root = Path.join(root, "workspaces")
    File.mkdir_p!(workspace_root)
    {:ok, workspace_root} = SymphonyElixir.PathSafety.canonicalize(workspace_root)
    workspace = Path.join(workspace_root, "MT-RESTORED")
    File.mkdir_p!(workspace)
    System.cmd("git", ["init", "--quiet"], cd: workspace)
    File.write!(Path.join(workspace, "tracked.txt"), "original\n")
    System.cmd("git", ["add", "tracked.txt"], cd: workspace)
    System.cmd("git", ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "fixture"], cd: workspace)
    File.write!(Path.join(workspace, "tracked.txt"), "modified\n")
    File.write!(Path.join(workspace, "untracked.txt"), "untracked\n")

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      tracker_operator_user_ids: ["operator-1"],
      workspace_root: workspace_root
    )

    issue = %Issue{id: "issue-restored", identifier: "MT-RESTORED", title: "Restored retry", state: "Done"}
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue])
    ledger = Path.join(root, "restored-ledger.jsonl")
    base = %{run_id: "previous-run", issue_id: issue.id, issue_identifier: issue.identifier, attempt: 1, worker_host: nil, workspace_path: workspace, workspace_root: workspace_root}

    for event <- [
          %{transition: "run_claimed", stage: "claimed"},
          %{transition: "run_started", stage: "running"},
          %{transition: "run_failed", stage: "released", terminal_reason: "worker_exit", next_action: "retry", next_attempt: 2},
          %{transition: "retry_scheduled", stage: "retry_queued", next_action: "retry", next_attempt: 2}
        ] do
      assert :ok = RunLedger.append(ledger, Map.merge(base, event))
    end

    assert :ok = RunLedger.append(ledger, %{transition: "dispatch_paused", stage: "operator", runner_generation: "fixture-runner"})

    if Keyword.get(opts, :cursor, true) do
      assert :ok =
               RunLedger.append(ledger, %{
                 transition: "operator_cursor_initialized",
                 stage: "operator",
                 runner_generation: "fixture-runner",
                 issue_id: issue.id,
                 comment_created_at: DateTime.to_iso8601(DateTime.add(DateTime.utc_now(), -60, :second))
               })
    end

    {start_state(ledger), issue, ledger, workspace}
  end

  defp start_state(ledger) do
    assert {:ok, state} = Orchestrator.init(run_ledger_path: ledger)
    Process.cancel_timer(state.tick_timer_ref)
    state
  end

  defp poll(state) do
    state = Orchestrator.run_poll_cycle_for_test(state, Tracker.current_poll_context())
    if is_reference(state.tick_timer_ref), do: Process.cancel_timer(state.tick_timer_ref)
    state
  end

  defp stop_comment(id, created_at) do
    %Comment{id: id, body: "$stop", created_at: created_at, author_id: "operator-1"}
  end
end
