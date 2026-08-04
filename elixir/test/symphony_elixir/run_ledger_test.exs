defmodule SymphonyElixir.RunLedgerTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RunLedger

  test "appends bounded events with restrictive file permissions" do
    path = ledger_path()

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_started",
               stage: "running",
               run_id: "run-1",
               issue_id: "issue-1",
               issue_identifier: "DUD-1",
               attempt: 0,
               resolved_model: "gpt-live",
               reasoning_effort: "high",
               model_catalog_source: "live",
               error_summary: "provider response must not persist",
               raw_prompt: "must not persist"
             })

    assert {:ok, [event]} = RunLedger.read_events(path)
    assert event["transition"] == "run_started"
    assert event["resolved_model"] == "gpt-live"
    assert event["reasoning_effort"] == "high"
    assert event["model_catalog_source"] == "live"
    refute Map.has_key?(event, "error_summary")
    refute Map.has_key?(event, "raw_prompt")

    assert {:ok, stat} = File.stat(path)
    assert Bitwise.band(stat.mode, 0o777) == 0o600
  end

  test "startup reconciliation closes unfinished runs and returns incremented attempts" do
    path = ledger_path()

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_started",
               stage: "running",
               runner_generation: "runner-old",
               run_id: "run-stale",
               issue_id: "issue-stale",
               issue_identifier: "DUD-2",
               attempt: 2,
               workspace_path: "/tmp/workspaces/DUD-2"
             })

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-new")
    assert recovery.recovered_attempts == %{"issue-stale" => 3}
    assert recovery.parked == %{}
    refute recovery.dispatch_paused
    assert recovery.processed_operator_comment_ids == MapSet.new()
    assert recovery.operator_comment_cursors == %{}

    assert {:ok, events} = RunLedger.read_events(path)

    assert Enum.any?(events, fn event ->
             event["run_id"] == "run-stale" and
               event["transition"] == "run_interrupted" and
               event["terminal_reason"] == "runner_restarted"
           end)

    assert Enum.any?(events, fn event ->
             event["transition"] == "runner_started" and
               event["runner_generation"] == "runner-new"
           end)

    assert {:ok, next_recovery} = RunLedger.reconcile_startup(path, "runner-next")
    assert next_recovery.recovered_attempts == %{}
    assert next_recovery.parked == %{}
  end

  test "ignores malformed trailing records during recovery" do
    path = ledger_path()
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, "{\"transition\":\"run_started\"\n")

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-new")
    assert recovery.recovered_attempts == %{}
    assert recovery.parked == %{}
  end

  test "ignores non-object records and defaults malformed attempts" do
    path = ledger_path()
    File.mkdir_p!(Path.dirname(path))

    File.write!(
      path,
      [
        Jason.encode!([]),
        "\n",
        Jason.encode!(%{
          "transition" => "run_started",
          "run_id" => "run-malformed-attempt",
          "issue_id" => "issue-malformed-attempt",
          "attempt" => "not-an-integer"
        }),
        "\n"
      ]
    )

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-new")
    assert recovery.recovered_attempts == %{"issue-malformed-attempt" => 1}
    assert recovery.parked == %{}
  end

  test "startup reconciliation restores parked waits until they are resumed" do
    path = ledger_path()

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_parked",
               stage: "parked",
               run_id: "run-parked",
               issue_id: "issue-parked",
               issue_identifier: "DUD-3",
               attempt: 1,
               wait_id: "wait-parked",
               parked_reason: "waiting_owner",
               allowed_actions: ["approve", "reject"],
               tracker_state: "Human Review"
             })

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-new")
    assert %{"issue-parked" => parked} = recovery.parked
    assert recovery.recovered_attempts == %{}

    assert parked["wait_id"] == "wait-parked"
    assert parked["parked_reason"] == "waiting_owner"

    assert :ok =
             RunLedger.append(path, %{
               transition: "wait_resumed",
               stage: "released",
               issue_id: "issue-parked",
               wait_id: "wait-parked"
             })

    assert {:ok, next_recovery} = RunLedger.reconcile_startup(path, "runner-next")
    assert next_recovery.recovered_attempts == %{}
    assert next_recovery.parked == %{}
  end

  test "startup reconciliation restores global pause and operator command cursors" do
    path = ledger_path()

    assert :ok = RunLedger.append(path, %{transition: "dispatch_paused", stage: "operator"})

    assert :ok =
             RunLedger.append(path, %{
               transition: "operator_cursor_initialized",
               stage: "operator",
               issue_id: "issue-1",
               comment_created_at: "2026-08-03T09:59:59.000Z"
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "operator_cursor_advanced",
               stage: "operator",
               issue_id: "issue-1",
               comment_id: "comment-1",
               comment_created_at: "2026-08-03T10:00:00.000Z"
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "operator_cursor_advanced",
               stage: "operator",
               issue_id: "issue-1",
               comment_id: "comment-2",
               comment_created_at: "2026-08-03T10:00:00.000Z"
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "operator_cursor_advanced",
               stage: "operator",
               issue_id: "issue-1",
               comment_id: "comment-old",
               comment_created_at: "2026-08-03T09:59:58.000Z"
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "operator_command_applied",
               stage: "operator",
               issue_id: "issue-1",
               comment_id: "comment-1",
               operator_command: "retry"
             })

    assert {:ok, paused_recovery} = RunLedger.reconcile_startup(path, "runner-paused")
    assert paused_recovery.dispatch_paused
    assert paused_recovery.processed_operator_comment_ids == MapSet.new(["comment-1"])

    assert paused_recovery.operator_comment_cursors == %{
             "issue-1" => %{
               created_at: "2026-08-03T10:00:00.000Z",
               comment_ids: MapSet.new(["comment-1", "comment-2"])
             }
           }

    assert :ok = RunLedger.append(path, %{transition: "dispatch_resumed", stage: "operator"})
    assert {:ok, resumed_recovery} = RunLedger.reconcile_startup(path, "runner-resumed")
    refute resumed_recovery.dispatch_paused
  end

  test "returns filesystem read and create errors" do
    assert {:ok, []} = RunLedger.read_events(ledger_path())

    directory_path = Path.dirname(ledger_path())
    File.mkdir_p!(directory_path)
    assert {:error, _reason} = RunLedger.read_events(directory_path)

    locked_directory = Path.join(System.tmp_dir!(), "symphony-ledger-locked")
    File.mkdir_p!(locked_directory)
    File.chmod!(locked_directory, 0o500)

    on_exit(fn ->
      File.chmod(locked_directory, 0o700)
      File.rm_rf(locked_directory)
    end)

    assert {:error, _reason} =
             RunLedger.append(Path.join(locked_directory, "events.jsonl"), %{
               transition: "runner_started"
             })
  end

  test "startup reconciliation stops when an interrupted event cannot be appended" do
    path = ledger_path()

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_started",
               run_id: "run-stale",
               issue_id: "issue-stale",
               attempt: 1
             })

    append_fn = fn _path, _event -> {:error, :forced_write_failure} end

    assert {:error, :forced_write_failure} =
             RunLedger.reconcile_startup(path, "runner-new", append_fn: append_fn)
  end

  defp ledger_path do
    Path.join(
      System.tmp_dir!(),
      "symphony-run-ledger-#{RunLedger.new_id("test")}/events.jsonl"
    )
  end
end
