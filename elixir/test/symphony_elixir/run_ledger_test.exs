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

  test "fails startup closed when a parked tail record is corrupted" do
    path = ledger_path()

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_started",
               run_id: "run-parked-tail",
               issue_id: "issue-parked-tail",
               attempt: 2
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_parked",
               run_id: "run-parked-tail",
               issue_id: "issue-parked-tail",
               attempt: 2,
               wait_id: "wait-parked-tail",
               parked_reason: "operator_wait"
             })

    corrupt_last_record!(path)

    assert {:error, {:invalid_ledger_record, 2, :malformed_json}} =
             RunLedger.reconcile_startup(path, "runner-new")
  end

  test "fails startup closed when a terminal tail record is corrupted" do
    path = ledger_path()

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_started",
               run_id: "run-terminal-tail",
               issue_id: "issue-terminal-tail",
               attempt: 1
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_completed",
               run_id: "run-terminal-tail",
               issue_id: "issue-terminal-tail",
               attempt: 1
             })

    corrupt_last_record!(path)

    assert {:error, {:invalid_ledger_record, 2, :malformed_json}} =
             RunLedger.reconcile_startup(path, "runner-new")
  end

  test "fails startup closed on corruption in the middle of the ledger" do
    path = ledger_path()

    assert :ok = RunLedger.append(path, %{transition: "dispatch_paused"})
    File.write!(path, "not-json\n", [:append])
    assert :ok = RunLedger.append(path, %{transition: "dispatch_resumed"})

    assert {:error, {:invalid_ledger_record, 2, :malformed_json}} =
             RunLedger.reconcile_startup(path, "runner-new")
  end

  test "rejects non-object and semantically invalid records" do
    non_object_path = ledger_path()
    File.mkdir_p!(Path.dirname(non_object_path))
    File.write!(non_object_path, Jason.encode!([]) <> "\n")

    assert {:error, {:invalid_ledger_record, 1, :not_an_object}} =
             RunLedger.read_events(non_object_path)

    invalid_attempt_path = ledger_path()

    assert :ok =
             RunLedger.append(invalid_attempt_path, %{
               transition: "run_started",
               run_id: "run-invalid-attempt",
               issue_id: "issue-invalid-attempt",
               attempt: 1
             })

    [event] = valid_records(invalid_attempt_path)
    File.write!(invalid_attempt_path, Jason.encode!(%{event | "attempt" => "not-an-integer"}) <> "\n")

    assert {:error, {:invalid_ledger_record, 1, {:invalid_field, "attempt"}}} =
             RunLedger.reconcile_startup(invalid_attempt_path, "runner-new")
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

  defp corrupt_last_record!(path) do
    records = valid_records(path)
    last = records |> List.last() |> Jason.encode!()
    truncated = binary_part(last, 0, byte_size(last) - 1)

    contents =
      records
      |> Enum.drop(-1)
      |> Enum.map_join("\n", &Jason.encode!/1)

    prefix = if contents == "", do: "", else: contents <> "\n"
    File.write!(path, prefix <> truncated <> "\n")
  end

  defp valid_records(path) do
    path
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.map(&Jason.decode!/1)
  end
end
