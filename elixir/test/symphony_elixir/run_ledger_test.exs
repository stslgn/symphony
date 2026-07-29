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
               error_summary: "provider response must not persist",
               raw_prompt: "must not persist"
             })

    assert {:ok, [event]} = RunLedger.read_events(path)
    assert event["transition"] == "run_started"
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

    assert {:ok, %{"issue-stale" => 3}} =
             RunLedger.reconcile_startup(path, "runner-new")

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

    assert {:ok, %{}} = RunLedger.reconcile_startup(path, "runner-next")
  end

  test "ignores malformed trailing records during recovery" do
    path = ledger_path()
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, "{\"transition\":\"run_started\"\n")

    assert {:ok, %{}} = RunLedger.reconcile_startup(path, "runner-new")
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

    assert {:ok, %{"issue-malformed-attempt" => 1}} =
             RunLedger.reconcile_startup(path, "runner-new")
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
      "symphony-run-ledger-#{System.unique_integer([:positive])}/events.jsonl"
    )
  end
end
