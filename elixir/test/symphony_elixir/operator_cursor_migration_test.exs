defmodule SymphonyElixir.OperatorCursorMigrationTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{OperatorCursorMigration, RunLedger}

  @boundary "2026-09-17T05:00:00.000Z"

  test "refuses mismatched approved snapshot identities and non-forward boundaries" do
    {path, expected} = fixture()
    before = File.read!(path)

    for changed <- [
          %{sha256: String.duplicate("0", 64)},
          %{runner_generation: "wrong-generation"},
          %{issue_ids: ["wrong-issue"]},
          %{issue_ids: ["issue-1", "issue-1"]},
          %{boundary: "2026-09-17T03:00:00.000Z"},
          %{boundary: "not-a-time"}
        ] do
      assert {:error, _} = OperatorCursorMigration.prepare(path, Map.merge(expected, changed))
      assert File.read!(path) == before
    end
  end

  test "prepares bounded cursor evidence without changing any ledger bytes" do
    {path, expected} = fixture()
    before = File.read!(path)
    assert {:ok, plan} = OperatorCursorMigration.prepare(path, expected)
    assert File.read!(path) == before
    assert {:ok, [event]} = OperatorCursorMigration.remaining(path, plan)
    assert event.transition == "operator_cursor_initialized"
    assert event.issue_id == "issue-1"
    assert event.comment_created_at == @boundary
    refute Map.has_key?(event, :comment_id)
    refute Map.has_key?(event, :operator_command)
    assert plan.recovery.recovered_dispatches["issue-1"].attempt == 2
  end

  test "resumes an exact partial batch and becomes a no-op after all four boundaries" do
    {path, expected} = fixture(4)
    before = File.read!(path)
    assert {:ok, plan} = OperatorCursorMigration.prepare(path, expected)

    for n <- 0..3 do
      assert {:ok, remaining} = OperatorCursorMigration.remaining(path, plan)
      assert length(remaining) == 4 - n
      assert :ok = RunLedger.append(path, hd(remaining))
    end

    assert {:ok, []} = OperatorCursorMigration.remaining(path, plan)
    assert {:ok, []} = OperatorCursorMigration.remaining(path, plan)
    assert String.starts_with?(File.read!(path), before)
    {:ok, after_recovery} = RunLedger.reconcile_startup(path, "preview", append_fn: fn _, _ -> :ok end)
    assert Map.delete(after_recovery, :operator_comment_cursors) == Map.delete(plan.recovery, :operator_comment_cursors)
    assert Enum.all?(after_recovery.operator_comment_cursors, fn {_, cursor} -> cursor.created_at == @boundary end)
  end

  test "rejects modified plans and paths even when another file has identical bytes" do
    {path, expected} = fixture()
    {:ok, plan} = OperatorCursorMigration.prepare(path, expected)
    changed = %{plan | events: [%{hd(plan.events) | issue_id: "wrong-issue"}]}
    assert {:error, _} = OperatorCursorMigration.remaining(path, changed)
    duplicate = path <> ".duplicate"
    File.cp!(path, duplicate)
    on_exit(fn -> File.rm!(duplicate) end)
    assert {:error, _} = OperatorCursorMigration.remaining(duplicate, plan)
  end

  test "refuses partial bytes, extra blank lines, unrelated appends and repeated migration events" do
    for mutation <- [:partial, :blank, :unrelated, :duplicate, :prefix] do
      {path, expected} = fixture()
      {:ok, plan} = OperatorCursorMigration.prepare(path, expected)

      case mutation do
        :partial ->
          File.write!(path, "{\"transition\":", [:append])

        :blank ->
          File.write!(path, "\n", [:append])

        :unrelated ->
          RunLedger.append(path, %{transition: "runner_started", stage: "startup", runner_generation: "other-runner"})

        :duplicate ->
          RunLedger.append(path, hd(plan.events))
          RunLedger.append(path, hd(plan.events))

        :prefix ->
          File.write!(path, String.replace(File.read!(path), "TEST-1", "TEST-X"))
      end

      before = File.read!(path)
      assert {:error, _} = OperatorCursorMigration.remaining(path, plan)
      assert File.read!(path) == before
    end
  end

  test "fails closed on invalid input and a symlink substituted for the approved file" do
    {path, expected} = fixture()
    {:ok, plan} = OperatorCursorMigration.prepare(path, expected)
    assert {:error, _} = OperatorCursorMigration.prepare(path, %{})
    assert {:error, _} = OperatorCursorMigration.remaining(path, %{})
    moved = path <> ".original"
    File.rename!(path, moved)
    File.ln_s!(moved, path)
    on_exit(fn -> File.rm!(moved) end)
    assert {:error, _} = OperatorCursorMigration.remaining(path, plan)
    assert {:error, _} = OperatorCursorMigration.prepare(path, expected)
  end

  test "rejects unfinished runs instead of silently projecting a retry" do
    {path, expected} = fixture()
    :ok = RunLedger.append(path, %{transition: "run_claimed", stage: "claimed", run_id: "unfinished", issue_id: "issue-unfinished", issue_identifier: "TEST-UNFINISHED", attempt: 1})
    before = File.read!(path)
    expected = %{expected | sha256: digest(before)}
    assert {:error, :unfinished_run} = OperatorCursorMigration.prepare(path, expected)
    assert File.read!(path) == before
  end

  test "preserves a different Human Review wait alongside all recovered queues" do
    {path, expected} = fixture(4)
    base = %{run_id: "run-parked", issue_id: "issue-parked", issue_identifier: "TEST-PARKED", attempt: 3}
    :ok = RunLedger.append(path, Map.merge(base, %{transition: "run_claimed", stage: "claimed"}))
    :ok = RunLedger.append(path, Map.merge(base, %{transition: "run_started", stage: "running"}))

    :ok =
      RunLedger.append(
        path,
        Map.merge(base, %{
          transition: "run_parked",
          stage: "parked",
          wait_id: "wait-human-review",
          parked_reason: "waiting_owner",
          allowed_actions: ["approve", "reject"],
          tracker_state: "Human Review"
        })
      )

    expected = %{expected | sha256: digest(File.read!(path))}
    {:ok, plan} = OperatorCursorMigration.prepare(path, expected)
    assert plan.recovery.parked["issue-parked"]["wait_id"] == "wait-human-review"
    {:ok, events} = OperatorCursorMigration.remaining(path, plan)
    Enum.each(events, &RunLedger.append(path, &1))
    {:ok, recovery} = RunLedger.reconcile_startup(path, "preview", append_fn: fn _, _ -> :ok end)
    assert Map.delete(recovery, :operator_comment_cursors) == Map.delete(plan.recovery, :operator_comment_cursors)
  end

  test "missing, empty, invalid and cursorless ledgers never produce a plan" do
    {path, expected} = fixture()
    assert {:error, :enoent} = OperatorCursorMigration.prepare(path <> ".missing", expected)

    for contents <- ["", "{", "{}\n"] do
      File.write!(path, contents)
      assert {:error, _} = OperatorCursorMigration.prepare(path, %{expected | sha256: digest(contents)})
    end

    {other, other_expected} = fixture()
    contents = other |> File.read!() |> String.split("\n", trim: true) |> Enum.reject(&String.contains?(&1, "operator_cursor_initialized")) |> Enum.join("\n")
    File.write!(other, contents <> "\n")
    assert {:error, _} = OperatorCursorMigration.prepare(other, %{other_expected | sha256: digest(File.read!(other))})
  end

  defp fixture(count \\ 1) do
    path = Path.join(System.tmp_dir!(), "cursor-migration-#{System.unique_integer([:positive])}.jsonl")
    on_exit(fn -> File.rm!(path) end)
    ids = Enum.map(1..count, &"issue-#{&1}")

    for n <- 1..count do
      base = %{run_id: "run-#{n}", issue_id: "issue-#{n}", issue_identifier: "TEST-#{n}", attempt: 1}

      for event <- [
            %{transition: "run_claimed", stage: "claimed"},
            %{transition: "run_started", stage: "running"},
            %{transition: "run_failed", stage: "released", terminal_reason: "worker_exit", next_action: "retry", next_attempt: 2},
            %{transition: "retry_scheduled", stage: "retry_queued", next_attempt: 2}
          ] do
        :ok = RunLedger.append(path, Map.merge(base, event))
      end
    end

    :ok = RunLedger.append(path, %{transition: "runner_started", stage: "startup", runner_generation: "runner-fixture"})

    for id <- ids do
      :ok = RunLedger.append(path, %{transition: "operator_cursor_initialized", stage: "operator", runner_generation: "runner-fixture", issue_id: id, comment_created_at: "2026-09-17T04:00:00.000Z"})
    end

    {path, %{sha256: digest(File.read!(path)), runner_generation: "runner-fixture", issue_ids: ids, boundary: @boundary}}
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
