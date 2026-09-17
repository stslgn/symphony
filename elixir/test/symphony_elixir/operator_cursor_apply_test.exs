defmodule SymphonyElixir.OperatorCursorApplyTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{OperatorCursorApply, OperatorCursorMigration, PathSafety, RunLedger}

  test "applies the exact approved batch once and preserves the original prefix" do
    {path, workflow, plan} = fixture()
    before = File.read!(path)
    assert {:ok, request} = OperatorCursorApply.request(plan, workflow)
    assert {:ok, %{appended: 1}} = OperatorCursorApply.apply(request, request.sha256)
    assert String.starts_with?(File.read!(path), before)
    assert {:ok, []} = OperatorCursorMigration.remaining(path, plan)
    assert {:ok, %{appended: 0}} = OperatorCursorApply.apply(request, request.sha256)
    refute File.exists?(Path.join(request.state_dir, "start-controller.lock"))
  end

  test "does not steal an existing controller lock even if its owner is dead" do
    {path, workflow, plan} = fixture()
    {:ok, request} = OperatorCursorApply.request(plan, workflow)
    lock = Path.join(request.state_dir, "start-controller.lock")
    File.mkdir!(lock)
    owner = "pid=999999999\nprocess_start=stale\n"
    File.write!(Path.join(lock, "owner"), owner)
    before = File.read!(path)
    assert {:error, _} = OperatorCursorApply.apply(request, request.sha256)
    assert File.read!(path) == before
    assert File.read!(Path.join(lock, "owner")) == owner
  end

  test "refuses a live PID even if no active agent is reported" do
    {path, workflow, plan} = fixture()
    {:ok, request} = OperatorCursorApply.request(plan, workflow)
    File.write!(Path.join(request.state_dir, "runner.pid"), System.pid() <> "\n")
    File.chmod!(Path.join(request.state_dir, "runner.pid"), 0o600)
    before = File.read!(path)
    assert {:error, _} = OperatorCursorApply.apply(request, request.sha256)
    assert File.read!(path) == before
    lock = Path.join(request.state_dir, "start-controller.lock")
    assert File.dir?(lock)
    assert File.read!(Path.join(lock, "owner")) =~ "pid=#{System.pid()}\n"
    assert Bitwise.band(File.stat!(lock).mode, 0o777) == 0o700
    assert Bitwise.band(File.stat!(Path.join(lock, "owner")).mode, 0o777) == 0o600
  end

  test "changed workflow or unsafe state permissions cannot authorize writes" do
    for mutation <- [:workflow, :permissions] do
      {path, workflow, plan} = fixture()
      {:ok, request} = OperatorCursorApply.request(plan, workflow)
      if mutation == :workflow, do: File.write!(workflow, "changed\n"), else: File.chmod!(request.state_dir, 0o777)
      before = File.read!(path)
      assert {:error, _} = OperatorCursorApply.apply(request, request.sha256)
      assert File.read!(path) == before
    end
  end

  test "approval mismatch and historical or malformed append never change ledger bytes" do
    {path, workflow, plan} = fixture()
    {:ok, request} = OperatorCursorApply.request(plan, workflow)
    before = File.read!(path)
    assert {:error, _} = OperatorCursorApply.apply(request, "not-approved")
    assert {:error, _} = OperatorCursorApply.apply(%{request | workflow: "changed"}, request.sha256)
    assert File.read!(path) == before
    File.write!(path, "{", [:append])
    assert {:error, _} = OperatorCursorApply.apply(request, request.sha256)
    assert File.read!(path) == before <> "{"
    refute File.exists?(Path.join(request.state_dir, "start-controller.lock"))
  end

  test "PID-file symlink is never followed" do
    {path, workflow, plan} = fixture()
    {:ok, request} = OperatorCursorApply.request(plan, workflow)
    target = Path.join(request.state_dir, "not-a-pid-file")
    File.write!(target, "999999999\n")
    File.ln_s!(target, Path.join(request.state_dir, "runner.pid"))
    before = File.read!(path)
    assert {:error, _} = OperatorCursorApply.apply(request, request.sha256)
    assert File.read!(path) == before
  end

  test "a lock-owner swap during application aborts without removing the replacement" do
    {path, workflow, plan} = fixture(12)
    {:ok, request} = OperatorCursorApply.request(plan, workflow)
    before = File.read!(path)
    task = Task.async(fn -> OperatorCursorApply.apply(request, request.sha256) end)
    owner = Path.join(request.state_dir, "start-controller.lock/owner")
    await_file(owner, 2000)
    File.write!(owner, "replacement-owner\n")
    assert {:error, _} = Task.await(task, 10_000)
    assert File.read!(owner) == "replacement-owner\n"
    assert String.starts_with?(File.read!(path), before)
  end

  test "a crashed apply task leaves a start-blocking lock and never reclaims it automatically" do
    {path, workflow, plan} = fixture(12)
    {:ok, request} = OperatorCursorApply.request(plan, workflow)
    task = Task.async(fn -> OperatorCursorApply.apply(request, request.sha256) end)
    lock = Path.join(request.state_dir, "start-controller.lock")
    await_file(Path.join(lock, "owner"), 2000)
    assert Task.shutdown(task, :brutal_kill) == nil
    refute Process.alive?(task.pid)
    assert File.dir?(lock)
    before = File.read!(path)
    assert {:error, _} = OperatorCursorApply.apply(request, request.sha256)
    assert File.read!(path) == before
    assert File.dir?(lock)
  end

  test "a matching live runtime image blocks writes even with no PID file" do
    {path, workflow, plan} = fixture()
    {:ok, request} = OperatorCursorApply.request(plan, workflow)
    image = Path.join(request.state_dir, "runtime-image.fixture")
    File.cp!("/bin/sleep", image)
    port = Port.open({:spawn_executable, String.to_charlist(image)}, [{:args, [~c"3"]}, :exit_status])
    before = File.read!(path)

    try do
      assert {:error, _} = OperatorCursorApply.apply(request, request.sha256)
      assert File.read!(path) == before
    after
      if Port.info(port), do: Port.close(port)
    end
  end

  test "resumes a whole-event partial batch under a new lock without duplicating cursors" do
    {path, workflow, plan} = fixture(4)
    {:ok, request} = OperatorCursorApply.request(plan, workflow)
    :ok = RunLedger.append(path, hd(plan.events))
    assert {:ok, %{appended: 3}} = OperatorCursorApply.apply(request, request.sha256)
    assert {:ok, []} = OperatorCursorMigration.remaining(path, plan)
  end

  test "invalid layout, missing workflow, empty approval and malformed PID fail closed" do
    {path, workflow, plan} = fixture()
    assert {:error, _} = OperatorCursorApply.request(%{}, workflow)
    assert {:error, _} = OperatorCursorApply.request(%{plan | path: Path.join(Path.dirname(path), "other")}, workflow)
    assert {:error, _} = OperatorCursorApply.request(plan, workflow <> ".missing")
    assert {:error, _} = OperatorCursorApply.apply(%{}, "")
    {:ok, request} = OperatorCursorApply.request(plan, workflow)
    pid = Path.join(request.state_dir, "runner.pid")
    File.write!(pid, "unknown\n")
    File.chmod!(pid, 0o600)
    before = File.read!(path)
    assert {:error, _} = OperatorCursorApply.apply(request, request.sha256)
    assert File.read!(path) == before
  end

  test "an actual append failure retains the lock and every prior byte" do
    {path, workflow, plan} = fixture()
    {:ok, request} = OperatorCursorApply.request(plan, workflow)
    before = File.read!(path)
    assert_append_failure(request, :os.type())
    assert File.read!(path) == before
    lock = Path.join(request.state_dir, "start-controller.lock")
    assert File.dir?(lock)
    assert File.read!(Path.join(lock, "owner")) =~ "plan_sha256=#{request.sha256}\n"
    assert {:ok, [_]} = OperatorCursorMigration.remaining(path, plan)
  end

  test "replacing the lock owner inode with identical content still stops the writer" do
    {path, workflow, plan} = fixture(12)
    {:ok, request} = OperatorCursorApply.request(plan, workflow)
    task = Task.async(fn -> OperatorCursorApply.apply(request, request.sha256) end)
    owner = Path.join(request.state_dir, "start-controller.lock/owner")
    await_file(owner, 2000)
    bytes = File.read!(owner)
    File.rename!(owner, owner <> ".preserved")
    File.write!(owner, bytes)
    File.chmod!(owner, 0o600)
    assert {:error, _} = Task.await(task, 10_000)
    assert File.read!(owner) == bytes
    assert File.read!(owner <> ".preserved") == bytes
    assert {:ok, _} = OperatorCursorMigration.remaining(path, plan)
  end

  # macOS does not enforce RLIMIT_FSIZE on this append path. Both platforms
  # exercise a real filesystem failure, without changing the required 0600 mode.
  defp assert_append_failure(request, {:unix, :darwin}) do
    path = request.plan.path
    assert {_, 0} = System.cmd("/usr/bin/chflags", ["uchg", path])

    try do
      assert {:error, {:workspace_preservation_required, {:error, :eperm}, _}} =
               OperatorCursorApply.apply(request, request.sha256)
    after
      assert {_, 0} = System.cmd("/usr/bin/chflags", ["nouchg", path])
    end
  end

  defp assert_append_failure(request, {:unix, :linux}) do
    before = File.read!(request.plan.path)
    assert byte_size(before) > 1024
    request_path = Path.join(request.state_dir, "synthetic-request.etf")
    File.write!(request_path, :erlang.term_to_binary(request))
    File.chmod!(request_path, 0o600)

    # Limit only the child: the existing ledger exceeds either shell block size
    # (512/1024 bytes), while the newly created lock owner fits below the limit.
    # Ignoring SIGXFSZ lets the real write return EFBIG instead of killing the VM.
    code = """
    request = System.argv() |> hd() |> File.read!() |> :erlang.binary_to_term()
    {:error, {:workspace_preservation_required, {:error, :efbig}, lock}} =
      SymphonyElixir.OperatorCursorApply.apply(request, request.sha256)
    true = lock == Path.join(request.state_dir, "start-controller.lock")
    IO.puts("append_failed_with_efbig")
    """

    assert {"append_failed_with_efbig\n", 0} =
             System.cmd("/bin/sh", [
               "-c",
               "trap '' XFSZ; ulimit -f 1 || exit 1; exec \"$@\"",
               "cursor-append-limit",
               System.find_executable("elixir"),
               "--erl",
               "+S 2:2",
               "-pa",
               Path.expand("_build/test/lib/*/ebin"),
               "-e",
               code,
               "--",
               request_path
             ])
  end

  defp await_file(_, 0), do: flunk("controller lock was not published")

  defp await_file(path, remaining) do
    ready =
      case File.read(path) do
        {:ok, bytes} -> String.contains?(bytes, "plan_sha256=") and Bitwise.band(File.stat!(path).mode, 0o777) == 0o600
        _ -> false
      end

    if ready do
      :ok
    else
      Process.sleep(1)
      await_file(path, remaining - 1)
    end
  end

  defp fixture(count \\ 1) do
    root = Path.join(System.tmp_dir!(), "cursor-apply-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(root, "logs/log"))
    {:ok, root} = PathSafety.canonicalize(root)
    for dir <- [root, Path.join(root, "logs"), Path.join(root, "logs/log")], do: File.chmod!(dir, 0o700)
    on_exit(fn -> File.rm_rf!(root) end)
    path = Path.join(root, "logs/log/run-ledger.jsonl")
    workflow = Path.join(root, "WORKFLOW.md")
    File.write!(workflow, "synthetic workflow\n")

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

    :ok = RunLedger.append(path, %{transition: "runner_started", stage: "startup", runner_generation: "fixture"})

    for n <- 1..count do
      :ok =
        RunLedger.append(path, %{transition: "operator_cursor_initialized", stage: "operator", runner_generation: "fixture", issue_id: "issue-#{n}", comment_created_at: "2026-09-17T04:00:00.000Z"})
    end

    sha = :crypto.hash(:sha256, File.read!(path)) |> Base.encode16(case: :lower)
    {:ok, plan} = OperatorCursorMigration.prepare(path, %{sha256: sha, runner_generation: "fixture", issue_ids: Enum.map(1..count, &"issue-#{&1}"), boundary: "2026-09-17T05:00:00.000Z"})
    {path, workflow, plan}
  end
end
