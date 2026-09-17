defmodule SymphonyElixir.OperatorCursorApply do
  @moduledoc """
  Explicit offline cursor application for an exclusively stopped managed project.
  Not invoked by startup or the HTTP API. The caller must obtain owner approval
  for the exact request digest; possession of a digest is not authentication.

  macOS managed controllers share an exclusive directory lock. All writers and
  starts must obey that protocol; it is not protection against privileged or
  malicious same-user writers bypassing it. Any failure after acquisition retains
  the lock for explicit recovery. No stale-lock reclamation or runner start occurs.
  """

  alias SymphonyElixir.{OperatorCursorMigration, PathSafety, RunLedger}

  @spec request(map(), Path.t()) :: {:ok, map()} | {:error, term()}
  def request(%{path: path} = plan, workflow) when is_binary(path) and is_binary(workflow) do
    state_dir = path |> Path.dirname() |> Path.dirname() |> Path.dirname()

    with true <- path == Path.join(state_dir, "logs/log/run-ledger.jsonl"),
         {:ok, _} <- OperatorCursorMigration.remaining(path, plan),
         {:ok, context} <- context_identity(path, state_dir, Path.expand(workflow)),
         {:ok, bytes} <- File.read(workflow) do
      request = %{plan: plan, state_dir: state_dir, workflow: Path.expand(workflow), workflow_sha256: digest(bytes), context: context}
      {:ok, Map.put(request, :sha256, seal(request))}
    else
      {:error, _} = error -> error
      _ -> {:error, :invalid_layout}
    end
  end

  def request(_, _), do: {:error, :invalid_request}

  @spec apply(map(), String.t()) :: {:ok, map()} | {:error, term()}
  def apply(%{sha256: approved, plan: plan, state_dir: dir} = request, approved) do
    with true <- approved == seal(Map.delete(request, :sha256)),
         :ok <- unchanged_context(request),
         {:ok, events} <- OperatorCursorMigration.remaining(plan.path, plan),
         {:ok, lock} <- acquire_lock(dir, approved) do
      apply_locked(request, events, lock)
    else
      {:error, _} = error -> error
      _ -> {:error, :approval_mismatch}
    end
  end

  def apply(_, _), do: {:error, :approval_mismatch}

  defp acquire_lock(dir, approved) do
    path = Path.join(dir, "start-controller.lock")

    with {started, 0} <- System.cmd("/bin/ps", ["-p", System.pid(), "-o", "lstart="], env: [{"TZ", "UTC"}, {"LC_ALL", "C"}]),
         true <- String.trim(started) != "",
         :ok <- File.mkdir(path),
         :ok <- File.chmod(path, 0o700) do
      owner = "pid=#{System.pid()}\nprocess_start=#{String.trim(started)}\nplan_sha256=#{approved}\n"

      with :ok <- File.write(Path.join(path, "owner"), "", [:exclusive, :sync]),
           {:ok, owner_stat} <- File.lstat(Path.join(path, "owner")),
           :ok <- File.chmod(Path.join(path, "owner"), 0o600),
           {:ok, stat} <- File.lstat(path),
           :ok <- File.write(Path.join(path, "owner"), owner, [:sync]) do
        {:ok, %{path: path, owner: owner, inode: stat.inode, owner_inode: owner_stat.inode, uid: stat.uid}}
      end
    else
      _ -> {:error, :controller_lock_unavailable}
    end
  end

  defp apply_locked(request, events, lock) do
    with :ok <- stopped(request),
         :ok <- append_events(request, events, lock),
         :ok <- unchanged_context(request),
         :ok <- stopped(request),
         {:ok, []} <- OperatorCursorMigration.remaining(request.plan.path, request.plan),
         :ok <- owned_lock(lock),
         :ok <- File.rm(Path.join(lock.path, "owner")),
         :ok <- File.rmdir(lock.path) do
      {:ok, %{appended: length(events)}}
    else
      error -> {:error, {:workspace_preservation_required, error, lock.path}}
    end
  end

  defp append_events(request, events, lock) do
    Enum.reduce_while(events, :ok, fn event, :ok ->
      case append_when_stopped(request, event, lock) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp owned_lock(lock) do
    with {:ok, %File.Stat{type: :directory, inode: inode, uid: uid, mode: mode}} <- File.lstat(lock.path),
         true <- inode == lock.inode and uid == lock.uid and Bitwise.band(mode, 0o777) == 0o700,
         {:ok, %File.Stat{type: :regular, uid: owner_uid, mode: owner_mode, links: 1, inode: owner_inode}} <- File.lstat(Path.join(lock.path, "owner")),
         true <- owner_inode == lock.owner_inode and owner_uid == uid and Bitwise.band(owner_mode, 0o777) == 0o600,
         {:ok, owner} <- File.read(Path.join(lock.path, "owner")),
         true <- owner == lock.owner do
      :ok
    else
      _ -> {:error, :controller_lock_changed}
    end
  end

  defp append_when_stopped(request, event, lock) do
    with :ok <- owned_lock(lock),
         :ok <- unchanged_context(request),
         :ok <- stopped(request),
         {:ok, [^event | _]} <- OperatorCursorMigration.remaining(request.plan.path, request.plan) do
      RunLedger.append(request.plan.path, event)
    else
      failure -> {:error, {:apply_guard_failed, failure}}
    end
  end

  defp unchanged_context(request) do
    with {:ok, context} <- context_identity(request.plan.path, request.state_dir, request.workflow),
         true <- context == request.context,
         {:ok, bytes} <- File.read(request.workflow),
         true <- digest(bytes) == request.workflow_sha256 do
      :ok
    else
      _ -> {:error, :context_changed}
    end
  end

  defp context_identity(path, dir, workflow) do
    with {uid_text, 0} <- System.cmd("/usr/bin/id", ["-u"]),
         {uid, ""} <- Integer.parse(String.trim(uid_text)),
         {:ok, ^dir} <- PathSafety.canonicalize(dir),
         {:ok, ^workflow} <- PathSafety.canonicalize(workflow),
         {:ok, root} <- private_identity(dir, :directory, 0o700, uid),
         {:ok, logs} <- private_identity(Path.join(dir, "logs"), :directory, 0o700, uid),
         {:ok, log} <- private_identity(Path.join(dir, "logs/log"), :directory, 0o700, uid),
         {:ok, ledger} <- private_identity(path, :regular, 0o600, uid),
         {:ok, flow} <- private_identity(workflow, :regular, nil, uid) do
      {:ok, [root, logs, log, ledger, flow]}
    else
      _ -> {:error, :unsafe_context}
    end
  end

  defp private_identity(path, type, mode, uid) do
    with {:ok, %File.Stat{type: ^type, uid: ^uid} = stat} <- File.lstat(path),
         true <- (is_nil(mode) and Bitwise.band(stat.mode, 0o022) == 0) or Bitwise.band(stat.mode, 0o777) == mode,
         true <- type == :directory or stat.links == 1 do
      {:ok, {stat.major_device, stat.minor_device, stat.inode, stat.uid, stat.mode}}
    else
      _ -> {:error, :unsafe_path}
    end
  end

  defp stopped(request) do
    with {output, 0} <- System.cmd("/bin/ps", ["-axww", "-o", "pid=,command="]),
         true <- Regex.match?(~r/(?:^|\n)\s*#{System.pid()}\s/, output),
         {:ok, pid} <- saved_pid(request.state_dir, elem(hd(request.context), 3)) do
      busy =
        Enum.any?(String.split(output, "\n", trim: true), fn line ->
          [current_pid, command] = String.split(String.trim(line), ~r/\s+/, parts: 2)

          current_pid == pid or String.contains?(command, request.state_dir <> "/runtime-image") or
            (String.contains?(command, request.workflow) and String.contains?(command, ["bin/symphony", "symphony-linear"]))
        end)

      if busy, do: {:error, :runner_not_stopped}, else: :ok
    else
      _ -> {:error, :process_snapshot_unavailable}
    end
  end

  defp saved_pid(dir, uid) do
    path = Path.join(dir, "runner.pid")

    case File.lstat(path) do
      {:error, :enoent} ->
        {:ok, nil}

      _ ->
        with {:ok, _} <- private_identity(path, :regular, 0o600, uid),
             {:ok, contents} <- File.read(path),
             pid = String.trim(contents),
             true <- Regex.match?(~r/^[1-9][0-9]*$/, pid) do
          {:ok, pid}
        else
          _ -> {:error, :invalid_pid_file}
        end
    end
  end

  defp seal(value), do: value |> :erlang.term_to_binary([:deterministic]) |> digest()
  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
