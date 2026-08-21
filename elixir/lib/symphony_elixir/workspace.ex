defmodule SymphonyElixir.Workspace do
  @moduledoc """
  Creates isolated per-issue workspaces for parallel Codex agents.
  """

  require Logger
  alias SymphonyElixir.{Config, PathSafety, SSH}

  @allow_test_local_durability_remotes Application.compile_env(
                                         :symphony_elixir,
                                         :allow_test_local_durability_remotes,
                                         false
                                       )

  @remote_workspace_marker "__SYMPHONY_WORKSPACE__"
  @remote_affinity_marker "__SYMPHONY_AFFINITY__"
  @remote_durability_marker "__SYMPHONY_DURABILITY__"

  @type worker_host :: String.t() | nil
  @type prepared_workspace :: %{
          path: Path.t(),
          root: Path.t(),
          created?: boolean()
        }

  @spec prepare_for_issue(map() | String.t() | nil, worker_host()) ::
          {:ok, prepared_workspace()} | {:error, term()}
  def prepare_for_issue(issue_or_identifier, worker_host \\ nil),
    do: prepare_for_issue(issue_or_identifier, worker_host, [])

  @spec prepare_for_issue(map() | String.t() | nil, worker_host(), keyword()) ::
          {:ok, prepared_workspace()} | {:error, term()}
  def prepare_for_issue(issue_or_identifier, worker_host, opts) when is_list(opts) do
    issue_context = issue_context(issue_or_identifier)

    try do
      safe_id = safe_identifier(issue_context.issue_identifier)

      with {:ok, target} <- prepare_target(safe_id, worker_host, opts),
           {:ok, workspace, created?} <- ensure_workspace(target.path, worker_host),
           :ok <- validate_ensured_workspace(workspace, target, worker_host) do
        {:ok, %{path: workspace, root: target.root, created?: created?}}
      end
    rescue
      error in [ArgumentError, ErlangError, File.Error] ->
        Logger.error("Workspace preparation failed #{issue_log_context(issue_context)} worker_host=#{worker_host_for_log(worker_host)} error=#{Exception.message(error)}")

        {:error, error}
    end
  end

  @spec create_for_issue(map() | String.t() | nil, worker_host()) ::
          {:ok, Path.t()} | {:error, term()}
  def create_for_issue(issue_or_identifier, worker_host \\ nil),
    do: create_for_issue(issue_or_identifier, worker_host, [])

  @spec create_for_issue(map() | String.t() | nil, worker_host(), keyword()) ::
          {:ok, Path.t()} | {:error, term()}
  def create_for_issue(issue_or_identifier, worker_host, opts) when is_list(opts) do
    issue_context = issue_context(issue_or_identifier)

    with {:ok, prepared} <- prepare_for_issue(issue_or_identifier, worker_host, opts),
         :ok <- run_after_create_hook(prepared.path, issue_context, prepared.created?, worker_host) do
      {:ok, prepared.path}
    end
  end

  @spec validate_prepared_workspace(prepared_workspace(), worker_host()) ::
          {:ok, prepared_workspace()} | {:error, term()}
  def validate_prepared_workspace(
        %{path: workspace, root: root, created?: created?} = prepared,
        worker_host
      )
      when is_binary(workspace) and is_binary(root) and is_boolean(created?) do
    with :ok <- validate_path_against_root(workspace, root, worker_host),
         :ok <- validate_prepared_workspace_exists(workspace, worker_host) do
      {:ok, prepared}
    end
  end

  def validate_prepared_workspace(_prepared, _worker_host),
    do: {:error, :invalid_prepared_workspace}

  @spec run_after_create_hook(Path.t(), map() | String.t() | nil, boolean(), worker_host()) ::
          :ok | {:error, term()}
  def run_after_create_hook(workspace, issue_or_identifier, created?, worker_host \\ nil)
      when is_binary(workspace) and is_boolean(created?) do
    issue_or_identifier
    |> issue_context()
    |> then(&maybe_run_after_create_hook(workspace, &1, created?, worker_host))
  end

  defp ensure_workspace(workspace, nil) do
    cond do
      File.dir?(workspace) ->
        {:ok, workspace, false}

      File.exists?(workspace) ->
        File.rm_rf!(workspace)
        create_workspace(workspace)

      true ->
        create_workspace(workspace)
    end
  end

  defp ensure_workspace(workspace, worker_host) when is_binary(worker_host) do
    script =
      [
        "set -eu",
        remote_shell_assign("workspace", workspace),
        "if [ -d \"$workspace\" ]; then",
        "  created=0",
        "elif [ -e \"$workspace\" ]; then",
        "  rm -rf \"$workspace\"",
        "  mkdir -p \"$workspace\"",
        "  created=1",
        "else",
        "  mkdir -p \"$workspace\"",
        "  created=1",
        "fi",
        "cd \"$workspace\"",
        "printf '%s\\t%s\\t%s\\n' '#{@remote_workspace_marker}' \"$created\" \"$(pwd -P)\""
      ]
      |> Enum.reject(&(&1 == ""))
      |> Enum.join("\n")

    case run_remote_command(worker_host, script, Config.settings!().hooks.timeout_ms) do
      {:ok, {output, 0}} ->
        parse_remote_workspace_output(output)

      {:ok, {output, status}} ->
        {:error, {:workspace_prepare_failed, worker_host, status, output}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_ensured_workspace(path, %{path: path}, _worker_host), do: :ok

  defp validate_ensured_workspace(path, %{path: expected_path}, worker_host) do
    {:error, {:workspace_affinity_mismatch, expected_path, path, worker_host}}
  end

  defp create_workspace(workspace) do
    File.rm_rf!(workspace)
    File.mkdir_p!(workspace)
    {:ok, workspace, true}
  end

  @spec remove(Path.t()) :: {:ok, [String.t()]} | {:error, term(), String.t()}
  def remove(workspace), do: remove(workspace, nil)

  @spec remove(Path.t(), worker_host()) :: {:ok, [String.t()]} | {:error, term(), String.t()}
  def remove(workspace, nil) do
    case File.exists?(workspace) do
      true ->
        case validate_workspace_path(workspace, nil) do
          :ok ->
            maybe_run_before_remove_hook(workspace, nil)
            File.rm_rf(workspace)

          {:error, reason} ->
            {:error, reason, ""}
        end

      false ->
        File.rm_rf(workspace)
    end
  end

  def remove(workspace, worker_host) when is_binary(worker_host) do
    maybe_run_before_remove_hook(workspace, worker_host)

    script =
      [
        remote_shell_assign("workspace", workspace),
        "rm -rf \"$workspace\""
      ]
      |> Enum.join("\n")

    case run_remote_command(worker_host, script, Config.settings!().hooks.timeout_ms) do
      {:ok, {_output, 0}} ->
        {:ok, []}

      {:ok, {output, status}} ->
        {:error, {:workspace_remove_failed, worker_host, status, output}, ""}

      {:error, reason} ->
        {:error, reason, ""}
    end
  end

  @spec remove_exact(Path.t(), Path.t(), worker_host()) ::
          {:ok, [String.t()]} | {:error, term(), String.t()}
  def remove_exact(workspace, captured_root, nil)
      when is_binary(workspace) and is_binary(captured_root) do
    case validate_path_against_root(workspace, captured_root, nil) do
      :ok ->
        maybe_run_before_remove_hook(workspace, nil)
        File.rm_rf(workspace)

      {:error, reason} ->
        {:error, reason, ""}
    end
  end

  def remove_exact(workspace, captured_root, worker_host)
      when is_binary(workspace) and is_binary(captured_root) and is_binary(worker_host) do
    with :ok <- validate_affinity_path(workspace, worker_host),
         :ok <- validate_affinity_path(captured_root, worker_host),
         {:ok, target} <- canonical_affinity_target(workspace, captured_root, worker_host),
         :ok <- validate_path_against_root(target.path, target.root, worker_host) do
      remove_exact_remote(target.path, worker_host)
    else
      {:error, reason} -> {:error, reason, ""}
    end
  end

  def remove_exact(workspace, captured_root, worker_host),
    do: {:error, {:invalid_exact_workspace, workspace, captured_root, worker_host}, ""}

  @spec remove_exact_if_durable(Path.t(), Path.t(), worker_host()) ::
          {:ok, [String.t()]} | {:error, term(), String.t()}
  def remove_exact_if_durable(workspace, captured_root, nil)
      when is_binary(workspace) and is_binary(captured_root) do
    with :ok <- validate_path_against_root(workspace, captured_root, nil),
         {:ok, remote_url} <- durability_remote_url(captured_root, nil),
         {:ok, quarantine} <- quarantine_local_workspace(workspace, captured_root) do
      cleanup_local_quarantine(quarantine, workspace, captured_root, remote_url)
    else
      {:error, reason} -> {:error, reason, ""}
    end
  end

  def remove_exact_if_durable(workspace, captured_root, worker_host)
      when is_binary(workspace) and is_binary(captured_root) and is_binary(worker_host) do
    with :ok <- validate_affinity_path(workspace, worker_host),
         :ok <- validate_affinity_path(captured_root, worker_host),
         {:ok, target} <- canonical_affinity_target(workspace, captured_root, worker_host),
         :ok <- validate_path_against_root(target.path, target.root, worker_host),
         {:ok, remote_url} <- durability_remote_url(target.root, worker_host) do
      remove_remote_quarantine_if_durable(
        target.path,
        target.root,
        remote_url,
        durability_boundary_path(remote_url),
        worker_host
      )
    else
      {:error, reason} -> {:error, reason, ""}
    end
  end

  def remove_exact_if_durable(workspace, captured_root, worker_host),
    do: {:error, {:invalid_exact_workspace, workspace, captured_root, worker_host}, ""}

  defp remove_exact_remote(workspace, worker_host) do
    maybe_run_before_remove_hook(workspace, worker_host)
    remove_exact_remote_after_hook(workspace, worker_host)
  end

  defp remove_exact_remote_after_hook(workspace, worker_host) do
    script =
      [
        remote_shell_assign("workspace", workspace),
        "rm -rf \"$workspace\""
      ]
      |> Enum.join("\n")

    case run_remote_command(worker_host, script, Config.settings!().hooks.timeout_ms) do
      {:ok, {_output, 0}} -> {:ok, []}
      {:ok, {output, status}} -> {:error, {:workspace_remove_failed, worker_host, status, output}, ""}
      {:error, reason} -> {:error, reason, ""}
    end
  end

  defp durability_remote_url(captured_root, worker_host) do
    case Config.settings!().workspace.durability_remote_url do
      value when is_binary(value) and value != "" ->
        with :ok <- validate_local_durability_boundary(value, captured_root, worker_host) do
          {:ok, value}
        end

      _value ->
        {:error, :workspace_preservation_required}
    end
  end

  defp validate_local_durability_boundary(remote_url, captured_root, nil) do
    case local_durability_path(remote_url) do
      {:ok, remote_path} ->
        validate_test_local_durability_boundary(remote_path, captured_root)

      :external ->
        :ok

      :invalid ->
        {:error, :workspace_preservation_required}
    end
  end

  defp validate_local_durability_boundary(_remote_url, _captured_root, worker_host)
       when is_binary(worker_host),
       do: :ok

  if @allow_test_local_durability_remotes do
    defp validate_test_local_durability_boundary(remote_path, captured_root) do
      with {:ok, canonical_root} <- PathSafety.canonicalize(captured_root),
           {:ok, canonical_remote} <- PathSafety.canonicalize(remote_path),
           false <- path_at_or_below?(canonical_remote, canonical_root) do
        :ok
      else
        _other -> {:error, :workspace_preservation_required}
      end
    end

    defp path_at_or_below?(path, root) do
      relative = Path.relative_to(path, root)
      path == root or (Path.type(relative) == :relative and hd(Path.split(relative)) != "..")
    end
  else
    defp validate_test_local_durability_boundary(_remote_path, _captured_root),
      do: {:error, :workspace_preservation_required}
  end

  defp local_durability_path(remote_url) when is_binary(remote_url) do
    cond do
      Path.type(remote_url) == :absolute ->
        {:ok, remote_url}

      String.starts_with?(remote_url, "file://") ->
        case URI.parse(remote_url) do
          %URI{scheme: "file", host: host, path: path}
          when host in [nil, ""] and is_binary(path) ->
            decode_uri_path(path)

          _uri ->
            :external
        end

      true ->
        :external
    end
  end

  defp decode_uri_path(path) do
    if Regex.match?(~r/%(?![0-9A-Fa-f]{2})/, path),
      do: :invalid,
      else: {:ok, URI.decode(path)}
  end

  defp durability_boundary_path(remote_url) do
    case local_durability_path(remote_url) do
      {:ok, path} -> path
      _external_or_invalid -> ""
    end
  end

  defp quarantine_local_workspace(workspace, captured_root) do
    quarantine = workspace <> ".symphony-cleanup"

    with {:ok, source_identity} <- validate_local_cleanup_source(workspace, quarantine) do
      rename_local_workspace_to_quarantine(
        workspace,
        quarantine,
        captured_root,
        source_identity
      )
    end
  end

  defp rename_local_workspace_to_quarantine(workspace, quarantine, captured_root, source_identity) do
    case File.rename(workspace, quarantine) do
      :ok ->
        case validate_local_quarantine(quarantine, captured_root, source_identity) do
          :ok ->
            {:ok, {quarantine, source_identity}}

          {:error, reason} ->
            restore_local_quarantine(quarantine, workspace, captured_root, source_identity)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, {:workspace_quarantine_failed, reason}}
    end
  end

  defp validate_local_cleanup_source(workspace, quarantine) do
    case {File.lstat(workspace), File.lstat(quarantine)} do
      {{:ok, %File.Stat{type: :directory} = stat}, {:error, :enoent}} ->
        {:ok, {stat.major_device, stat.minor_device, stat.inode}}

      _other ->
        {:error, :workspace_preservation_required}
    end
  end

  defp local_quarantine_identity(quarantine) do
    case File.lstat(quarantine) do
      {:ok, %File.Stat{type: :directory} = stat} ->
        {:ok, {stat.major_device, stat.minor_device, stat.inode}}

      _other ->
        {:error, :workspace_preservation_required}
    end
  end

  defp validate_local_quarantine(quarantine, captured_root, expected_identity) do
    with {:ok, ^expected_identity} <- local_quarantine_identity(quarantine),
         :ok <- validate_path_against_root(quarantine, captured_root, nil) do
      :ok
    else
      _other -> {:error, :workspace_preservation_required}
    end
  end

  defp cleanup_local_quarantine(
         {quarantine, identity},
         workspace,
         captured_root,
         remote_url
       ) do
    do_cleanup_local_quarantine(quarantine, workspace, captured_root, identity, remote_url)
  end

  defp do_cleanup_local_quarantine(quarantine, workspace, captured_root, identity, remote_url) do
    result =
      with :ok <- validate_local_quarantine(quarantine, captured_root, identity),
           :ok <- validate_local_workspace_durable(quarantine, remote_url),
           :ok <- maybe_run_before_remove_hook(quarantine, nil),
           :ok <- validate_local_quarantine(quarantine, captured_root, identity),
           :ok <- maybe_revalidate_local_durability_after_hook(quarantine, remote_url),
           :ok <- validate_local_quarantine(quarantine, captured_root, identity) do
        {:ok, []}
      end

    case result do
      {:ok, _removed_paths} = removed ->
        removed

      {:error, reason} ->
        restore_local_quarantine(quarantine, workspace, captured_root, identity)
        {:error, reason, ""}
    end
  end

  defp restore_local_quarantine(quarantine, workspace, captured_root, expected_identity) do
    with {:error, :enoent} <- File.lstat(workspace),
         :ok <- validate_local_quarantine(quarantine, captured_root, expected_identity) do
      File.rename(quarantine, workspace)
    else
      _other -> {:error, :workspace_preservation_required}
    end
  end

  defp maybe_revalidate_local_durability_after_hook(workspace, remote_url) do
    case Config.settings!().hooks.before_remove do
      command when is_binary(command) and command != "" ->
        validate_local_workspace_durable(workspace, remote_url)

      _other ->
        :ok
    end
  end

  defp validate_local_workspace_durable(workspace, remote_url) do
    case File.lstat(workspace) do
      {:ok, %File.Stat{type: :directory}} ->
        "git"
        |> System.cmd(
          [
            "-c",
            "core.fsmonitor=false",
            "-C",
            workspace,
            "status",
            "--porcelain=v1",
            "--untracked-files=normal"
          ],
          stderr_to_stdout: true
        )
        |> validate_local_git_status(workspace, remote_url)

      _other ->
        {:error, :workspace_preservation_required}
    end
  end

  defp validate_local_git_status({output, 0}, workspace, remote_url) do
    if String.trim(output) == "",
      do: validate_local_head_durable(workspace, remote_url),
      else: {:error, :workspace_preservation_required}
  end

  defp validate_local_git_status({_output, _status}, _workspace, _remote_url),
    do: {:error, :workspace_preservation_required}

  defp validate_local_head_durable(workspace, remote_url) do
    verifier_root = Path.join(System.tmp_dir!(), "symphony-durability-" <> cleanup_token())
    verifier_repo = Path.join(verifier_root, "repo.git")
    verifier_home = Path.join(verifier_root, "home")

    git_env = [
      {"GIT_CONFIG_NOSYSTEM", "1"},
      {"GIT_TERMINAL_PROMPT", "0"},
      {"GCM_INTERACTIVE", "never"},
      {"GIT_SSH_COMMAND", "ssh -o BatchMode=yes -o ConnectTimeout=10 -o ConnectionAttempts=1"},
      {"HOME", verifier_home},
      {"XDG_CONFIG_HOME", verifier_home}
    ]

    try do
      with {head_oid, 0} <-
             System.cmd(
               "git",
               ["-C", workspace, "rev-parse", "--verify", "HEAD^{commit}"],
               stderr_to_stdout: true
             ),
           :ok <- File.mkdir_p(verifier_home),
           :ok <- File.chmod(verifier_root, 0o700),
           {_output, 0} <-
             System.cmd("git", ["init", "--quiet", "--bare", verifier_repo],
               stderr_to_stdout: true,
               env: git_env
             ),
           {_output, 0} <-
             System.cmd(
               "git",
               [
                 "-C",
                 verifier_repo,
                 "fetch",
                 "--quiet",
                 "--no-tags",
                 "--force",
                 remote_url,
                 "+refs/heads/*:refs/verify/*"
               ],
               stderr_to_stdout: true,
               env: git_env
             ),
           {remote_refs, 0} <-
             System.cmd(
               "git",
               [
                 "-C",
                 verifier_repo,
                 "for-each-ref",
                 "--format=%(refname)",
                 "--contains",
                 String.trim(head_oid),
                 "refs/verify/"
               ],
               stderr_to_stdout: true,
               env: git_env
             ) do
        if String.trim(remote_refs) == "",
          do: {:error, :workspace_preservation_required},
          else: :ok
      else
        {_output, _status} -> {:error, :workspace_preservation_required}
      end
    after
      File.rm_rf(verifier_root)
    end
  end

  defp remove_remote_quarantine_if_durable(
         workspace,
         root,
         remote_url,
         durability_path,
         worker_host
       ) do
    hook_command = Config.settings!().hooks.before_remove || ""

    script =
      [
        "set -eu",
        remote_shell_assign("root", root),
        remote_shell_assign("workspace", workspace),
        remote_shell_assign("remote_url", remote_url),
        remote_shell_assign("durability_path", durability_path),
        remote_shell_assign("hook_command", hook_command),
        "quarantine=\"${workspace}.symphony-cleanup\"",
        "canonical_root=$(CDPATH= cd -P \"$root\" 2>/dev/null && pwd -P) || exit 75",
        "if [ -n \"$durability_path\" ]; then",
        "  canonical_durability=$(CDPATH= cd -P \"$durability_path\" 2>/dev/null && pwd -P) || exit 75",
        "  case \"$canonical_durability\" in",
        "    \"$canonical_root\"|\"$canonical_root\"/*) exit 75 ;;",
        "  esac",
        "fi",
        "quarantine_parent=${quarantine%/*}",
        "canonical_parent=$(CDPATH= cd -P \"$quarantine_parent\" 2>/dev/null && pwd -P) || exit 75",
        "test \"$canonical_parent\" = \"$canonical_root\" || exit 75",
        "if [ ! -e \"$workspace\" ]; then",
        "  exit 72",
        "fi",
        "test ! -e \"$quarantine\" || exit 72",
        "test -d \"$workspace\" && test ! -L \"$workspace\" || exit 73",
        "stat_identity() {",
        "  if stat -c '%d:%i' \"$1\" >/dev/null 2>&1; then",
        "    stat -c '%d:%i' \"$1\" 2>/dev/null",
        "  else",
        "    stat -f '%d:%i' \"$1\" 2>/dev/null",
        "  fi",
        "}",
        "source_identity=$(stat_identity \"$workspace\") || exit 73",
        "mv -- \"$workspace\" \"$quarantine\" || exit 73",
        "restore=1",
        "verifier=",
        "check_identity() {",
        "  test -d \"$quarantine\" && test ! -L \"$quarantine\" || return 74",
        "  current_identity=$(stat_identity \"$quarantine\") || return 74",
        "  test \"$current_identity\" = \"$source_identity\" || return 74",
        "  current_parent=${quarantine%/*}",
        "  current_canonical_parent=$(CDPATH= cd -P \"$current_parent\" 2>/dev/null && pwd -P) || return 75",
        "  test \"$current_canonical_parent\" = \"$canonical_root\" || return 75",
        "}",
        "restore_workspace() {",
        "  status=$?",
        "  trap - EXIT HUP INT TERM",
        "  if [ -n \"$verifier\" ]; then rm -rf -- \"$verifier\" >/dev/null 2>&1 || true; fi",
        "  if [ \"$restore\" = 1 ] && [ -e \"$quarantine\" ] && [ ! -e \"$workspace\" ]; then",
        "    if check_identity; then mv -- \"$quarantine\" \"$workspace\" >/dev/null 2>&1 || true; fi",
        "  fi",
        "  exit \"$status\"",
        "}",
        "trap restore_workspace EXIT HUP INT TERM",
        "check_identity",
        "check_durable() {",
        "  git -C \"$quarantine\" rev-parse --is-inside-work-tree >/dev/null 2>&1 || return 76",
        "  test -z \"$(git -c core.fsmonitor=false -C \"$quarantine\" status --porcelain=v1 --untracked-files=normal)\" || return 77",
        "  head_oid=$(git -C \"$quarantine\" rev-parse --verify 'HEAD^{commit}') || return 78",
        "  verifier=$(mktemp -d \"${TMPDIR:-/tmp}/symphony-durability.XXXXXX\") || return 79",
        "  chmod 700 \"$verifier\" || return 79",
        "  mkdir -p \"$verifier/home\" || return 79",
        "  export GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0 GCM_INTERACTIVE=never",
        "  export GIT_SSH_COMMAND='ssh -o BatchMode=yes -o ConnectTimeout=10 -o ConnectionAttempts=1'",
        "  export HOME=\"$verifier/home\" XDG_CONFIG_HOME=\"$verifier/home\"",
        "  git init --quiet --bare \"$verifier/repo.git\" || return 79",
        "  git -C \"$verifier/repo.git\" fetch --quiet --no-tags --force \"$remote_url\" '+refs/heads/*:refs/verify/*' >/dev/null 2>&1 || return 79",
        "  remote_refs=$(git -C \"$verifier/repo.git\" for-each-ref --format='%(refname)' --contains \"$head_oid\" refs/verify/)",
        "  rm -rf -- \"$verifier\"",
        "  verifier=",
        "  test -n \"$remote_refs\" || return 80",
        "}",
        "check_identity",
        "check_durable",
        "if [ -n \"$hook_command\" ]; then",
        "  (cd \"$quarantine\" && sh -lc \"$hook_command\") || true",
        "  check_identity",
        "  check_durable",
        "fi",
        "check_identity",
        "restore=0",
        "trap - EXIT HUP INT TERM",
        "printf '%s\\n' '#{@remote_durability_marker}'"
      ]
      |> Enum.join("\n")

    case run_remote_command(worker_host, script, Config.settings!().hooks.timeout_ms) do
      {:ok, {output, 0}} ->
        if output |> String.split("\n", trim: true) |> Enum.member?(@remote_durability_marker),
          do: {:ok, []},
          else: {:error, :workspace_preservation_required, ""}

      {:ok, {_output, _status}} ->
        {:error, :workspace_preservation_required, ""}

      {:error, _reason} ->
        {:error, :workspace_preservation_required, ""}
    end
  end

  defp cleanup_token do
    12
    |> :crypto.strong_rand_bytes()
    |> Base.encode16(case: :lower)
  end

  @spec remove_issue_workspaces(term()) :: :ok
  def remove_issue_workspaces(identifier), do: remove_issue_workspaces(identifier, nil)

  @spec remove_issue_workspaces(term(), worker_host()) :: :ok
  def remove_issue_workspaces(identifier, worker_host) when is_binary(identifier) and is_binary(worker_host) do
    safe_id = safe_identifier(identifier)

    case workspace_path_for_issue(safe_id, worker_host) do
      {:ok, workspace} -> remove(workspace, worker_host)
      {:error, _reason} -> :ok
    end

    :ok
  end

  def remove_issue_workspaces(identifier, nil) when is_binary(identifier) do
    safe_id = safe_identifier(identifier)

    case Config.settings!().worker.ssh_hosts do
      [] ->
        case workspace_path_for_issue(safe_id, nil) do
          {:ok, workspace} -> remove(workspace, nil)
          {:error, _reason} -> :ok
        end

      worker_hosts ->
        Enum.each(worker_hosts, &remove_issue_workspaces(identifier, &1))
    end

    :ok
  end

  def remove_issue_workspaces(_identifier, _worker_host) do
    :ok
  end

  @spec run_before_run_hook(Path.t(), map() | String.t() | nil, worker_host()) ::
          :ok | {:error, term()}
  def run_before_run_hook(workspace, issue_or_identifier, worker_host \\ nil) when is_binary(workspace) do
    issue_context = issue_context(issue_or_identifier)
    hooks = Config.settings!().hooks

    case hooks.before_run do
      nil ->
        :ok

      command ->
        run_hook(command, workspace, issue_context, "before_run", worker_host)
    end
  end

  @spec run_after_run_hook(Path.t(), map() | String.t() | nil, worker_host()) :: :ok
  def run_after_run_hook(workspace, issue_or_identifier, worker_host \\ nil) when is_binary(workspace) do
    issue_context = issue_context(issue_or_identifier)
    hooks = Config.settings!().hooks

    case hooks.after_run do
      nil ->
        :ok

      command ->
        run_hook(command, workspace, issue_context, "after_run", worker_host)
        |> ignore_hook_failure()
    end
  end

  defp workspace_path_for_issue(safe_id, nil) when is_binary(safe_id) do
    Config.settings!().workspace.root
    |> Path.join(safe_id)
    |> PathSafety.canonicalize()
  end

  defp workspace_path_for_issue(safe_id, worker_host) when is_binary(safe_id) and is_binary(worker_host) do
    {:ok, Path.join(Config.settings!().workspace.root, safe_id)}
  end

  defp prepare_target(safe_id, worker_host, opts) do
    expected_path = Keyword.get(opts, :expected_workspace_path)

    if is_binary(expected_path) do
      prepare_affinity_target(expected_path, worker_host, opts)
    else
      prepare_fresh_target(safe_id, worker_host)
    end
  end

  defp prepare_fresh_target(safe_id, nil) do
    with {:ok, workspace} <- workspace_path_for_issue(safe_id, nil),
         :ok <- validate_workspace_path(workspace, nil),
         {:ok, root} <- workspace_root(nil) do
      {:ok, %{path: workspace, root: root}}
    end
  end

  defp prepare_fresh_target(safe_id, worker_host) when is_binary(worker_host) do
    root = Config.settings!().workspace.root
    workspace = Path.join(root, safe_id)

    with :ok <- validate_affinity_path(workspace, worker_host),
         :ok <- validate_affinity_path(root, worker_host),
         {:ok, target} <- resolve_remote_affinity_target(workspace, root, worker_host),
         :ok <- validate_path_against_root(target.path, target.root, worker_host) do
      {:ok, target}
    end
  end

  defp prepare_affinity_target(expected_path, worker_host, opts) do
    expected_host = Keyword.get(opts, :expected_worker_host, worker_host)
    expected_root = Keyword.get(opts, :expected_workspace_root) || Path.dirname(expected_path)

    with :ok <- validate_expected_worker_host(expected_host, worker_host),
         :ok <- validate_affinity_path(expected_path, worker_host),
         :ok <- validate_affinity_path(expected_root, worker_host),
         {:ok, target} <- canonical_affinity_target(expected_path, expected_root, worker_host),
         :ok <- validate_path_against_root(target.path, target.root, worker_host) do
      {:ok, target}
    end
  end

  defp validate_affinity_path(path, worker_host) when is_binary(path) do
    if String.trim(path) != "" and not String.contains?(path, ["\n", "\r", "\t", <<0>>]) do
      :ok
    else
      {:error, {:workspace_affinity_mismatch, path, :invalid_path, worker_host}}
    end
  end

  defp validate_affinity_path(path, worker_host),
    do: {:error, {:workspace_affinity_mismatch, path, :invalid_path, worker_host}}

  defp canonical_affinity_target(expected_path, expected_root, nil)
       when is_binary(expected_path) and is_binary(expected_root) do
    with {:ok, path} <- PathSafety.canonicalize(expected_path),
         {:ok, root} <- PathSafety.canonicalize(expected_root) do
      {:ok, %{path: path, root: root}}
    end
  end

  defp canonical_affinity_target(expected_path, expected_root, worker_host)
       when is_binary(expected_path) and is_binary(expected_root) and is_binary(worker_host) do
    with :ok <- validate_persisted_remote_path_forms(expected_path, expected_root, worker_host),
         {:ok, target} <- resolve_remote_affinity_target(expected_path, expected_root, worker_host),
         :ok <- validate_resolved_remote_affinity(expected_path, expected_root, target, worker_host) do
      {:ok, target}
    end
  end

  defp canonical_affinity_target(expected_path, expected_root, worker_host),
    do: {:error, {:workspace_affinity_mismatch, expected_path, expected_root, worker_host}}

  defp validate_persisted_remote_path_forms(path, root, worker_host) do
    absolute_affinity? = remote_absolute_path?(path) and remote_absolute_path?(root)

    legacy_tilde_affinity? =
      (remote_tilde_path?(path) or remote_tilde_path?(root)) and
        remote_resolvable_path?(path) and remote_resolvable_path?(root)

    if absolute_affinity? or legacy_tilde_affinity?,
      do: :ok,
      else: {:error, {:workspace_affinity_mismatch, path, root, worker_host}}
  end

  defp validate_resolved_remote_affinity(path, root, target, worker_host) do
    path_matches? = remote_tilde_path?(path) or target.path == path
    root_matches? = remote_tilde_path?(root) or target.root == root

    if path_matches? and root_matches?,
      do: :ok,
      else: {:error, {:workspace_affinity_mismatch, path, root, target.path, target.root, worker_host}}
  end

  defp resolve_remote_affinity_target(path, root, worker_host) do
    with :ok <- validate_remote_resolvable_path(path, worker_host),
         :ok <- validate_remote_resolvable_path(root, worker_host) do
      script = remote_affinity_preflight_script(path, root)

      case run_remote_command(worker_host, script, Config.settings!().hooks.timeout_ms) do
        {:ok, {output, 0}} ->
          parse_remote_affinity_output(output, worker_host)

        {:ok, {output, status}} ->
          {:error, {:workspace_affinity_preflight_failed, worker_host, status, output}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp remote_affinity_preflight_script(path, root) do
    [
      "set -eu",
      remote_shell_assign("root", root),
      remote_shell_assign("workspace", path),
      "resolve_remote_path() {",
      "  candidate=$1",
      "  case \"$candidate\" in /*) ;; *) return 64 ;; esac",
      "  parent=$candidate",
      "  suffix=",
      "  while [ ! -d \"$parent\" ]; do",
      "    [ \"$parent\" = / ] && return 65",
      "    leaf=${parent##*/}",
      "    [ -n \"$leaf\" ] || return 65",
      "    suffix=/$leaf$suffix",
      "    parent=${parent%/*}",
      "    [ -n \"$parent\" ] || parent=/",
      "  done",
      "  canonical_parent=$(CDPATH= cd -P \"$parent\" 2>/dev/null && pwd -P) || return 65",
      "  case \"$canonical_parent\" in",
      "    /) printf '/%s\\n' \"${suffix#/}\" ;;",
      "    *) printf '%s%s\\n' \"$canonical_parent\" \"$suffix\" ;;",
      "  esac",
      "}",
      "canonical_root=$(resolve_remote_path \"$root\")",
      "canonical_workspace=$(resolve_remote_path \"$workspace\")",
      "printf '%s\\t%s\\t%s\\n' '#{@remote_affinity_marker}' \"$canonical_root\" \"$canonical_workspace\""
    ]
    |> Enum.join("\n")
  end

  defp validate_remote_resolvable_path(path, worker_host) do
    if remote_resolvable_path?(path) and not remote_dot_segment?(path),
      do: :ok,
      else: {:error, {:workspace_affinity_mismatch, path, :invalid_remote_path, worker_host}}
  end

  defp remote_resolvable_path?(path), do: remote_absolute_path?(path) or remote_tilde_path?(path)
  defp remote_absolute_path?(path) when is_binary(path), do: String.starts_with?(path, "/")
  defp remote_absolute_path?(_path), do: false
  defp remote_tilde_path?("~"), do: true
  defp remote_tilde_path?("~/" <> _rest), do: true
  defp remote_tilde_path?(_path), do: false

  defp remote_dot_segment?(path) do
    path
    |> String.split("/", trim: true)
    |> Enum.any?(&(&1 in [".", ".."]))
  end

  defp validate_expected_worker_host(worker_host, worker_host), do: :ok

  defp validate_expected_worker_host(expected_host, worker_host),
    do: {:error, {:workspace_host_affinity_mismatch, expected_host, worker_host}}

  defp workspace_root(nil), do: PathSafety.canonicalize(Config.settings!().workspace.root)

  defp safe_identifier(identifier) do
    String.replace(identifier || "issue", ~r/[^a-zA-Z0-9._-]/, "_")
  end

  defp maybe_run_after_create_hook(workspace, issue_context, created?, worker_host) do
    hooks = Config.settings!().hooks

    case created? do
      true ->
        case hooks.after_create do
          nil ->
            :ok

          command ->
            run_hook(command, workspace, issue_context, "after_create", worker_host)
        end

      false ->
        :ok
    end
  end

  defp maybe_run_before_remove_hook(workspace, nil) do
    hooks = Config.settings!().hooks

    case File.dir?(workspace) do
      true ->
        case hooks.before_remove do
          nil ->
            :ok

          command ->
            run_hook(
              command,
              workspace,
              %{issue_id: nil, issue_identifier: Path.basename(workspace)},
              "before_remove",
              nil
            )
            |> ignore_hook_failure()
        end

      false ->
        :ok
    end
  end

  defp maybe_run_before_remove_hook(workspace, worker_host) when is_binary(worker_host) do
    hooks = Config.settings!().hooks

    case hooks.before_remove do
      nil ->
        :ok

      command ->
        script =
          [
            remote_shell_assign("workspace", workspace),
            "if [ -d \"$workspace\" ]; then",
            "  cd \"$workspace\"",
            "  #{command}",
            "fi"
          ]
          |> Enum.join("\n")

        run_remote_command(worker_host, script, Config.settings!().hooks.timeout_ms)
        |> case do
          {:ok, {output, status}} ->
            handle_hook_command_result(
              {output, status},
              workspace,
              %{issue_id: nil, issue_identifier: Path.basename(workspace)},
              "before_remove"
            )

          {:error, {:workspace_hook_timeout, "before_remove", _timeout_ms} = reason} ->
            {:error, reason}

          {:error, reason} ->
            {:error, reason}
        end
        |> ignore_hook_failure()
    end
  end

  defp ignore_hook_failure(:ok), do: :ok
  defp ignore_hook_failure({:error, _reason}), do: :ok

  defp run_hook(command, workspace, issue_context, hook_name, nil) do
    timeout_ms = Config.settings!().hooks.timeout_ms

    Logger.info("Running workspace hook hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} worker_host=local")

    task =
      Task.async(fn ->
        System.cmd("sh", ["-lc", command], cd: workspace, stderr_to_stdout: true)
      end)

    case Task.yield(task, timeout_ms) do
      {:ok, cmd_result} ->
        handle_hook_command_result(cmd_result, workspace, issue_context, hook_name)

      nil ->
        Task.shutdown(task, :brutal_kill)

        Logger.warning("Workspace hook timed out hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} worker_host=local timeout_ms=#{timeout_ms}")

        {:error, {:workspace_hook_timeout, hook_name, timeout_ms}}
    end
  end

  defp run_hook(command, workspace, issue_context, hook_name, worker_host) when is_binary(worker_host) do
    timeout_ms = Config.settings!().hooks.timeout_ms

    Logger.info("Running workspace hook hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} worker_host=#{worker_host}")

    case run_remote_command(worker_host, "cd #{shell_escape(workspace)} && #{command}", timeout_ms) do
      {:ok, cmd_result} ->
        handle_hook_command_result(cmd_result, workspace, issue_context, hook_name)

      {:error, {:workspace_hook_timeout, ^hook_name, _timeout_ms} = reason} ->
        {:error, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_hook_command_result({_output, 0}, _workspace, _issue_id, _hook_name) do
    :ok
  end

  defp handle_hook_command_result({output, status}, workspace, issue_context, hook_name) do
    sanitized_output = sanitize_hook_output_for_log(output)

    Logger.warning("Workspace hook failed hook=#{hook_name} #{issue_log_context(issue_context)} workspace=#{workspace} status=#{status} output=#{inspect(sanitized_output)}")

    {:error, {:workspace_hook_failed, hook_name, status, output}}
  end

  defp sanitize_hook_output_for_log(output, max_bytes \\ 2_048) do
    binary_output = IO.iodata_to_binary(output)

    case byte_size(binary_output) <= max_bytes do
      true ->
        binary_output

      false ->
        binary_part(binary_output, 0, max_bytes) <> "... (truncated)"
    end
  end

  defp validate_workspace_path(workspace, nil) when is_binary(workspace) do
    expanded_workspace = Path.expand(workspace)
    expanded_root = Path.expand(Config.settings!().workspace.root)
    expanded_root_prefix = expanded_root <> "/"

    with {:ok, canonical_workspace} <- PathSafety.canonicalize(expanded_workspace),
         {:ok, canonical_root} <- PathSafety.canonicalize(expanded_root) do
      canonical_root_prefix = canonical_root <> "/"

      cond do
        canonical_workspace == canonical_root ->
          {:error, {:workspace_equals_root, canonical_workspace, canonical_root}}

        String.starts_with?(canonical_workspace <> "/", canonical_root_prefix) ->
          :ok

        String.starts_with?(expanded_workspace <> "/", expanded_root_prefix) ->
          {:error, {:workspace_symlink_escape, expanded_workspace, canonical_root}}

        true ->
          {:error, {:workspace_outside_root, canonical_workspace, canonical_root}}
      end
    else
      {:error, {:path_canonicalize_failed, path, reason}} ->
        {:error, {:workspace_path_unreadable, path, reason}}
    end
  end

  defp validate_path_against_root(workspace, root, nil)
       when is_binary(workspace) and is_binary(root) do
    with {:ok, canonical_workspace} <- PathSafety.canonicalize(workspace),
         {:ok, canonical_root} <- PathSafety.canonicalize(root) do
      root_prefix = canonical_root <> "/"

      cond do
        canonical_workspace == canonical_root ->
          {:error, {:workspace_equals_root, canonical_workspace, canonical_root}}

        String.starts_with?(canonical_workspace <> "/", root_prefix) ->
          :ok

        true ->
          {:error, {:workspace_outside_root, canonical_workspace, canonical_root}}
      end
    end
  end

  defp validate_path_against_root(workspace, root, worker_host)
       when is_binary(workspace) and is_binary(root) and is_binary(worker_host) do
    with :ok <- validate_remote_absolute_path(workspace, worker_host),
         :ok <- validate_remote_absolute_path(root, worker_host) do
      root_prefix = if root == "/", do: root, else: root <> "/"

      cond do
        workspace == root ->
          {:error, {:workspace_equals_root, workspace, root}}

        String.starts_with?(workspace, root_prefix) ->
          :ok

        true ->
          {:error, {:workspace_outside_root, workspace, root}}
      end
    end
  end

  defp validate_remote_absolute_path(path, worker_host) do
    canonical_shape? =
      remote_absolute_path?(path) and
        not String.contains?(path, "//") and
        (path == "/" or not String.ends_with?(path, "/")) and
        not remote_dot_segment?(path)

    if canonical_shape?,
      do: :ok,
      else: {:error, {:workspace_affinity_mismatch, path, :noncanonical_remote_path, worker_host}}
  end

  defp validate_prepared_workspace_exists(workspace, nil) do
    if File.dir?(workspace), do: :ok, else: {:error, {:prepared_workspace_missing, workspace, nil}}
  end

  defp validate_prepared_workspace_exists(workspace, worker_host) when is_binary(worker_host) do
    if String.trim(workspace) != "" and not String.contains?(workspace, ["\n", "\r", <<0>>]) do
      :ok
    else
      {:error, {:prepared_workspace_missing, workspace, worker_host}}
    end
  end

  defp remote_shell_assign(variable_name, raw_path)
       when is_binary(variable_name) and is_binary(raw_path) do
    [
      "#{variable_name}=#{shell_escape(raw_path)}",
      "case \"$#{variable_name}\" in",
      "  '~') #{variable_name}=\"$HOME\" ;;",
      "  '~/'*) " <> variable_name <> "=\"$HOME/${" <> variable_name <> "#~/}\" ;;",
      "esac"
    ]
    |> Enum.join("\n")
  end

  defp parse_remote_workspace_output(output) do
    lines = String.split(IO.iodata_to_binary(output), "\n", trim: true)

    payload =
      Enum.find_value(lines, fn line ->
        case String.split(line, "\t", parts: 3) do
          [@remote_workspace_marker, created, path] when created in ["0", "1"] and path != "" ->
            {created == "1", path}

          _ ->
            nil
        end
      end)

    case payload do
      {created?, workspace} when is_boolean(created?) and is_binary(workspace) ->
        {:ok, workspace, created?}

      _ ->
        {:error, {:workspace_prepare_failed, :invalid_output, output}}
    end
  end

  defp parse_remote_affinity_output(output, worker_host) do
    payload =
      output
      |> IO.iodata_to_binary()
      |> String.split("\n", trim: true)
      |> Enum.find_value(fn line ->
        case String.split(line, "\t", parts: 3) do
          [@remote_affinity_marker, root, path] when root != "" and path != "" ->
            %{path: path, root: root}

          _ ->
            nil
        end
      end)

    case payload do
      %{path: path, root: root} = target ->
        with :ok <- validate_remote_absolute_path(path, worker_host),
             :ok <- validate_remote_absolute_path(root, worker_host) do
          {:ok, target}
        end

      _ ->
        {:error, {:workspace_affinity_preflight_failed, worker_host, :invalid_output, output}}
    end
  end

  defp run_remote_command(worker_host, script, timeout_ms)
       when is_binary(worker_host) and is_binary(script) and is_integer(timeout_ms) and timeout_ms > 0 do
    task =
      Task.async(fn ->
        SSH.run(worker_host, script, stderr_to_stdout: true)
      end)

    case Task.yield(task, timeout_ms) do
      {:ok, result} ->
        result

      nil ->
        Task.shutdown(task, :brutal_kill)
        {:error, {:workspace_hook_timeout, "remote_command", timeout_ms}}
    end
  end

  defp shell_escape(value) when is_binary(value) do
    "'" <> String.replace(value, "'", "'\"'\"'") <> "'"
  end

  defp worker_host_for_log(nil), do: "local"
  defp worker_host_for_log(worker_host), do: worker_host

  defp issue_context(%{id: issue_id, identifier: identifier}) do
    %{
      issue_id: issue_id,
      issue_identifier: identifier || "issue"
    }
  end

  defp issue_context(identifier) when is_binary(identifier) do
    %{
      issue_id: nil,
      issue_identifier: identifier
    }
  end

  defp issue_context(_identifier) do
    %{
      issue_id: nil,
      issue_identifier: "issue"
    }
  end

  defp issue_log_context(%{issue_id: issue_id, issue_identifier: issue_identifier}) do
    "issue_id=#{issue_id || "n/a"} issue_identifier=#{issue_identifier || "issue"}"
  end
end
