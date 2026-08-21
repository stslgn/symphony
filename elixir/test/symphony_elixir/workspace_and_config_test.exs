defmodule SymphonyElixir.WorkspaceAndConfigTest do
  use SymphonyElixir.TestSupport
  alias Ecto.Changeset
  alias SymphonyElixir.Config.Schema
  alias SymphonyElixir.Config.Schema.{Codex, StringOrMap}
  alias SymphonyElixir.Linear.Client

  test "workspace bootstrap can be implemented in after_create hook" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-workspace-hook-bootstrap-#{System.unique_integer([:positive])}"
      )

    try do
      template_repo = Path.join(test_root, "source")
      workspace_root = Path.join(test_root, "workspaces")

      File.mkdir_p!(template_repo)
      File.mkdir_p!(Path.join(template_repo, "keep"))
      File.write!(Path.join([template_repo, "keep", "file.txt"]), "keep me")
      File.write!(Path.join(template_repo, "README.md"), "hook clone\n")
      System.cmd("git", ["-C", template_repo, "init", "-b", "main"])
      System.cmd("git", ["-C", template_repo, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", template_repo, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", template_repo, "add", "README.md", "keep/file.txt"])
      System.cmd("git", ["-C", template_repo, "commit", "-m", "initial"])

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        hook_after_create: "git clone --depth 1 #{template_repo} ."
      )

      assert {:ok, workspace} = Workspace.create_for_issue("S-1")
      assert File.exists?(Path.join(workspace, ".git"))
      assert File.read!(Path.join(workspace, "README.md")) == "hook clone\n"
      assert File.read!(Path.join([workspace, "keep", "file.txt"])) == "keep me"
    after
      File.rm_rf(test_root)
    end
  end

  test "workspace path is deterministic per issue identifier" do
    workspace_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-workspace-deterministic-#{System.unique_integer([:positive])}"
      )

    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: workspace_root)

    assert {:ok, first_workspace} = Workspace.create_for_issue("MT/Det")
    assert {:ok, second_workspace} = Workspace.create_for_issue("MT/Det")

    assert first_workspace == second_workspace
    assert Path.basename(first_workspace) == "MT_Det"
  end

  test "workspace reuses existing issue directory without deleting local changes" do
    workspace_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-workspace-reuse-#{System.unique_integer([:positive])}"
      )

    try do
      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        hook_after_create: "echo first > README.md"
      )

      assert {:ok, first_workspace} = Workspace.create_for_issue("MT-REUSE")

      File.write!(Path.join(first_workspace, "README.md"), "changed\n")
      File.write!(Path.join(first_workspace, "local-progress.txt"), "in progress\n")
      File.mkdir_p!(Path.join(first_workspace, "deps"))
      File.mkdir_p!(Path.join(first_workspace, "_build"))
      File.mkdir_p!(Path.join(first_workspace, "tmp"))
      File.write!(Path.join([first_workspace, "deps", "cache.txt"]), "cached deps\n")
      File.write!(Path.join([first_workspace, "_build", "artifact.txt"]), "compiled artifact\n")
      File.write!(Path.join([first_workspace, "tmp", "scratch.txt"]), "remove me\n")

      assert {:ok, second_workspace} = Workspace.create_for_issue("MT-REUSE")
      assert second_workspace == first_workspace
      assert File.read!(Path.join(second_workspace, "README.md")) == "changed\n"
      assert File.read!(Path.join(second_workspace, "local-progress.txt")) == "in progress\n"
      assert File.read!(Path.join([second_workspace, "deps", "cache.txt"])) == "cached deps\n"
      assert File.read!(Path.join([second_workspace, "_build", "artifact.txt"])) == "compiled artifact\n"
      assert File.read!(Path.join([second_workspace, "tmp", "scratch.txt"])) == "remove me\n"
    after
      File.rm_rf(workspace_root)
    end
  end

  test "workspace replaces stale non-directory paths" do
    workspace_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-workspace-stale-path-#{System.unique_integer([:positive])}"
      )

    try do
      stale_workspace = Path.join(workspace_root, "MT-STALE")
      File.mkdir_p!(workspace_root)
      File.write!(stale_workspace, "old state\n")

      write_workflow_file!(Workflow.workflow_file_path(), workspace_root: workspace_root)

      assert {:ok, canonical_workspace} = SymphonyElixir.PathSafety.canonicalize(stale_workspace)
      assert {:ok, workspace} = Workspace.create_for_issue("MT-STALE")
      assert workspace == canonical_workspace
      assert File.dir?(workspace)
    after
      File.rm_rf(workspace_root)
    end
  end

  test "workspace rejects symlink escapes under the configured root" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-workspace-symlink-#{System.unique_integer([:positive])}"
      )

    try do
      workspace_root = Path.join(test_root, "workspaces")
      outside_root = Path.join(test_root, "outside")
      symlink_path = Path.join(workspace_root, "MT-SYM")

      File.mkdir_p!(workspace_root)
      File.mkdir_p!(outside_root)
      File.ln_s!(outside_root, symlink_path)

      write_workflow_file!(Workflow.workflow_file_path(), workspace_root: workspace_root)

      assert {:ok, canonical_outside_root} = SymphonyElixir.PathSafety.canonicalize(outside_root)
      assert {:ok, canonical_workspace_root} = SymphonyElixir.PathSafety.canonicalize(workspace_root)

      assert {:error, {:workspace_outside_root, ^canonical_outside_root, ^canonical_workspace_root}} =
               Workspace.create_for_issue("MT-SYM")
    after
      File.rm_rf(test_root)
    end
  end

  test "workspace canonicalizes symlinked workspace roots before creating issue directories" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-workspace-root-symlink-#{System.unique_integer([:positive])}"
      )

    try do
      actual_root = Path.join(test_root, "actual-workspaces")
      linked_root = Path.join(test_root, "linked-workspaces")

      File.mkdir_p!(actual_root)
      File.ln_s!(actual_root, linked_root)

      write_workflow_file!(Workflow.workflow_file_path(), workspace_root: linked_root)

      assert {:ok, canonical_workspace} =
               SymphonyElixir.PathSafety.canonicalize(Path.join(actual_root, "MT-LINK"))

      assert {:ok, workspace} = Workspace.create_for_issue("MT-LINK")
      assert workspace == canonical_workspace
      assert File.dir?(workspace)
    after
      File.rm_rf(test_root)
    end
  end

  test "workspace remove rejects the workspace root itself with a distinct error" do
    workspace_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-workspace-root-remove-#{System.unique_integer([:positive])}"
      )

    try do
      File.mkdir_p!(workspace_root)
      write_workflow_file!(Workflow.workflow_file_path(), workspace_root: workspace_root)

      assert {:ok, canonical_workspace_root} =
               SymphonyElixir.PathSafety.canonicalize(workspace_root)

      assert {:error, {:workspace_equals_root, ^canonical_workspace_root, ^canonical_workspace_root}, ""} =
               Workspace.remove(workspace_root)
    after
      File.rm_rf(workspace_root)
    end
  end

  test "automatic terminal cleanup preserves a dirty workspace" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-dirty-terminal-preservation-#{System.unique_integer([:positive])}"
      )

    source = Path.join(test_root, "source")
    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-152")
    uncommitted = Path.join(workspace, "migration.sql")

    try do
      File.mkdir_p!(source)
      File.write!(Path.join(source, "README.md"), "baseline\n")
      System.cmd("git", ["-C", source, "init", "-b", "main"])
      System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", source, "add", "README.md"])
      System.cmd("git", ["-C", source, "commit", "-m", "baseline"])
      File.mkdir_p!(workspace_root)
      System.cmd("git", ["clone", source, workspace])
      File.write!(uncommitted, "create table preserved_work();\n")

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        workspace_durability_remote_url: source,
        hook_before_remove: "touch #{Path.join(test_root, "hook-must-not-run")}"
      )

      assert {:error, :workspace_preservation_required, ""} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      assert File.read!(uncommitted) == "create table preserved_work();\n"
      refute File.exists?(Path.join(test_root, "hook-must-not-run"))
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup preserves commits absent from remote refs" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-unpushed-terminal-preservation-#{System.unique_integer([:positive])}"
      )

    source = Path.join(test_root, "source")
    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-152")
    committed = Path.join(workspace, "migration.sql")

    try do
      File.mkdir_p!(source)
      File.write!(Path.join(source, "README.md"), "baseline\n")
      System.cmd("git", ["-C", source, "init", "-b", "main"])
      System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", source, "add", "README.md"])
      System.cmd("git", ["-C", source, "commit", "-m", "baseline"])
      File.mkdir_p!(workspace_root)
      System.cmd("git", ["clone", source, workspace])
      System.cmd("git", ["-C", workspace, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", workspace, "config", "user.email", "test@example.com"])
      File.write!(committed, "create table preserved_work();\n")
      System.cmd("git", ["-C", workspace, "add", "migration.sql"])
      System.cmd("git", ["-C", workspace, "commit", "-m", "unpublished migration"])

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        workspace_durability_remote_url: source
      )

      assert {:error, :workspace_preservation_required, ""} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      assert File.read!(committed) == "create table preserved_work();\n"
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup rejects unrelated remote-tracking refs" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-forged-ref-preservation-#{System.unique_integer([:positive])}"
      )

    source = Path.join(test_root, "source")
    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-152")
    committed = Path.join(workspace, "migration.sql")

    try do
      File.mkdir_p!(source)
      File.write!(Path.join(source, "README.md"), "baseline\n")
      System.cmd("git", ["-C", source, "init", "-b", "main"])
      System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", source, "add", "README.md"])
      System.cmd("git", ["-C", source, "commit", "-m", "baseline"])
      File.mkdir_p!(workspace_root)
      System.cmd("git", ["clone", source, workspace])
      System.cmd("git", ["-C", workspace, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", workspace, "config", "user.email", "test@example.com"])
      File.write!(committed, "create table preserved_work();\n")
      System.cmd("git", ["-C", workspace, "add", "migration.sql"])
      System.cmd("git", ["-C", workspace, "commit", "-m", "unpublished migration"])
      System.cmd("git", ["-C", workspace, "update-ref", "refs/remotes/fake/main", "HEAD"])

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        workspace_durability_remote_url: source
      )

      assert {:error, :workspace_preservation_required, ""} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      assert File.read!(committed) == "create table preserved_work();\n"
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup ignores a worker-modified origin URL" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-forged-origin-preservation-#{System.unique_integer([:positive])}"
      )

    trusted_source = Path.join(test_root, "trusted-source")
    forged_source = Path.join(test_root, "forged-source")
    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-152")
    committed = Path.join(workspace, "migration.sql")

    try do
      File.mkdir_p!(trusted_source)
      File.write!(Path.join(trusted_source, "README.md"), "baseline\n")
      System.cmd("git", ["-C", trusted_source, "init", "-b", "main"])
      System.cmd("git", ["-C", trusted_source, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", trusted_source, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", trusted_source, "add", "README.md"])
      System.cmd("git", ["-C", trusted_source, "commit", "-m", "baseline"])
      File.mkdir_p!(workspace_root)
      System.cmd("git", ["clone", trusted_source, workspace])
      System.cmd("git", ["-C", workspace, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", workspace, "config", "user.email", "test@example.com"])
      File.write!(committed, "create table preserved_work();\n")
      System.cmd("git", ["-C", workspace, "add", "migration.sql"])
      System.cmd("git", ["-C", workspace, "commit", "-m", "unpublished migration"])
      System.cmd("git", ["clone", "--bare", workspace, forged_source])
      System.cmd("git", ["-C", workspace, "remote", "set-url", "origin", forged_source])

      System.cmd("git", [
        "-C",
        workspace,
        "config",
        "url.#{forged_source}.insteadOf",
        trusted_source
      ])

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        workspace_durability_remote_url: trusted_source
      )

      assert {:error, :workspace_preservation_required, ""} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      assert File.read!(committed) == "create table preserved_work();\n"
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup preserves a clean workspace when trusted fetch fails" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-fetch-failure-preservation-#{System.unique_integer([:positive])}"
      )

    source = Path.join(test_root, "source")
    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-152")

    try do
      File.mkdir_p!(source)
      File.write!(Path.join(source, "README.md"), "baseline\n")
      System.cmd("git", ["-C", source, "init", "-b", "main"])
      System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", source, "add", "README.md"])
      System.cmd("git", ["-C", source, "commit", "-m", "baseline"])
      File.mkdir_p!(workspace_root)
      System.cmd("git", ["clone", source, workspace])

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        workspace_durability_remote_url: Path.join(test_root, "missing-source")
      )

      assert {:error, :workspace_preservation_required, ""} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      assert File.dir?(workspace)
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup bounds local git output before the command deadline" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-output-limit-preservation-#{System.unique_integer([:positive])}"
      )

    source = Path.join(test_root, "source")
    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-OUTPUT-LIMIT")
    fake_bin = Path.join(test_root, "fake-bin")
    fake_git = Path.join(fake_bin, "git")
    process_group_file = Path.join(test_root, "git-process-group.pid")
    previous_path = System.get_env("PATH")
    output_chunk = String.duplicate("x", 1_024)

    on_exit(fn ->
      case File.read(process_group_file) do
        {:ok, raw_pid} ->
          process_group = raw_pid |> String.trim() |> String.to_integer()

          System.cmd(
            "/bin/kill",
            ["-KILL", "--", "-#{process_group}"],
            stderr_to_stdout: true
          )

        {:error, _reason} ->
          :ok
      end

      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(source)
    File.write!(Path.join(source, "README.md"), "baseline\n")
    System.cmd("git", ["-C", source, "init", "-b", "main"])
    System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
    System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
    System.cmd("git", ["-C", source, "add", "README.md"])
    System.cmd("git", ["-C", source, "commit", "-m", "baseline"])
    File.mkdir_p!(workspace_root)
    System.cmd("git", ["clone", source, workspace])
    File.mkdir_p!(fake_bin)

    File.write!(fake_git, """
    #!/bin/sh
    printf '%s\\n' "$$" > '#{process_group_file}'
    output_chunk='#{output_chunk}'
    index=0
    while [ "$index" -lt 128 ]; do
      printf '%s' "$output_chunk"
      index=$((index + 1))
    done
    sleep 30
    """)

    File.chmod!(fake_git, 0o755)
    System.put_env("PATH", fake_bin <> ":" <> (previous_path || ""))

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: workspace_root,
      workspace_durability_remote_url: source,
      hook_timeout_ms: 30_000
    )

    task =
      Task.async(fn ->
        Workspace.remove_exact_if_durable(workspace, workspace_root, nil)
      end)

    result =
      case Task.yield(task, 3_000) do
        {:ok, result} ->
          result

        nil ->
          Task.shutdown(task, :brutal_kill)
          flunk("local durability validation exceeded the bounded output deadline")
      end

    assert {:error, :workspace_preservation_required, ""} = result
    assert File.dir?(workspace)
    refute File.exists?(workspace <> ".symphony-cleanup")
  end

  test "automatic terminal cleanup quarantines a clean remote-backed workspace" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-durable-terminal-cleanup-#{System.unique_integer([:positive])}"
      )

    source = Path.join(test_root, "source")
    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-153")
    quarantine = workspace <> ".symphony-cleanup"

    try do
      File.mkdir_p!(source)
      File.write!(Path.join(source, "README.md"), "durable baseline\n")
      System.cmd("git", ["-C", source, "init", "-b", "main"])
      System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", source, "add", "README.md"])
      System.cmd("git", ["-C", source, "commit", "-m", "durable baseline"])
      File.mkdir_p!(workspace_root)
      System.cmd("git", ["clone", source, workspace])

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        workspace_durability_remote_url: source
      )

      assert {:ok, _removed_paths} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      refute File.exists?(workspace)
      assert File.read!(Path.join(quarantine, "README.md")) == "durable baseline\n"
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup disables worker-controlled fsmonitor" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-fsmonitor-preservation-#{System.unique_integer([:positive])}"
      )

    source = Path.join(test_root, "source")
    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-153")
    fsmonitor = Path.join(test_root, "worker-fsmonitor")
    trace_file = Path.join(test_root, "fsmonitor.trace")

    try do
      File.mkdir_p!(source)
      File.write!(Path.join(source, "README.md"), "durable baseline\n")
      System.cmd("git", ["-C", source, "init", "-b", "main"])
      System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", source, "add", "README.md"])
      System.cmd("git", ["-C", source, "commit", "-m", "durable baseline"])
      File.mkdir_p!(workspace_root)
      System.cmd("git", ["clone", source, workspace])

      File.write!(fsmonitor, "#!/bin/sh\nprintf invoked >> '#{trace_file}'\n")
      File.chmod!(fsmonitor, 0o755)
      System.cmd("git", ["-C", workspace, "config", "core.fsmonitor", fsmonitor])

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        workspace_durability_remote_url: source
      )

      assert {:ok, []} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      refute File.exists?(trace_file)
      assert File.dir?(workspace <> ".symphony-cleanup")
    after
      File.rm_rf(test_root)
    end
  end

  test "successful terminal quarantine retains ignored and alternate-ref data" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-terminal-artifact-retention-#{System.unique_integer([:positive])}"
      )

    source = Path.join(test_root, "source")
    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-153")
    quarantine = workspace <> ".symphony-cleanup"

    try do
      File.mkdir_p!(source)
      File.write!(Path.join(source, "README.md"), "durable baseline\n")
      File.write!(Path.join(source, ".gitignore"), "ignored.log\n")
      System.cmd("git", ["-C", source, "init", "-b", "main"])
      System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", source, "add", "README.md", ".gitignore"])
      System.cmd("git", ["-C", source, "commit", "-m", "durable baseline"])
      File.mkdir_p!(workspace_root)
      System.cmd("git", ["clone", source, workspace])
      File.write!(Path.join(workspace, "ignored.log"), "local ignored evidence\n")
      System.cmd("git", ["-C", workspace, "branch", "local-evidence"])

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        workspace_durability_remote_url: source
      )

      assert {:ok, _removed_paths} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      assert File.read!(Path.join(quarantine, "ignored.log")) == "local ignored evidence\n"
      assert {_output, 0} = System.cmd("git", ["-C", quarantine, "rev-parse", "local-evidence"])
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup fails closed without a trusted durability remote" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-missing-durability-remote-#{System.unique_integer([:positive])}"
      )

    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-153")

    try do
      File.mkdir_p!(workspace)
      File.write!(Path.join(workspace, "progress.txt"), "preserve me\n")
      write_workflow_file!(Workflow.workflow_file_path(), workspace_root: workspace_root)

      assert {:error, :workspace_preservation_required, ""} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      assert File.read!(Path.join(workspace, "progress.txt")) == "preserve me\n"
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup preserves an interrupted local quarantine" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-quarantine-recovery-#{System.unique_integer([:positive])}"
      )

    source = Path.join(test_root, "source")
    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-153")
    quarantine = workspace <> ".symphony-cleanup"

    try do
      File.mkdir_p!(source)
      File.write!(Path.join(source, "README.md"), "durable baseline\n")
      System.cmd("git", ["-C", source, "init", "-b", "main"])
      System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", source, "add", "README.md"])
      System.cmd("git", ["-C", source, "commit", "-m", "durable baseline"])
      File.mkdir_p!(workspace_root)
      System.cmd("git", ["clone", source, workspace])
      File.rename!(workspace, quarantine)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        workspace_durability_remote_url: source
      )

      assert {:error, :workspace_preservation_required, ""} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      refute File.exists?(workspace)
      assert File.read!(Path.join(quarantine, "README.md")) == "durable baseline\n"
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup fails closed when workspace and quarantine are absent" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-missing-artifact-#{System.unique_integer([:positive])}"
      )

    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-153")
    File.mkdir_p!(workspace_root)

    try do
      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        workspace_durability_remote_url: "/srv/git/repo.git"
      )

      assert {:error, :workspace_preservation_required, ""} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup rejects conflicting workspace and quarantine paths" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-quarantine-conflict-#{System.unique_integer([:positive])}"
      )

    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-153")
    quarantine = workspace <> ".symphony-cleanup"

    try do
      File.mkdir_p!(workspace)
      File.mkdir_p!(quarantine)
      File.write!(Path.join(workspace, "progress.txt"), "primary\n")
      File.write!(Path.join(quarantine, "progress.txt"), "quarantine\n")
      write_workflow_file!(Workflow.workflow_file_path(), workspace_root: workspace_root)

      assert {:error, :workspace_preservation_required, ""} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      assert File.read!(Path.join(workspace, "progress.txt")) == "primary\n"
      assert File.read!(Path.join(quarantine, "progress.txt")) == "quarantine\n"
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup rechecks durability after before_remove" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-post-hook-terminal-preservation-#{System.unique_integer([:positive])}"
      )

    source = Path.join(test_root, "source")
    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-154")
    hook_output = Path.join(workspace, "post-hook.txt")

    try do
      File.mkdir_p!(source)
      File.write!(Path.join(source, "README.md"), "durable baseline\n")
      System.cmd("git", ["-C", source, "init", "-b", "main"])
      System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", source, "add", "README.md"])
      System.cmd("git", ["-C", source, "commit", "-m", "durable baseline"])
      File.mkdir_p!(workspace_root)
      System.cmd("git", ["clone", source, workspace])

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        workspace_durability_remote_url: source,
        hook_before_remove: "printf generated > post-hook.txt"
      )

      assert {:error, :workspace_preservation_required, ""} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      assert File.read!(hook_output) == "generated"
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup rejects a quarantine path swap" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-quarantine-swap-#{System.unique_integer([:positive])}"
      )

    source = Path.join(test_root, "source")
    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-155")
    outside = Path.join(test_root, "outside")
    sentinel = Path.join(outside, "must-survive.txt")

    try do
      File.mkdir_p!(source)
      File.mkdir_p!(outside)
      File.write!(Path.join(source, "README.md"), "durable baseline\n")
      File.write!(sentinel, "outside data\n")
      System.cmd("git", ["-C", source, "init", "-b", "main"])
      System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", source, "add", "README.md"])
      System.cmd("git", ["-C", source, "commit", "-m", "durable baseline"])
      File.mkdir_p!(workspace_root)
      System.cmd("git", ["clone", source, workspace])

      swap_command =
        "quarantine=$PWD; original=${quarantine%.symphony-cleanup}; " <>
          "cd ..; mv \"$quarantine\" \"$original\"; ln -s #{outside} \"$quarantine\""

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        workspace_durability_remote_url: source,
        hook_before_remove: swap_command
      )

      assert {:error, :workspace_preservation_required, ""} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      assert File.read!(Path.join(workspace, "README.md")) == "durable baseline\n"
      assert File.read!(sentinel) == "outside data\n"
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup rejects a real-directory quarantine swap" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-quarantine-directory-swap-#{System.unique_integer([:positive])}"
      )

    source = Path.join(test_root, "source")
    replacement_source = Path.join(test_root, "replacement-source")
    workspace_root = Path.join(test_root, "workspaces")
    workspace = Path.join(workspace_root, "DUD-157")
    saved = Path.join(test_root, "saved-original")

    try do
      File.mkdir_p!(source)
      File.write!(Path.join(source, "README.md"), "durable baseline\n")
      System.cmd("git", ["-C", source, "init", "-b", "main"])
      System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", source, "add", "README.md"])
      System.cmd("git", ["-C", source, "commit", "-m", "durable baseline"])
      File.write!(Path.join(source, "README.md"), "attacker replacement\n")
      System.cmd("git", ["-C", source, "commit", "-am", "replacement"])
      System.cmd("git", ["clone", "--bare", source, replacement_source])
      System.cmd("git", ["-C", source, "reset", "--hard", "HEAD~1"])
      File.mkdir_p!(workspace_root)
      System.cmd("git", ["clone", source, workspace])

      swap_command =
        "quarantine=$PWD; cd ..; mv \"$quarantine\" #{saved}; " <>
          "git clone #{replacement_source} \"$quarantine\" >/dev/null 2>&1"

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        workspace_durability_remote_url: source,
        hook_before_remove: swap_command
      )

      assert {:error, :workspace_preservation_required, ""} =
               Workspace.remove_exact_if_durable(workspace, workspace_root, nil)

      assert File.read!(Path.join(saved, "README.md")) == "durable baseline\n"
      refute File.exists?(workspace)

      assert File.read!(Path.join(workspace <> ".symphony-cleanup", "README.md")) ==
               "attacker replacement\n"
    after
      File.rm_rf(test_root)
    end
  end

  test "workspace surfaces after_create hook failures" do
    workspace_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-workspace-hook-failure-#{System.unique_integer([:positive])}"
      )

    try do
      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        hook_after_create: "echo nope && exit 17"
      )

      assert {:error, {:workspace_hook_failed, "after_create", 17, _output}} =
               Workspace.create_for_issue("MT-FAIL")
    after
      File.rm_rf(workspace_root)
    end
  end

  test "workspace surfaces after_create hook timeouts" do
    workspace_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-workspace-hook-timeout-#{System.unique_integer([:positive])}"
      )

    try do
      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        hook_timeout_ms: 10,
        hook_after_create: "sleep 1"
      )

      assert {:error, {:workspace_hook_timeout, "after_create", 10}} =
               Workspace.create_for_issue("MT-TIMEOUT")
    after
      File.rm_rf(workspace_root)
    end
  end

  test "workspace creates an empty directory when no bootstrap hook is configured" do
    workspace_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-workspace-empty-#{System.unique_integer([:positive])}"
      )

    try do
      write_workflow_file!(Workflow.workflow_file_path(), workspace_root: workspace_root)

      workspace = Path.join(workspace_root, "MT-608")
      assert {:ok, canonical_workspace} = SymphonyElixir.PathSafety.canonicalize(workspace)

      assert {:ok, ^canonical_workspace} = Workspace.create_for_issue("MT-608")
      assert File.dir?(workspace)
      assert {:ok, []} = File.ls(workspace)
    after
      File.rm_rf(workspace_root)
    end
  end

  test "workspace removes all workspaces for a closed issue identifier" do
    workspace_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-issue-workspace-cleanup-#{System.unique_integer([:positive])}"
      )

    try do
      target_workspace = Path.join(workspace_root, "S_1")
      untouched_workspace = Path.join(workspace_root, "OTHER-#{System.unique_integer([:positive])}")

      File.mkdir_p!(target_workspace)
      File.mkdir_p!(untouched_workspace)
      File.write!(Path.join(target_workspace, "marker.txt"), "stale")
      File.write!(Path.join(untouched_workspace, "marker.txt"), "keep")

      write_workflow_file!(Workflow.workflow_file_path(), workspace_root: workspace_root)

      assert :ok = Workspace.remove_issue_workspaces("S_1")
      refute File.exists?(target_workspace)
      assert File.exists?(untouched_workspace)
    after
      File.rm_rf(workspace_root)
    end
  end

  test "workspace cleanup handles missing workspace root" do
    missing_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-missing-workspaces-#{System.unique_integer([:positive])}"
      )

    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: missing_root)

    assert :ok = Workspace.remove_issue_workspaces("S-2")
  end

  test "workspace cleanup ignores non-binary identifier" do
    assert :ok = Workspace.remove_issue_workspaces(nil)
  end

  test "linear issue helpers" do
    issue = %Issue{
      id: "abc",
      labels: ["frontend", "infra"],
      assigned_to_worker: false
    }

    assert Issue.label_names(issue) == ["frontend", "infra"]
    assert issue.labels == ["frontend", "infra"]
    refute issue.assigned_to_worker
  end

  test "linear client normalizes blockers from inverse relations" do
    raw_issue = %{
      "id" => "issue-1",
      "identifier" => "MT-1",
      "title" => "Blocked todo",
      "description" => "Needs dependency",
      "priority" => 2,
      "state" => %{"name" => "Todo"},
      "branchName" => "mt-1",
      "url" => "https://example.org/issues/MT-1",
      "assignee" => %{
        "id" => "user-1"
      },
      "labels" => %{"nodes" => [%{"name" => "Backend"}]},
      "inverseRelations" => %{
        "nodes" => [
          %{
            "type" => "blocks",
            "issue" => %{
              "id" => "issue-2",
              "identifier" => "MT-2",
              "state" => %{"name" => "In Progress"}
            }
          },
          %{
            "type" => "relatesTo",
            "issue" => %{
              "id" => "issue-3",
              "identifier" => "MT-3",
              "state" => %{"name" => "Done"}
            }
          }
        ]
      },
      "createdAt" => "2026-01-01T00:00:00Z",
      "updatedAt" => "2026-01-02T00:00:00Z"
    }

    issue = Client.normalize_issue_for_test(raw_issue, "user-1")

    assert issue.blocked_by == [%{id: "issue-2", identifier: "MT-2", state: "In Progress"}]
    assert issue.labels == ["backend"]
    assert issue.priority == 2
    assert issue.state == "Todo"
    assert issue.assignee_id == "user-1"
    assert issue.assigned_to_worker
  end

  test "linear client marks explicitly unassigned issues as not routed to worker" do
    raw_issue = %{
      "id" => "issue-99",
      "identifier" => "MT-99",
      "title" => "Someone else's task",
      "state" => %{"name" => "Todo"},
      "assignee" => %{
        "id" => "user-2"
      }
    }

    issue = Client.normalize_issue_for_test(raw_issue, "user-1")

    refute issue.assigned_to_worker
  end

  test "linear client pagination merge helper preserves issue ordering" do
    issue_page_1 = [
      %Issue{id: "issue-1", identifier: "MT-1"},
      %Issue{id: "issue-2", identifier: "MT-2"}
    ]

    issue_page_2 = [
      %Issue{id: "issue-3", identifier: "MT-3"}
    ]

    merged = Client.merge_issue_pages_for_test([issue_page_1, issue_page_2])

    assert Enum.map(merged, & &1.identifier) == ["MT-1", "MT-2", "MT-3"]
  end

  test "linear client paginates issue state fetches by id beyond one page" do
    issue_ids = Enum.map(1..55, &"issue-#{&1}")
    first_batch_ids = Enum.take(issue_ids, 50)
    second_batch_ids = Enum.drop(issue_ids, 50)

    raw_issue = fn issue_id ->
      suffix = String.replace_prefix(issue_id, "issue-", "")

      %{
        "id" => issue_id,
        "identifier" => "MT-#{suffix}",
        "title" => "Issue #{suffix}",
        "description" => "Description #{suffix}",
        "state" => %{"name" => "In Progress"},
        "labels" => %{"nodes" => []},
        "inverseRelations" => %{"nodes" => []}
      }
    end

    graphql_fun = fn query, variables ->
      send(self(), {:fetch_issue_states_page, query, variables})

      body = %{
        "data" => %{
          "issues" => %{
            "nodes" => Enum.map(variables.ids, raw_issue)
          }
        }
      }

      {:ok, body}
    end

    assert {:ok, issues} = Client.fetch_issue_states_by_ids_for_test(issue_ids, graphql_fun)

    assert Enum.map(issues, & &1.id) == issue_ids

    assert_receive {:fetch_issue_states_page, query, %{ids: ^first_batch_ids, first: 50, relationFirst: 50}}
    assert query =~ "SymphonyLinearIssuesById"

    assert_receive {:fetch_issue_states_page, ^query, %{ids: ^second_batch_ids, first: 5, relationFirst: 50}}
  end

  test "linear client revalidates tracker authority before every batched request" do
    issue_ids = Enum.map(1..55, &"issue-authority-#{&1}")

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "linear",
      tracker_endpoint: "https://approved.example/graphql",
      tracker_api_token: "approved-token",
      tracker_project_slug: "approved-project"
    )

    context = SymphonyElixir.Tracker.current_poll_context()
    workflow_path = Workflow.workflow_file_path()

    request_fun = fn payload, _headers ->
      request_number = Process.get(:authority_request_number, 0) + 1
      Process.put(:authority_request_number, request_number)
      send(self(), {:authority_request, request_number})

      if request_number == 1 do
        workflow_path
        |> File.read!()
        |> String.replace(
          "https://approved.example/graphql",
          "https://unapproved.example/graphql"
        )
        |> then(&File.write!(workflow_path, &1))
      end

      raw_issues =
        Enum.map(payload["variables"].ids, fn issue_id ->
          %{
            "id" => issue_id,
            "identifier" => String.upcase(issue_id),
            "title" => issue_id,
            "state" => %{"name" => "In Progress"},
            "labels" => %{"nodes" => []},
            "inverseRelations" => %{"nodes" => []}
          }
        end)

      {:ok, %{status: 200, body: %{"data" => %{"issues" => %{"nodes" => raw_issues}}}}}
    end

    graphql_fun = fn query, variables ->
      Client.graphql(query, variables,
        tracker_context: context,
        request_fun: request_fun
      )
    end

    assert {:error, :tracker_authority_invalidated} =
             Client.fetch_issue_states_by_ids_for_test(issue_ids, graphql_fun)

    assert_receive {:authority_request, 1}
    refute_receive {:authority_request, 2}
  end

  test "linear client rejects a missing tracker context before network I/O" do
    assert {:error, :tracker_context_required} =
             Client.graphql(
               "query Viewer { viewer { id } }",
               %{},
               request_fun: fn _payload, _headers ->
                 flunk("request must not run without an admitted tracker context")
               end
             )
  end

  test "linear client logs response bodies for non-200 graphql responses" do
    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:error, {:linear_api_status, 400}} =
                 Client.graphql(
                   "query Viewer { viewer { id } }",
                   %{},
                   tracker_context: SymphonyElixir.Tracker.current_poll_context(),
                   request_fun: fn _payload, _headers ->
                     {:ok,
                      %{
                        status: 400,
                        body: %{
                          "errors" => [
                            %{
                              "message" => "Variable \"$ids\" got invalid value",
                              "extensions" => %{"code" => "BAD_USER_INPUT"}
                            }
                          ]
                        }
                      }}
                   end
                 )
      end)

    assert log =~ "Linear GraphQL request failed status=400"
    assert log =~ ~s(body=%{"errors" => [%{"extensions" => %{"code" => "BAD_USER_INPUT"})
    assert log =~ "Variable \\\"$ids\\\" got invalid value"
  end

  test "orchestrator sorts dispatch by priority then oldest created_at" do
    issue_same_priority_older = %Issue{
      id: "issue-old-high",
      identifier: "MT-200",
      title: "Old high priority",
      state: "Todo",
      priority: 1,
      created_at: ~U[2026-01-01 00:00:00Z]
    }

    issue_same_priority_newer = %Issue{
      id: "issue-new-high",
      identifier: "MT-201",
      title: "New high priority",
      state: "Todo",
      priority: 1,
      created_at: ~U[2026-01-02 00:00:00Z]
    }

    issue_lower_priority_older = %Issue{
      id: "issue-old-low",
      identifier: "MT-199",
      title: "Old lower priority",
      state: "Todo",
      priority: 2,
      created_at: ~U[2025-12-01 00:00:00Z]
    }

    sorted =
      Orchestrator.sort_issues_for_dispatch_for_test([
        issue_lower_priority_older,
        issue_same_priority_newer,
        issue_same_priority_older
      ])

    assert Enum.map(sorted, & &1.identifier) == ["MT-200", "MT-201", "MT-199"]
  end

  test "todo issue with non-terminal blocker is not dispatch-eligible" do
    state = %Orchestrator.State{
      max_concurrent_agents: 3,
      running: %{},
      claimed: MapSet.new(),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
      retry_attempts: %{}
    }

    issue = %Issue{
      id: "blocked-1",
      identifier: "MT-1001",
      title: "Blocked work",
      state: "Todo",
      blocked_by: [%{id: "blocker-1", identifier: "MT-1002", state: "In Progress"}]
    }

    refute Orchestrator.should_dispatch_issue_for_test(issue, state)
  end

  test "issue assigned to another worker is not dispatch-eligible" do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_assignee: "dev@example.com")

    state = %Orchestrator.State{
      max_concurrent_agents: 3,
      running: %{},
      claimed: MapSet.new(),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
      retry_attempts: %{}
    }

    issue = %Issue{
      id: "assigned-away-1",
      identifier: "MT-1007",
      title: "Owned elsewhere",
      state: "Todo",
      assigned_to_worker: false
    }

    refute Orchestrator.should_dispatch_issue_for_test(issue, state)
  end

  test "todo issue with terminal blockers remains dispatch-eligible" do
    state = %Orchestrator.State{
      max_concurrent_agents: 3,
      running: %{},
      claimed: MapSet.new(),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
      retry_attempts: %{}
    }

    issue = %Issue{
      id: "ready-1",
      identifier: "MT-1003",
      title: "Ready work",
      state: "Todo",
      blocked_by: [%{id: "blocker-2", identifier: "MT-1004", state: "Closed"}]
    }

    assert Orchestrator.should_dispatch_issue_for_test(issue, state)
  end

  test "dispatch revalidation skips stale todo issue once a non-terminal blocker appears" do
    stale_issue = %Issue{
      id: "blocked-2",
      identifier: "MT-1005",
      title: "Stale blocked work",
      state: "Todo",
      blocked_by: []
    }

    refreshed_issue = %Issue{
      id: "blocked-2",
      identifier: "MT-1005",
      title: "Stale blocked work",
      state: "Todo",
      blocked_by: [%{id: "blocker-3", identifier: "MT-1006", state: "In Progress"}]
    }

    fetcher = fn ["blocked-2"] -> {:ok, [refreshed_issue]} end

    assert {:skip, %Issue{} = skipped_issue} =
             Orchestrator.revalidate_issue_for_dispatch_for_test(stale_issue, fetcher)

    assert skipped_issue.identifier == "MT-1005"
    assert skipped_issue.blocked_by == [%{id: "blocker-3", identifier: "MT-1006", state: "In Progress"}]
  end

  test "workspace remove returns error information for missing directory" do
    random_path =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-missing-#{System.unique_integer([:positive])}"
      )

    assert {:ok, []} = Workspace.remove(random_path)
  end

  test "workspace hooks support multiline YAML scripts and run at lifecycle boundaries" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-workspace-hooks-#{System.unique_integer([:positive])}"
      )

    try do
      workspace_root = Path.join(test_root, "workspaces")
      before_remove_marker = Path.join(test_root, "before_remove.log")
      after_create_counter = Path.join(test_root, "after_create.count")

      File.mkdir_p!(workspace_root)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        hook_after_create: "echo after_create > after_create.log\necho call >> \"#{after_create_counter}\"",
        hook_before_remove: "echo before_remove > \"#{before_remove_marker}\""
      )

      config = Config.settings!()
      assert config.hooks.after_create =~ "echo after_create > after_create.log"
      assert config.hooks.before_remove =~ "echo before_remove >"

      assert {:ok, workspace} = Workspace.create_for_issue("MT-HOOKS")
      assert File.read!(Path.join(workspace, "after_create.log")) == "after_create\n"

      assert {:ok, _workspace} = Workspace.create_for_issue("MT-HOOKS")
      assert length(String.split(String.trim(File.read!(after_create_counter)), "\n")) == 1

      assert :ok = Workspace.remove_issue_workspaces("MT-HOOKS")
      assert File.read!(before_remove_marker) == "before_remove\n"
      refute File.exists?(workspace)
    after
      File.rm_rf(test_root)
    end
  end

  test "workspace remove continues when before_remove hook fails" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-workspace-hooks-fail-#{System.unique_integer([:positive])}"
      )

    try do
      workspace_root = Path.join(test_root, "workspaces")

      File.mkdir_p!(workspace_root)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        hook_before_remove: "echo failure && exit 17"
      )

      assert {:ok, workspace} = Workspace.create_for_issue("MT-HOOKS-FAIL")
      assert :ok = Workspace.remove_issue_workspaces("MT-HOOKS-FAIL")
      refute File.exists?(workspace)
    after
      File.rm_rf(test_root)
    end
  end

  test "workspace remove continues when before_remove hook fails with large output" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-workspace-hooks-large-fail-#{System.unique_integer([:positive])}"
      )

    try do
      workspace_root = Path.join(test_root, "workspaces")

      File.mkdir_p!(workspace_root)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        hook_before_remove: "i=0; while [ $i -lt 3000 ]; do printf a; i=$((i+1)); done; exit 17"
      )

      assert {:ok, workspace} = Workspace.create_for_issue("MT-HOOKS-LARGE-FAIL")
      assert :ok = Workspace.remove_issue_workspaces("MT-HOOKS-LARGE-FAIL")
      refute File.exists?(workspace)
    after
      File.rm_rf(test_root)
    end
  end

  test "workspace remove continues when before_remove hook times out" do
    previous_timeout = Application.get_env(:symphony_elixir, :workspace_hook_timeout_ms)

    on_exit(fn ->
      if is_nil(previous_timeout) do
        Application.delete_env(:symphony_elixir, :workspace_hook_timeout_ms)
      else
        Application.put_env(:symphony_elixir, :workspace_hook_timeout_ms, previous_timeout)
      end
    end)

    Application.put_env(:symphony_elixir, :workspace_hook_timeout_ms, 10)

    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-workspace-hooks-timeout-#{System.unique_integer([:positive])}"
      )

    try do
      workspace_root = Path.join(test_root, "workspaces")

      File.mkdir_p!(workspace_root)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        hook_before_remove: "sleep 1"
      )

      assert {:ok, workspace} = Workspace.create_for_issue("MT-HOOKS-TIMEOUT")
      assert :ok = Workspace.remove_issue_workspaces("MT-HOOKS-TIMEOUT")
      refute File.exists?(workspace)
    after
      File.rm_rf(test_root)
    end
  end

  test "config reads defaults for optional settings" do
    previous_linear_api_key = System.get_env("LINEAR_API_KEY")
    on_exit(fn -> restore_env("LINEAR_API_KEY", previous_linear_api_key) end)
    System.delete_env("LINEAR_API_KEY")

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: nil,
      max_concurrent_agents: nil,
      codex_approval_policy: nil,
      codex_thread_sandbox: nil,
      codex_turn_sandbox_policy: nil,
      codex_turn_timeout_ms: nil,
      codex_read_timeout_ms: nil,
      codex_stall_timeout_ms: nil,
      tracker_api_token: nil,
      tracker_project_slug: nil
    )

    config = Config.settings!()
    assert config.tracker.endpoint == "https://api.linear.app/graphql"
    assert config.tracker.api_key == nil
    assert config.tracker.project_slug == nil
    assert config.tracker.operator_user_ids == []
    assert config.workspace.root == Path.join(System.tmp_dir!(), "symphony_workspaces")
    assert config.worker.max_concurrent_agents_per_host == nil
    assert config.agent.max_concurrent_agents == 10
    assert config.codex.command == "codex app-server"

    assert config.codex.approval_policy == %{
             "reject" => %{
               "sandbox_approval" => true,
               "rules" => true,
               "mcp_elicitations" => true
             }
           }

    assert config.codex.thread_sandbox == "workspace-write"

    assert {:ok, canonical_default_workspace_root} =
             SymphonyElixir.PathSafety.canonicalize(Path.join(System.tmp_dir!(), "symphony_workspaces"))

    assert Config.codex_turn_sandbox_policy() == %{
             "type" => "workspaceWrite",
             "writableRoots" => [canonical_default_workspace_root],
             "readOnlyAccess" => %{"type" => "fullAccess"},
             "networkAccess" => false,
             "excludeTmpdirEnvVar" => false,
             "excludeSlashTmp" => false
           }

    assert config.codex.turn_timeout_ms == 3_600_000
    assert config.codex.read_timeout_ms == 5_000
    assert config.codex.stall_timeout_ms == 300_000

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_operator_user_ids: ["operator-1", "operator-2"]
    )

    assert Config.settings!().tracker.operator_user_ids == ["operator-1", "operator-2"]

    write_workflow_file!(Workflow.workflow_file_path(), tracker_operator_user_ids: [""])
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "tracker.operator_user_ids"

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_command: "codex --config 'model=\"gpt-5.5\"' app-server"
    )

    assert Config.settings!().codex.command ==
             "codex --config 'model=\"gpt-5.5\"' app-server"

    explicit_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-explicit-sandbox-root-#{System.unique_integer([:positive])}"
      )

    explicit_workspace = Path.join(explicit_root, "MT-EXPLICIT")
    explicit_cache = Path.join(explicit_workspace, "cache")
    File.mkdir_p!(explicit_cache)

    on_exit(fn -> File.rm_rf(explicit_root) end)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: explicit_root,
      codex_approval_policy: "on-request",
      codex_thread_sandbox: "workspace-write",
      codex_turn_sandbox_policy: %{
        type: "workspaceWrite",
        writableRoots: [explicit_workspace, explicit_cache]
      }
    )

    config = Config.settings!()
    assert config.codex.approval_policy == "on-request"
    assert config.codex.thread_sandbox == "workspace-write"

    assert Config.codex_turn_sandbox_policy(explicit_workspace) == %{
             "type" => "workspaceWrite",
             "writableRoots" => [explicit_workspace, explicit_cache]
           }

    write_workflow_file!(Workflow.workflow_file_path(), tracker_active_states: ",")
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "tracker.active_states"

    write_workflow_file!(Workflow.workflow_file_path(), max_concurrent_agents: "bad")
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "agent.max_concurrent_agents"

    write_workflow_file!(Workflow.workflow_file_path(), worker_max_concurrent_agents_per_host: 0)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "worker.max_concurrent_agents_per_host"

    write_workflow_file!(Workflow.workflow_file_path(), codex_turn_timeout_ms: "bad")
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.turn_timeout_ms"

    write_workflow_file!(Workflow.workflow_file_path(), codex_read_timeout_ms: "bad")
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.read_timeout_ms"

    write_workflow_file!(Workflow.workflow_file_path(), codex_stall_timeout_ms: "bad")
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.stall_timeout_ms"

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_active_states: %{todo: true},
      tracker_terminal_states: %{done: true},
      poll_interval_ms: %{bad: true},
      workspace_root: 123,
      max_retry_backoff_ms: 0,
      max_concurrent_agents_by_state: %{"Todo" => "1", "Review" => 0, "Done" => "bad"},
      hook_timeout_ms: 0,
      observability_enabled: "maybe",
      observability_refresh_ms: %{bad: true},
      observability_render_interval_ms: %{bad: true},
      server_port: -1,
      server_host: 123
    )

    assert {:error, {:invalid_workflow_config, _message}} = Config.validate!()

    write_workflow_file!(Workflow.workflow_file_path(), codex_approval_policy: "")
    assert :ok = Config.validate!()
    assert Config.settings!().codex.approval_policy == ""

    write_workflow_file!(Workflow.workflow_file_path(), codex_thread_sandbox: "")
    assert :ok = Config.validate!()
    assert Config.settings!().codex.thread_sandbox == ""

    write_workflow_file!(Workflow.workflow_file_path(), codex_turn_sandbox_policy: "bad")
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.turn_sandbox_policy"

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_approval_policy: "future-policy",
      codex_thread_sandbox: "future-sandbox",
      codex_turn_sandbox_policy: %{
        type: "futureSandbox",
        nested: %{flag: true}
      }
    )

    config = Config.settings!()
    assert config.codex.approval_policy == "future-policy"
    assert config.codex.thread_sandbox == "future-sandbox"

    assert :ok = Config.validate!()

    assert Config.codex_turn_sandbox_policy() == %{
             "type" => "futureSandbox",
             "nested" => %{"flag" => true}
           }

    write_workflow_file!(Workflow.workflow_file_path(), codex_command: "codex app-server")
    assert Config.settings!().codex.command == "codex app-server"
  end

  test "config resolves $VAR references for env-backed secret and path values" do
    workspace_env_var = "SYMP_WORKSPACE_ROOT_#{System.unique_integer([:positive])}"
    api_key_env_var = "SYMP_LINEAR_API_KEY_#{System.unique_integer([:positive])}"
    workspace_root = Path.join("/tmp", "symphony-workspace-root")
    api_key = "resolved-secret"
    codex_bin = Path.join(["~", "bin", "codex"])

    previous_workspace_root = System.get_env(workspace_env_var)
    previous_api_key = System.get_env(api_key_env_var)

    System.put_env(workspace_env_var, workspace_root)
    System.put_env(api_key_env_var, api_key)

    on_exit(fn ->
      restore_env(workspace_env_var, previous_workspace_root)
      restore_env(api_key_env_var, previous_api_key)
    end)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_api_token: "$#{api_key_env_var}",
      workspace_root: "$#{workspace_env_var}",
      codex_command: "#{codex_bin} app-server"
    )

    config = Config.settings!()
    assert config.tracker.api_key == api_key
    assert config.workspace.root == Path.expand(workspace_root)
    assert config.codex.command == "#{codex_bin} app-server"
  end

  test "config no longer resolves legacy env: references" do
    workspace_env_var = "SYMP_WORKSPACE_ROOT_#{System.unique_integer([:positive])}"
    api_key_env_var = "SYMP_LINEAR_API_KEY_#{System.unique_integer([:positive])}"
    workspace_root = Path.join("/tmp", "symphony-workspace-root")
    api_key = "resolved-secret"

    previous_workspace_root = System.get_env(workspace_env_var)
    previous_api_key = System.get_env(api_key_env_var)

    System.put_env(workspace_env_var, workspace_root)
    System.put_env(api_key_env_var, api_key)

    on_exit(fn ->
      restore_env(workspace_env_var, previous_workspace_root)
      restore_env(api_key_env_var, previous_api_key)
    end)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_api_token: "env:#{api_key_env_var}",
      workspace_root: "env:#{workspace_env_var}"
    )

    config = Config.settings!()
    assert config.tracker.api_key == "env:#{api_key_env_var}"
    assert config.workspace.root == "env:#{workspace_env_var}"
  end

  test "config supports per-state max concurrent agent overrides" do
    workflow = """
    ---
    workflow:
      runtime_prompt_mode: full_prompt_compat
    agent:
      max_concurrent_agents: 10
      max_concurrent_agents_by_state:
        todo: 1
        "In Progress": 4
        "In Review": 2
    ---
    """

    File.write!(Workflow.workflow_file_path(), workflow)

    assert Config.settings!().agent.max_concurrent_agents == 10
    assert Config.max_concurrent_agents_for_state("Todo") == 1
    assert Config.max_concurrent_agents_for_state("In Progress") == 4
    assert Config.max_concurrent_agents_for_state("In Review") == 2
    assert Config.max_concurrent_agents_for_state("Closed") == 10
    assert Config.max_concurrent_agents_for_state(:not_a_string) == 10

    write_workflow_file!(Workflow.workflow_file_path(), worker_max_concurrent_agents_per_host: 2)
    assert :ok = Config.validate!()
    assert Config.settings!().worker.max_concurrent_agents_per_host == 2
  end

  test "schema helpers cover custom type and state limit validation" do
    assert StringOrMap.type() == :map
    assert StringOrMap.embed_as(:json) == :self
    assert StringOrMap.equal?(%{"a" => 1}, %{"a" => 1})
    refute StringOrMap.equal?(%{"a" => 1}, %{"a" => 2})

    assert {:ok, "value"} = StringOrMap.cast("value")
    assert {:ok, %{"a" => 1}} = StringOrMap.cast(%{"a" => 1})
    assert :error = StringOrMap.cast(123)

    assert {:ok, "value"} = StringOrMap.load("value")
    assert :error = StringOrMap.load(123)

    assert {:ok, %{"a" => 1}} = StringOrMap.dump(%{"a" => 1})
    assert :error = StringOrMap.dump(123)

    assert Schema.normalize_state_limits(nil) == %{}

    assert Schema.normalize_state_limits(%{"In Progress" => 2, todo: 1}) == %{
             "todo" => 1,
             "in progress" => 2
           }

    changeset =
      {%{}, %{limits: :map}}
      |> Changeset.cast(%{limits: %{"" => 1, "todo" => 0}}, [:limits])
      |> Schema.validate_state_limits(:limits)

    assert changeset.errors == [
             limits: {"state names must not be blank", []},
             limits: {"limits must be positive integers", []}
           ]
  end

  test "schema parse normalizes policy keys and env-backed fallbacks" do
    missing_workspace_env = "SYMP_MISSING_WORKSPACE_#{System.unique_integer([:positive])}"
    empty_secret_env = "SYMP_EMPTY_SECRET_#{System.unique_integer([:positive])}"
    missing_secret_env = "SYMP_MISSING_SECRET_#{System.unique_integer([:positive])}"

    previous_missing_workspace_env = System.get_env(missing_workspace_env)
    previous_empty_secret_env = System.get_env(empty_secret_env)
    previous_missing_secret_env = System.get_env(missing_secret_env)
    previous_linear_api_key = System.get_env("LINEAR_API_KEY")

    System.delete_env(missing_workspace_env)
    System.put_env(empty_secret_env, "")
    System.delete_env(missing_secret_env)
    System.put_env("LINEAR_API_KEY", "fallback-linear-token")

    on_exit(fn ->
      restore_env(missing_workspace_env, previous_missing_workspace_env)
      restore_env(empty_secret_env, previous_empty_secret_env)
      restore_env(missing_secret_env, previous_missing_secret_env)
      restore_env("LINEAR_API_KEY", previous_linear_api_key)
    end)

    assert {:ok, settings} =
             Schema.parse(%{
               tracker: %{api_key: "$#{empty_secret_env}"},
               workspace: %{root: "$#{missing_workspace_env}"},
               codex: %{approval_policy: %{reject: %{sandbox_approval: true}}}
             })

    assert settings.tracker.api_key == nil
    assert settings.workspace.root == Path.join(System.tmp_dir!(), "symphony_workspaces")

    assert settings.codex.approval_policy == %{
             "reject" => %{"sandbox_approval" => true}
           }

    assert {:ok, settings} =
             Schema.parse(%{
               tracker: %{api_key: "$#{missing_secret_env}"},
               workspace: %{root: ""}
             })

    assert settings.tracker.api_key == "fallback-linear-token"
    assert settings.workspace.root == Path.join(System.tmp_dir!(), "symphony_workspaces")
  end

  test "schema resolves sandbox policies from explicit and default workspaces" do
    explicit_policy = %{"type" => "workspaceWrite", "writableRoots" => ["/tmp/explicit"]}

    assert Schema.resolve_turn_sandbox_policy(%Schema{
             codex: %Codex{turn_sandbox_policy: explicit_policy},
             workspace: %Schema.Workspace{root: "/tmp/ignored"}
           }) == explicit_policy

    assert Schema.resolve_turn_sandbox_policy(%Schema{
             codex: %Codex{turn_sandbox_policy: nil},
             workspace: %Schema.Workspace{root: ""}
           }) == %{
             "type" => "workspaceWrite",
             "writableRoots" => [Path.expand(Path.join(System.tmp_dir!(), "symphony_workspaces"))],
             "readOnlyAccess" => %{"type" => "fullAccess"},
             "networkAccess" => false,
             "excludeTmpdirEnvVar" => false,
             "excludeSlashTmp" => false
           }

    assert Schema.resolve_turn_sandbox_policy(
             %Schema{
               codex: %Codex{turn_sandbox_policy: nil},
               workspace: %Schema.Workspace{root: "/tmp/ignored"}
             },
             "/tmp/workspace"
           ) == %{
             "type" => "workspaceWrite",
             "writableRoots" => [Path.expand("/tmp/workspace")],
             "readOnlyAccess" => %{"type" => "fullAccess"},
             "networkAccess" => false,
             "excludeTmpdirEnvVar" => false,
             "excludeSlashTmp" => false
           }
  end

  test "schema keeps workspace roots raw while sandbox helpers expand only for local use" do
    assert {:ok, settings} =
             Schema.parse(%{
               workspace: %{root: "~/.symphony-workspaces"},
               codex: %{}
             })

    assert settings.workspace.root == "~/.symphony-workspaces"

    assert Schema.resolve_turn_sandbox_policy(settings) == %{
             "type" => "workspaceWrite",
             "writableRoots" => [Path.expand("~/.symphony-workspaces")],
             "readOnlyAccess" => %{"type" => "fullAccess"},
             "networkAccess" => false,
             "excludeTmpdirEnvVar" => false,
             "excludeSlashTmp" => false
           }

    assert {:ok, remote_policy} =
             Schema.resolve_runtime_turn_sandbox_policy(settings, nil, remote: true)

    assert remote_policy == %{
             "type" => "workspaceWrite",
             "writableRoots" => ["~/.symphony-workspaces"],
             "readOnlyAccess" => %{"type" => "fullAccess"},
             "networkAccess" => false,
             "excludeTmpdirEnvVar" => false,
             "excludeSlashTmp" => false
           }
  end

  test "runtime sandbox policy resolution passes explicit policies through unchanged" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-runtime-sandbox-#{System.unique_integer([:positive])}"
      )

    try do
      workspace_root = Path.join(test_root, "workspaces")
      issue_workspace = Path.join(workspace_root, "MT-100")
      File.mkdir_p!(issue_workspace)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        codex_turn_sandbox_policy: %{
          type: "workspaceWrite",
          writableRoots: ["relative/path"],
          networkAccess: true
        }
      )

      assert {:ok, runtime_settings} = Config.codex_runtime_settings(issue_workspace)

      assert runtime_settings.turn_sandbox_policy == %{
               "type" => "workspaceWrite",
               "writableRoots" => ["relative/path"],
               "networkAccess" => true
             }

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        codex_turn_sandbox_policy: %{
          type: "futureSandbox",
          nested: %{flag: true}
        }
      )

      assert {:ok, runtime_settings} = Config.codex_runtime_settings(issue_workspace)

      assert runtime_settings.turn_sandbox_policy == %{
               "type" => "futureSandbox",
               "nested" => %{"flag" => true}
             }
    after
      File.rm_rf(test_root)
    end
  end

  test "path safety returns errors for invalid path segments" do
    invalid_segment = String.duplicate("a", 300)
    path = Path.join(System.tmp_dir!(), invalid_segment)
    expanded_path = Path.expand(path)

    assert {:error, {:path_canonicalize_failed, ^expanded_path, :enametoolong}} =
             SymphonyElixir.PathSafety.canonicalize(path)
  end

  test "runtime sandbox policy resolution defaults when omitted and ignores workspace for explicit policies" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-runtime-sandbox-branches-#{System.unique_integer([:positive])}"
      )

    try do
      workspace_root = Path.join(test_root, "workspaces")
      issue_workspace = Path.join(workspace_root, "MT-101")

      File.mkdir_p!(issue_workspace)

      write_workflow_file!(Workflow.workflow_file_path(), workspace_root: workspace_root)

      settings = Config.settings!()

      assert {:ok, canonical_workspace_root} =
               SymphonyElixir.PathSafety.canonicalize(workspace_root)

      assert {:ok, default_policy} = Schema.resolve_runtime_turn_sandbox_policy(settings)
      assert default_policy["type"] == "workspaceWrite"
      assert default_policy["writableRoots"] == [canonical_workspace_root]

      assert {:ok, blank_workspace_policy} =
               Schema.resolve_runtime_turn_sandbox_policy(settings, "")

      assert blank_workspace_policy == default_policy

      read_only_settings = %{
        settings
        | codex: %{settings.codex | turn_sandbox_policy: %{"type" => "readOnly", "networkAccess" => true}}
      }

      assert {:ok, %{"type" => "readOnly", "networkAccess" => true}} =
               Schema.resolve_runtime_turn_sandbox_policy(read_only_settings, 123)

      future_settings = %{
        settings
        | codex: %{settings.codex | turn_sandbox_policy: %{"type" => "futureSandbox", "nested" => %{"flag" => true}}}
      }

      assert {:ok, %{"type" => "futureSandbox", "nested" => %{"flag" => true}}} =
               Schema.resolve_runtime_turn_sandbox_policy(future_settings, 123)

      assert {:error, {:unsafe_turn_sandbox_policy, {:invalid_workspace_root, 123}}} =
               Schema.resolve_runtime_turn_sandbox_policy(settings, 123)
    after
      File.rm_rf(test_root)
    end
  end

  test "workflow prompt is used when building base prompt" do
    workflow_prompt = "Workflow prompt body used as codex instruction."

    write_workflow_file!(Workflow.workflow_file_path(), prompt: workflow_prompt)
    assert Config.workflow_prompt() == workflow_prompt
  end

  test "remote workspace lifecycle uses ssh host aliases from worker config" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-remote-workspace-#{System.unique_integer([:positive])}"
      )

    previous_path = System.get_env("PATH")
    previous_trace = System.get_env("SYMP_TEST_SSH_TRACE")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      restore_env("SYMP_TEST_SSH_TRACE", previous_trace)
    end)

    try do
      trace_file = Path.join(test_root, "ssh.trace")
      fake_ssh = Path.join(test_root, "ssh")
      workspace_root = "~/.symphony-remote-workspaces"
      canonical_workspace_root = "/remote/home/.symphony-remote-workspaces"
      workspace_path = "/remote/home/.symphony-remote-workspaces/MT-SSH-WS"

      File.mkdir_p!(test_root)
      System.put_env("SYMP_TEST_SSH_TRACE", trace_file)
      System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

      File.write!(fake_ssh, """
      #!/bin/sh
      trace_file="${SYMP_TEST_SSH_TRACE:-/tmp/symphony-fake-ssh.trace}"
      printf 'ARGV:%s\\n' "$*" >> "$trace_file"

      case "$*" in
        *"__SYMPHONY_AFFINITY__"*)
          printf '%s\\t%s\\t%s\\n' '__SYMPHONY_AFFINITY__' '#{canonical_workspace_root}' '#{workspace_path}'
          ;;
        *"__SYMPHONY_WORKSPACE__"*)
          printf '%s\\t%s\\t%s\\n' '__SYMPHONY_WORKSPACE__' '1' '#{workspace_path}'
          ;;
      esac

      exit 0
      """)

      File.chmod!(fake_ssh, 0o755)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        worker_ssh_hosts: ["worker-01:2200"],
        hook_before_run: "echo before-run",
        hook_after_run: "echo after-run",
        hook_before_remove: "echo before-remove"
      )

      assert Config.settings!().worker.ssh_hosts == ["worker-01:2200"]
      assert Config.settings!().workspace.root == workspace_root
      assert {:ok, ^workspace_path} = Workspace.create_for_issue("MT-SSH-WS", "worker-01:2200")
      assert :ok = Workspace.run_before_run_hook(workspace_path, "MT-SSH-WS", "worker-01:2200")
      assert :ok = Workspace.run_after_run_hook(workspace_path, "MT-SSH-WS", "worker-01:2200")
      assert :ok = Workspace.remove_issue_workspaces("MT-SSH-WS", "worker-01:2200")

      trace = File.read!(trace_file)
      assert trace =~ "-p 2200 worker-01 /bin/bash --noprofile --norc -c"
      assert trace =~ "__SYMPHONY_AFFINITY__"
      assert trace =~ "__SYMPHONY_WORKSPACE__"
      assert trace =~ "~/.symphony-remote-workspaces/MT-SSH-WS"
      assert trace =~ "${workspace#~/}"
      assert trace =~ "echo before-run"
      assert trace =~ "echo after-run"
      assert trace =~ "echo before-remove"
      assert trace =~ "rm -rf"
      assert trace =~ workspace_path
    after
      File.rm_rf(test_root)
    end
  end

  test "automatic terminal cleanup preserves a non-durable remote workspace" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-remote-preservation-#{System.unique_integer([:positive])}"
      )

    worker_host = "worker-preserve"
    remote_root = "/remote/workspaces"
    remote_workspace = remote_root <> "/DUD-152"
    trace_file = Path.join(test_root, "ssh.trace")
    fake_ssh = Path.join(test_root, "ssh")
    previous_path = System.get_env("PATH")
    previous_trace = System.get_env("SYMP_TEST_SSH_TRACE")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      restore_env("SYMP_TEST_SSH_TRACE", previous_trace)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(test_root)
    System.put_env("SYMP_TEST_SSH_TRACE", trace_file)
    System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

    File.write!(fake_ssh, """
    #!/bin/sh
    trace_file="${SYMP_TEST_SSH_TRACE}"

    case "$*" in
      *"__SYMPHONY_AFFINITY__"*)
        printf '%s\n' 'AFFINITY' >> "$trace_file"
        printf '%s\t%s\t%s\n' '__SYMPHONY_AFFINITY__' '#{remote_root}' '#{remote_workspace}'
        ;;
      *"__SYMPHONY_DURABILITY__"*)
        printf '%s\n' 'PRESERVATION_REQUIRED' >> "$trace_file"
        exit 75
        ;;
      *"rm -rf"*)
        printf '%s\n' 'CLEANUP' >> "$trace_file"
        ;;
    esac

    exit 0
    """)

    File.chmod!(fake_ssh, 0o755)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: remote_root,
      workspace_durability_remote_url: "git@example.test:repo.git"
    )

    assert {:error, :workspace_preservation_required, ""} =
             Workspace.remove_exact_if_durable(remote_workspace, remote_root, worker_host)

    trace = File.read!(trace_file)
    assert trace =~ "PRESERVATION_REQUIRED"
    refute trace =~ "CLEANUP"
  end

  test "automatic terminal cleanup treats remote durability timeout as preservation-required" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-remote-preservation-timeout-#{System.unique_integer([:positive])}"
      )

    worker_host = "worker-preserve-timeout"
    remote_root = "/remote/workspaces"
    remote_workspace = remote_root <> "/DUD-152"
    fake_ssh = Path.join(test_root, "ssh")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(test_root)
    System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

    File.write!(fake_ssh, """
    #!/bin/sh
    case "$*" in
      *"__SYMPHONY_AFFINITY__"*)
        printf '%s\t%s\t%s\n' '__SYMPHONY_AFFINITY__' '#{remote_root}' '#{remote_workspace}'
        ;;
      *"__SYMPHONY_DURABILITY__"*)
        sleep 3
        ;;
    esac
    """)

    File.chmod!(fake_ssh, 0o755)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: remote_root,
      workspace_durability_remote_url: "git@example.test:repo.git",
      hook_timeout_ms: 1_500
    )

    assert {:error, :workspace_preservation_required, ""} =
             Workspace.remove_exact_if_durable(remote_workspace, remote_root, worker_host)
  end

  test "remote terminal cleanup restores a workspace dirtied by before_remove" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-remote-post-hook-preservation-#{System.unique_integer([:positive])}"
      )

    worker_host = "worker-post-hook"
    source = Path.join(test_root, "source")
    remote_root = Path.join(test_root, "remote-workspaces")
    remote_workspace = Path.join(remote_root, "DUD-156")
    fake_ssh = Path.join(test_root, "ssh")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(source)
    File.write!(Path.join(source, "README.md"), "durable baseline\n")
    System.cmd("git", ["-C", source, "init", "-b", "main"])
    System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
    System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
    System.cmd("git", ["-C", source, "add", "README.md"])
    System.cmd("git", ["-C", source, "commit", "-m", "durable baseline"])
    File.mkdir_p!(remote_root)
    System.cmd("git", ["clone", source, remote_workspace])
    System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

    File.write!(fake_ssh, """
    #!/bin/sh
    case "$*" in
      *"__SYMPHONY_AFFINITY__"*)
        printf '%s\t%s\t%s\n' '__SYMPHONY_AFFINITY__' '#{remote_root}' '#{remote_workspace}'
        ;;
      *)
        for last_arg do :; done
        exec sh -lc "$last_arg"
        ;;
    esac
    """)

    File.chmod!(fake_ssh, 0o755)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: remote_root,
      workspace_durability_remote_url: source,
      hook_before_remove: "printf generated > post-hook.txt"
    )

    assert {:error, :workspace_preservation_required, ""} =
             Workspace.remove_exact_if_durable(remote_workspace, remote_root, worker_host)

    assert File.read!(Path.join(remote_workspace, "post-hook.txt")) == "generated"
    assert File.read!(Path.join(remote_workspace, "README.md")) == "durable baseline\n"
  end

  test "remote terminal cleanup executes retained quarantine contract" do
    {:ok, canonical_tmp} = SymphonyElixir.PathSafety.canonicalize(System.tmp_dir!())

    test_root =
      Path.join(
        canonical_tmp,
        "symphony-elixir-remote-retained-quarantine-#{System.unique_integer([:positive])}"
      )

    worker_host = "worker-retained-quarantine"
    source = Path.join(test_root, "source")
    remote_root = Path.join(test_root, "remote-workspaces")
    remote_workspace = Path.join(remote_root, "DUD-158")
    quarantine = remote_workspace <> ".symphony-cleanup"
    fake_ssh = Path.join(test_root, "ssh")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(source)
    File.write!(Path.join(source, "README.md"), "durable remote bytes\n")
    System.cmd("git", ["-C", source, "init", "-b", "main"])
    System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
    System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
    System.cmd("git", ["-C", source, "add", "README.md"])
    System.cmd("git", ["-C", source, "commit", "-m", "durable baseline"])
    File.mkdir_p!(remote_root)
    System.cmd("git", ["clone", source, remote_workspace])
    original_inode = File.stat!(remote_workspace).inode
    System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

    File.write!(fake_ssh, """
    #!/bin/sh
    for last_arg do :; done
    exec sh -lc "$last_arg"
    """)

    File.chmod!(fake_ssh, 0o755)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: remote_root,
      workspace_durability_remote_url: source
    )

    assert {:ok, []} =
             Workspace.remove_exact_if_durable(remote_workspace, remote_root, worker_host)

    refute File.exists?(remote_workspace)
    assert File.stat!(quarantine).inode == original_inode
    assert File.read!(Path.join(quarantine, "README.md")) == "durable remote bytes\n"
  end

  test "remote terminal cleanup ignores output from an unsupported stat dialect" do
    {:ok, canonical_tmp} = SymphonyElixir.PathSafety.canonicalize(System.tmp_dir!())

    test_root =
      Path.join(
        canonical_tmp,
        "symphony-elixir-remote-stat-dialect-#{System.unique_integer([:positive])}"
      )

    worker_host = "worker-stat-dialect"
    source = Path.join(test_root, "source")
    remote_root = Path.join(test_root, "remote-workspaces")
    remote_workspace = Path.join(remote_root, "DUD-158")
    quarantine = remote_workspace <> ".symphony-cleanup"
    fake_ssh = Path.join(test_root, "ssh")
    fake_stat = Path.join(test_root, "stat")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(source)
    File.write!(Path.join(source, "README.md"), "durable remote bytes\n")
    System.cmd("git", ["-C", source, "init", "-b", "main"])
    System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
    System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
    System.cmd("git", ["-C", source, "add", "README.md"])
    System.cmd("git", ["-C", source, "commit", "-m", "durable baseline"])
    File.mkdir_p!(remote_root)
    System.cmd("git", ["clone", source, remote_workspace])
    System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

    File.write!(fake_stat, """
    #!/bin/sh
    case "$1" in
      -c)
        printf '%s\n' '7:11'
        exit 0
        ;;
      -f)
        for last_arg do :; done
        printf 'unsupported-stat-output:%s\n' "$last_arg"
        exit 1
        ;;
      *)
        exit 64
        ;;
    esac
    """)

    File.write!(fake_ssh, """
    #!/bin/sh
    PATH='#{test_root}':"$PATH"
    export PATH
    for last_arg do :; done
    exec sh -c "$last_arg"
    """)

    File.chmod!(fake_stat, 0o755)
    File.chmod!(fake_ssh, 0o755)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: remote_root,
      workspace_durability_remote_url: source
    )

    assert {:ok, []} =
             Workspace.remove_exact_if_durable(remote_workspace, remote_root, worker_host)

    refute File.exists?(remote_workspace)
    assert File.read!(Path.join(quarantine, "README.md")) == "durable remote bytes\n"
  end

  test "remote terminal cleanup disables worker-controlled fsmonitor" do
    {:ok, canonical_tmp} = SymphonyElixir.PathSafety.canonicalize(System.tmp_dir!())

    test_root =
      Path.join(
        canonical_tmp,
        "symphony-elixir-remote-fsmonitor-#{System.unique_integer([:positive])}"
      )

    worker_host = "worker-fsmonitor"
    source = Path.join(test_root, "source")
    remote_root = Path.join(test_root, "remote-workspaces")
    remote_workspace = Path.join(remote_root, "DUD-158")
    fsmonitor = Path.join(test_root, "worker-fsmonitor")
    trace_file = Path.join(test_root, "fsmonitor.trace")
    fake_ssh = Path.join(test_root, "ssh")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(source)
    File.write!(Path.join(source, "README.md"), "durable remote bytes\n")
    System.cmd("git", ["-C", source, "init", "-b", "main"])
    System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
    System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
    System.cmd("git", ["-C", source, "add", "README.md"])
    System.cmd("git", ["-C", source, "commit", "-m", "durable baseline"])
    File.mkdir_p!(remote_root)
    System.cmd("git", ["clone", source, remote_workspace])

    File.write!(fsmonitor, "#!/bin/sh\nprintf invoked >> '#{trace_file}'\n")
    File.chmod!(fsmonitor, 0o755)
    System.cmd("git", ["-C", remote_workspace, "config", "core.fsmonitor", fsmonitor])

    System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

    File.write!(fake_ssh, """
    #!/bin/sh
    for last_arg do :; done
    exec sh -lc "$last_arg"
    """)

    File.chmod!(fake_ssh, 0o755)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: remote_root,
      workspace_durability_remote_url: source
    )

    assert {:ok, []} =
             Workspace.remove_exact_if_durable(remote_workspace, remote_root, worker_host)

    refute File.exists?(trace_file)
    assert File.dir?(remote_workspace <> ".symphony-cleanup")
  end

  test "remote terminal cleanup checks the decoded file URL path on the worker" do
    {:ok, canonical_tmp} = SymphonyElixir.PathSafety.canonicalize(System.tmp_dir!())

    test_root =
      Path.join(
        canonical_tmp,
        "symphony-elixir-remote-decoded-boundary-#{System.unique_integer([:positive])}"
      )

    worker_host = "worker-decoded-boundary"
    raw_source = Path.join(test_root, "%72epo.git")
    decoded_source = Path.join(test_root, "repo.git")
    remote_root = Path.join(test_root, "remote-workspaces")
    remote_workspace = Path.join(remote_root, "DUD-158")
    fake_ssh = Path.join(test_root, "ssh")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(raw_source)
    File.write!(Path.join(raw_source, "README.md"), "durable remote bytes\n")
    System.cmd("git", ["-C", raw_source, "init", "-b", "main"])
    System.cmd("git", ["-C", raw_source, "config", "user.name", "Test User"])
    System.cmd("git", ["-C", raw_source, "config", "user.email", "test@example.com"])
    System.cmd("git", ["-C", raw_source, "add", "README.md"])
    System.cmd("git", ["-C", raw_source, "commit", "-m", "durable baseline"])
    File.mkdir_p!(remote_root)
    System.cmd("git", ["clone", raw_source, remote_workspace])
    System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

    File.write!(fake_ssh, """
    #!/bin/sh
    case "$*" in
      *"__SYMPHONY_DURABILITY__"*)
        ln -s '#{remote_workspace}' '#{decoded_source}'
        ;;
    esac
    for last_arg do :; done
    exec sh -lc "$last_arg"
    """)

    File.chmod!(fake_ssh, 0o755)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: remote_root,
      workspace_durability_remote_url: "file://#{raw_source}"
    )

    assert {:error, :workspace_preservation_required, ""} =
             Workspace.remove_exact_if_durable(remote_workspace, remote_root, worker_host)

    assert File.dir?(remote_workspace)
    refute File.exists?(remote_workspace <> ".symphony-cleanup")
  end

  test "remote terminal cleanup fails closed when both paths are absent" do
    {:ok, canonical_tmp} = SymphonyElixir.PathSafety.canonicalize(System.tmp_dir!())

    test_root =
      Path.join(
        canonical_tmp,
        "symphony-elixir-remote-missing-artifacts-#{System.unique_integer([:positive])}"
      )

    worker_host = "worker-missing-artifacts"
    remote_root = Path.join(test_root, "remote-workspaces")
    remote_workspace = Path.join(remote_root, "DUD-159")
    fake_ssh = Path.join(test_root, "ssh")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(remote_root)
    System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

    File.write!(fake_ssh, """
    #!/bin/sh
    for last_arg do :; done
    exec sh -lc "$last_arg"
    """)

    File.chmod!(fake_ssh, 0o755)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: remote_root,
      workspace_durability_remote_url: "/srv/git/repo.git"
    )

    assert {:error, :workspace_preservation_required, ""} =
             Workspace.remove_exact_if_durable(remote_workspace, remote_root, worker_host)

    refute File.exists?(remote_workspace)
    refute File.exists?(remote_workspace <> ".symphony-cleanup")
  end

  test "remote terminal cleanup rejects a real-directory quarantine swap" do
    {:ok, canonical_tmp} = SymphonyElixir.PathSafety.canonicalize(System.tmp_dir!())

    test_root =
      Path.join(
        canonical_tmp,
        "symphony-elixir-remote-directory-swap-#{System.unique_integer([:positive])}"
      )

    worker_host = "worker-directory-swap"
    source = Path.join(test_root, "source")
    replacement_source = Path.join(test_root, "replacement-source")
    remote_root = Path.join(test_root, "remote-workspaces")
    remote_workspace = Path.join(remote_root, "DUD-160")
    saved = Path.join(test_root, "saved-original")
    fake_ssh = Path.join(test_root, "ssh")
    previous_path = System.get_env("PATH")

    on_exit(fn ->
      restore_env("PATH", previous_path)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(source)
    File.write!(Path.join(source, "README.md"), "durable original bytes\n")
    System.cmd("git", ["-C", source, "init", "-b", "main"])
    System.cmd("git", ["-C", source, "config", "user.name", "Test User"])
    System.cmd("git", ["-C", source, "config", "user.email", "test@example.com"])
    System.cmd("git", ["-C", source, "add", "README.md"])
    System.cmd("git", ["-C", source, "commit", "-m", "durable baseline"])
    File.write!(Path.join(source, "README.md"), "attacker replacement\n")
    System.cmd("git", ["-C", source, "commit", "-am", "replacement"])
    System.cmd("git", ["clone", "--bare", source, replacement_source])
    System.cmd("git", ["-C", source, "reset", "--hard", "HEAD~1"])
    File.mkdir_p!(remote_root)
    System.cmd("git", ["clone", source, remote_workspace])
    System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

    File.write!(fake_ssh, """
    #!/bin/sh
    for last_arg do :; done
    exec sh -lc "$last_arg"
    """)

    File.chmod!(fake_ssh, 0o755)

    swap_command =
      "quarantine=$PWD; cd ..; mv \"$quarantine\" #{saved}; " <>
        "git clone #{replacement_source} \"$quarantine\" >/dev/null 2>&1"

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: remote_root,
      workspace_durability_remote_url: source,
      hook_before_remove: swap_command
    )

    assert {:error, :workspace_preservation_required, ""} =
             Workspace.remove_exact_if_durable(remote_workspace, remote_root, worker_host)

    assert File.read!(Path.join(saved, "README.md")) == "durable original bytes\n"
    refute File.exists?(remote_workspace)

    assert File.read!(Path.join(remote_workspace <> ".symphony-cleanup", "README.md")) ==
             "attacker replacement\n"
  end

  test "mixed remote affinity keeps each absolute field immutable before prepare and exact cleanup" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-mixed-remote-affinity-#{System.unique_integer([:positive])}"
      )

    worker_host = "worker-mixed"
    local_home = Path.join(test_root, "local-home")
    remote_home = "/home/remote-user"
    remote_root = remote_home <> "/durable-workspaces"
    absolute_path = remote_root <> "/MT-MIXED-PATH"
    tilde_root = "~/durable-workspaces"
    tilde_path = "~/durable-workspaces/MT-MIXED-ROOT"
    absolute_root = remote_root
    resolved_tilde_path = remote_root <> "/MT-MIXED-ROOT"
    drifted_path = remote_root <> "/MT-MIXED-PATH-DRIFTED"
    drifted_root = remote_home <> "/drifted-workspaces"
    drifted_tilde_path = drifted_root <> "/MT-MIXED-ROOT"
    trace_file = Path.join(test_root, "ssh.trace")
    fake_ssh = Path.join(test_root, "ssh")
    remote_wrong_target_sentinel = Path.join(test_root, "remote-wrong-target-must-survive")

    local_wrong_target_sentinel =
      Path.join([local_home, "durable-workspaces", "MT-MIXED-ROOT", "must-survive"])

    tracked_env = [
      "HOME",
      "PATH",
      "SYMP_TEST_SSH_TRACE",
      "SYMP_TEST_RESOLVED_ROOT",
      "SYMP_TEST_RESOLVED_PATH",
      "SYMP_TEST_FAIL_IF_MUTATED",
      "SYMP_TEST_WRONG_TARGET_SENTINEL"
    ]

    previous_env = Map.new(tracked_env, &{&1, System.get_env(&1)})

    on_exit(fn ->
      Enum.each(previous_env, fn {name, value} -> restore_env(name, value) end)
      File.rm_rf(test_root)
    end)

    File.mkdir_p!(Path.dirname(local_wrong_target_sentinel))
    File.write!(local_wrong_target_sentinel, "local-home-target-must-survive")
    File.write!(remote_wrong_target_sentinel, "remote-target-must-survive")
    System.put_env("HOME", local_home)
    System.put_env("SYMP_TEST_SSH_TRACE", trace_file)
    System.put_env("SYMP_TEST_WRONG_TARGET_SENTINEL", remote_wrong_target_sentinel)
    System.put_env("PATH", test_root <> ":" <> (previous_env["PATH"] || ""))

    File.write!(fake_ssh, """
    #!/bin/sh
    trace_file="${SYMP_TEST_SSH_TRACE}"

    record_mutation() {
      if [ "${SYMP_TEST_FAIL_IF_MUTATED:-0}" = 1 ]; then
        rm -f "${SYMP_TEST_WRONG_TARGET_SENTINEL}"
      fi
    }

    case "$*" in
      *"rm -rf"*"__SYMPHONY_AFFINITY__"*)
        printf '%s\n' 'UNSAFE_PREFLIGHT' >> "$trace_file"
        record_mutation
        exit 76
        ;;
      *"__SYMPHONY_AFFINITY__"*)
        printf '%s\n' 'PREFLIGHT' >> "$trace_file"
        printf '%s\t%s\t%s\n' '__SYMPHONY_AFFINITY__' "${SYMP_TEST_RESOLVED_ROOT}" "${SYMP_TEST_RESOLVED_PATH}"
        ;;
      *"__SYMPHONY_WORKSPACE__"*)
        printf '%s\n' 'MUTATE' >> "$trace_file"
        record_mutation
        printf '%s\t%s\t%s\n' '__SYMPHONY_WORKSPACE__' '0' "${SYMP_TEST_RESOLVED_PATH}"
        ;;
      *b4-2-before-remove*)
        printf '%s\n' 'HOOK' >> "$trace_file"
        record_mutation
        ;;
      *"rm -rf"*)
        printf '%s\n' 'CLEANUP' >> "$trace_file"
        record_mutation
        ;;
      *)
        printf '%s\n' 'UNCLASSIFIED' >> "$trace_file"
        exit 75
        ;;
    esac
    """)

    File.chmod!(fake_ssh, 0o755)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: tilde_root,
      worker_ssh_hosts: [worker_host],
      hook_before_remove: "echo b4-2-before-remove"
    )

    set_remote_target = fn root, path ->
      System.put_env("SYMP_TEST_RESOLVED_ROOT", root)
      System.put_env("SYMP_TEST_RESOLVED_PATH", path)
    end

    reset_trace = fn -> File.write!(trace_file, "") end

    trace_lines = fn ->
      trace_file
      |> File.read!()
      |> String.split("\n", trim: true)
    end

    prepare = fn identifier, path, root ->
      Workspace.prepare_for_issue(identifier, worker_host,
        expected_workspace_path: path,
        expected_workspace_root: root,
        expected_worker_host: worker_host
      )
    end

    System.put_env("SYMP_TEST_FAIL_IF_MUTATED", "0")

    set_remote_target.(remote_root, absolute_path)

    assert {:ok, %{path: ^absolute_path, root: ^remote_root}} =
             prepare.("MT-MIXED-PATH", absolute_path, tilde_root)

    assert trace_lines.() == ["PREFLIGHT", "MUTATE"]
    reset_trace.()

    assert {:ok, []} = Workspace.remove_exact(absolute_path, tilde_root, worker_host)
    assert trace_lines.() == ["PREFLIGHT", "HOOK", "CLEANUP"]
    reset_trace.()

    set_remote_target.(remote_root, resolved_tilde_path)

    assert {:ok, %{path: ^resolved_tilde_path, root: ^absolute_root}} =
             prepare.("MT-MIXED-ROOT", tilde_path, absolute_root)

    assert trace_lines.() == ["PREFLIGHT", "MUTATE"]
    reset_trace.()

    assert {:ok, []} = Workspace.remove_exact(tilde_path, absolute_root, worker_host)
    assert trace_lines.() == ["PREFLIGHT", "HOOK", "CLEANUP"]
    reset_trace.()

    System.put_env("SYMP_TEST_FAIL_IF_MUTATED", "1")
    set_remote_target.(remote_root, drifted_path)

    path_drift =
      {:workspace_affinity_mismatch, absolute_path, tilde_root, drifted_path, remote_root, worker_host}

    assert {:error, ^path_drift} = prepare.("MT-MIXED-PATH", absolute_path, tilde_root)
    assert trace_lines.() == ["PREFLIGHT"]
    reset_trace.()

    assert {:error, ^path_drift, ""} = Workspace.remove_exact(absolute_path, tilde_root, worker_host)
    assert trace_lines.() == ["PREFLIGHT"]
    reset_trace.()

    set_remote_target.(drifted_root, drifted_tilde_path)

    root_drift =
      {:workspace_affinity_mismatch, tilde_path, absolute_root, drifted_tilde_path, drifted_root, worker_host}

    assert {:error, ^root_drift} = prepare.("MT-MIXED-ROOT", tilde_path, absolute_root)
    assert trace_lines.() == ["PREFLIGHT"]
    reset_trace.()

    assert {:error, ^root_drift, ""} = Workspace.remove_exact(tilde_path, absolute_root, worker_host)
    assert trace_lines.() == ["PREFLIGHT"]

    assert local_home != remote_home
    assert File.read!(local_wrong_target_sentinel) == "local-home-target-must-survive"
    assert File.read!(remote_wrong_target_sentinel) == "remote-target-must-survive"
  end
end
