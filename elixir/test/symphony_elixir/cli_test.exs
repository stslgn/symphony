defmodule SymphonyElixir.CLITest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias SymphonyElixir.CLI

  @ack_flag "--i-understand-that-this-will-be-running-without-the-usual-guardrails"

  test "reports startup protocol without starting the application" do
    assert CLI.startup_protocol() == "3"
    assert capture_io(fn -> assert :ok = CLI.main(["--startup-protocol"]) end) == "3\n"
  end

  test "reports a loaded-code runtime identity without starting the application" do
    output = capture_io(fn -> assert :ok = CLI.main(["--runtime-identity"]) end)

    assert output =~
             ~r/\Aimage_sha256=[0-9a-f]{64}\nexecution_sha256=[0-9a-f]{64}\n\z/
  end

  test "reports managed workflow operator ids without starting the application" do
    workflow_path =
      Path.join(System.tmp_dir!(), "symphony-managed-workflow-#{System.unique_integer([:positive])}.md")

    on_exit(fn -> File.rm(workflow_path) end)

    File.write!(workflow_path, """
    ---
    tracker:
      kind: linear
      api_key: $LINEAR_API_KEY
      project_slug: test-project
      assignee: managed-assignee
      operator_user_ids:
        - 11111111-1111-1111-1111-111111111111
    ---
    ## Symphony Runtime Prompt
    Test prompt.
    """)

    assert {:ok, ["11111111-1111-1111-1111-111111111111"]} =
             CLI.managed_workflow_identity(workflow_path, "test-project")

    assert capture_io(fn ->
             assert :ok =
                      CLI.main([
                        "--managed-workflow-identity",
                        workflow_path,
                        "test-project"
                      ])
           end) == "11111111-1111-1111-1111-111111111111\n"
  end

  test "managed workflow identity rejects tracker authority drift" do
    workflow_path =
      Path.join(System.tmp_dir!(), "symphony-managed-workflow-#{System.unique_integer([:positive])}.md")

    on_exit(fn -> File.rm(workflow_path) end)

    File.write!(workflow_path, """
    ---
    tracker:
      kind: linear
      api_key: $OTHER_TOKEN
      project_slug: test-project
      operator_user_ids: []
    ---
    ## Symphony Runtime Prompt
    Test prompt.
    """)

    assert {:error, :tracker_api_key_must_use_linear_api_key_env} =
             CLI.managed_workflow_identity(workflow_path, "test-project")
  end

  test "managed workflow identity accepts an omitted assignee independently of ambient state" do
    previous_assignee = System.get_env("LINEAR_ASSIGNEE")
    System.put_env("LINEAR_ASSIGNEE", "ambient-assignee")

    on_exit(fn ->
      if previous_assignee do
        System.put_env("LINEAR_ASSIGNEE", previous_assignee)
      else
        System.delete_env("LINEAR_ASSIGNEE")
      end
    end)

    workflow_path = write_managed_workflow("operator_user_ids: []", nil)

    assert {:ok, []} =
             CLI.managed_workflow_identity(workflow_path, "test-project")
  end

  test "managed workflow identity rejects malformed operator ids" do
    for operator_user_ids <- [
          "operator_user_ids: operator",
          "operator_user_ids: [123]",
          "operator_user_ids: ['']",
          "operator_user_ids: [\"11111111-1111-1111-1111-111111111111\\n22222222-2222-2222-2222-222222222222\"]"
        ] do
      workflow_path = write_managed_workflow(operator_user_ids)

      expected_reason =
        if operator_user_ids == "operator_user_ids: operator",
          do: :operator_user_ids_must_be_a_list,
          else: :operator_user_ids_must_be_linear_actor_ids

      assert {:error, ^expected_reason} =
               CLI.managed_workflow_identity(workflow_path, "test-project")
    end
  end

  test "returns the guardrails acknowledgement banner when the flag is missing" do
    parent = self()

    deps = %{
      file_regular?: fn _path ->
        send(parent, :file_checked)
        true
      end,
      set_workflow_file_path: fn _path ->
        send(parent, :workflow_set)
        :ok
      end,
      set_logs_root: fn _path ->
        send(parent, :logs_root_set)
        :ok
      end,
      set_server_port_override: fn _port ->
        send(parent, :port_set)
        :ok
      end,
      ensure_all_started: fn ->
        send(parent, :started)
        {:ok, [:symphony_elixir]}
      end
    }

    assert {:error, banner} = CLI.evaluate(["WORKFLOW.md"], deps)
    assert banner =~ "This Symphony implementation is a low key engineering preview."
    assert banner =~ "Codex will run without any guardrails."
    assert banner =~ "SymphonyElixir is not a supported product and is presented as-is."
    assert banner =~ @ack_flag
    refute_received :file_checked
    refute_received :workflow_set
    refute_received :logs_root_set
    refute_received :port_set
    refute_received :started
  end

  test "defaults to WORKFLOW.md when workflow path is missing" do
    deps = %{
      file_regular?: fn path -> Path.basename(path) == "WORKFLOW.md" end,
      set_workflow_file_path: fn _path -> :ok end,
      set_logs_root: fn _path -> :ok end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> {:ok, [:symphony_elixir]} end
    }

    assert :ok = CLI.evaluate([@ack_flag], deps)
  end

  defp write_managed_workflow(operator_user_ids, assignee \\ "managed-assignee") do
    workflow_path =
      Path.join(System.tmp_dir!(), "symphony-managed-workflow-#{System.unique_integer([:positive])}.md")

    on_exit(fn -> File.rm(workflow_path) end)

    assignee_line = if assignee, do: "assignee: #{assignee}\n  ", else: ""

    File.write!(workflow_path, """
    ---
    tracker:
      kind: linear
      api_key: $LINEAR_API_KEY
      project_slug: test-project
      #{assignee_line}#{operator_user_ids}
    ---
    ## Symphony Runtime Prompt
    Test prompt.
    """)

    workflow_path
  end

  test "uses an explicit workflow path override when provided" do
    parent = self()
    workflow_path = "tmp/custom/WORKFLOW.md"
    expanded_path = Path.expand(workflow_path)

    deps = %{
      file_regular?: fn path ->
        send(parent, {:workflow_checked, path})
        path == expanded_path
      end,
      set_workflow_file_path: fn path ->
        send(parent, {:workflow_set, path})
        :ok
      end,
      set_logs_root: fn _path -> :ok end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> {:ok, [:symphony_elixir]} end
    }

    assert :ok = CLI.evaluate([@ack_flag, workflow_path], deps)
    assert_received {:workflow_checked, ^expanded_path}
    assert_received {:workflow_set, ^expanded_path}
  end

  test "accepts --logs-root and passes an expanded root to runtime deps" do
    parent = self()

    deps = %{
      file_regular?: fn _path -> true end,
      set_workflow_file_path: fn _path -> :ok end,
      set_logs_root: fn path ->
        send(parent, {:logs_root, path})
        :ok
      end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> {:ok, [:symphony_elixir]} end
    }

    assert :ok = CLI.evaluate([@ack_flag, "--logs-root", "tmp/custom-logs", "WORKFLOW.md"], deps)
    assert_received {:logs_root, expanded_path}
    assert expanded_path == Path.expand("tmp/custom-logs")
  end

  test "returns not found when workflow file does not exist" do
    deps = %{
      file_regular?: fn _path -> false end,
      set_workflow_file_path: fn _path -> :ok end,
      set_logs_root: fn _path -> :ok end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> {:ok, [:symphony_elixir]} end
    }

    assert {:error, message} = CLI.evaluate([@ack_flag, "WORKFLOW.md"], deps)
    assert message =~ "Workflow file not found:"
  end

  test "returns startup error when app cannot start" do
    deps = %{
      file_regular?: fn _path -> true end,
      set_workflow_file_path: fn _path -> :ok end,
      set_logs_root: fn _path -> :ok end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> {:error, :boom} end
    }

    assert {:error, message} = CLI.evaluate([@ack_flag, "WORKFLOW.md"], deps)
    assert message =~ "Failed to start Symphony with workflow"
    assert message =~ ":boom"
  end

  test "returns ok when workflow exists and app starts" do
    deps = %{
      file_regular?: fn _path -> true end,
      set_workflow_file_path: fn _path -> :ok end,
      set_logs_root: fn _path -> :ok end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> {:ok, [:symphony_elixir]} end
    }

    assert :ok = CLI.evaluate([@ack_flag, "WORKFLOW.md"], deps)
  end
end
