defmodule SymphonyElixir.CoreTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.RunLedger

  test "config defaults and validation checks" do
    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_api_token: nil,
      tracker_webhook_secret: nil,
      tracker_project_slug: nil,
      poll_interval_ms: nil,
      tracker_active_states: nil,
      tracker_terminal_states: nil,
      codex_command: nil,
      codex_dynamic_tool_allowlist: nil,
      codex_required_dynamic_tools: nil,
      codex_mcp_tool_auto_approve_allowlist: nil,
      codex_mcp_elicitation_auto_approve_allowlist: nil
    )

    config = Config.settings!()
    assert config.polling.interval_ms == 30_000
    assert config.tracker.active_states == ["Todo", "In Progress"]
    assert config.tracker.terminal_states == ["Closed", "Cancelled", "Canceled", "Duplicate", "Done"]
    assert config.tracker.assignee == nil
    assert config.tracker.webhook_secret == nil
    assert config.agent.max_turns == 20
    assert config.agent.max_run_tokens == nil
    assert config.agent.max_run_seconds == nil
    assert config.workflow.runtime_prompt_mode == "full_prompt_compat"
    assert config.codex.dynamic_tool_allowlist == []
    assert config.codex.required_dynamic_tools == []
    assert config.codex.mcp_tool_auto_approve_allowlist == []
    assert config.codex.mcp_elicitation_auto_approve_allowlist == []

    webhook_env = "SYMPHONY_TEST_LINEAR_WEBHOOK_SECRET"
    previous_webhook_env = System.get_env(webhook_env)
    System.put_env(webhook_env, "synthetic-env-webhook-secret")
    on_exit(fn -> restore_env(webhook_env, previous_webhook_env) end)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_webhook_secret: "$#{webhook_env}"
    )

    assert Config.settings!().tracker.webhook_secret == "synthetic-env-webhook-secret"

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_webhook_secret: "plaintext-webhook-secret"
    )

    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "tracker.webhook_secret"
    assert message =~ "must be an environment reference"

    write_workflow_file!(Workflow.workflow_file_path(), poll_interval_ms: "invalid")

    assert_raise ArgumentError, ~r/interval_ms/, fn ->
      Config.settings!().polling.interval_ms
    end

    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "polling.interval_ms"

    write_workflow_file!(Workflow.workflow_file_path(), poll_interval_ms: 45_000)
    assert Config.settings!().polling.interval_ms == 45_000

    write_workflow_file!(Workflow.workflow_file_path(), max_turns: 0)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "agent.max_turns"

    write_workflow_file!(Workflow.workflow_file_path(), max_turns: 5)
    assert Config.settings!().agent.max_turns == 5

    write_workflow_file!(Workflow.workflow_file_path(),
      max_run_tokens: 250_000,
      max_run_seconds: 7_200
    )

    assert Config.settings!().agent.max_run_tokens == 250_000
    assert Config.settings!().agent.max_run_seconds == 7_200

    write_workflow_file!(Workflow.workflow_file_path(), max_run_tokens: 0)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "agent.max_run_tokens"

    write_workflow_file!(Workflow.workflow_file_path(), max_run_seconds: 0)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "agent.max_run_seconds"

    write_workflow_file!(Workflow.workflow_file_path(), tracker_active_states: "Todo,  Review,")
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "tracker.active_states"

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_api_token: "token",
      tracker_project_slug: nil
    )

    assert {:error, :missing_linear_project_slug} = Config.validate!()

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_project_slug: "project",
      codex_command: ""
    )

    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.command"
    assert message =~ "can't be blank"

    write_workflow_file!(Workflow.workflow_file_path(), codex_command: "   ")
    assert :ok = Config.validate!()
    assert Config.settings!().codex.command == "   "

    write_workflow_file!(Workflow.workflow_file_path(), codex_command: "/bin/sh app-server")
    assert :ok = Config.validate!()

    write_workflow_file!(Workflow.workflow_file_path(), codex_approval_policy: "definitely-not-valid")
    assert :ok = Config.validate!()

    write_workflow_file!(Workflow.workflow_file_path(), codex_thread_sandbox: "unsafe-ish")
    assert :ok = Config.validate!()

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_turn_sandbox_policy: %{type: "workspaceWrite", writableRoots: ["relative/path"]}
    )

    assert :ok = Config.validate!()

    write_workflow_file!(Workflow.workflow_file_path(), codex_approval_policy: 123)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.approval_policy"

    write_workflow_file!(Workflow.workflow_file_path(), codex_thread_sandbox: 123)
    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.thread_sandbox"

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_dynamic_tool_allowlist: [" linear_graphql ", "linear_graphql"],
      codex_required_dynamic_tools: [" linear_graphql ", "linear_graphql"],
      codex_mcp_tool_auto_approve_allowlist: [" Linear / Save issue "],
      codex_mcp_elicitation_auto_approve_allowlist: [" Linear "]
    )

    assert config = Config.settings!().codex
    assert config.dynamic_tool_allowlist == ["linear_graphql"]
    assert config.required_dynamic_tools == ["linear_graphql"]
    assert config.mcp_tool_auto_approve_allowlist == ["Linear/Save issue"]
    assert config.mcp_elicitation_auto_approve_allowlist == ["Linear"]

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_dynamic_tool_allowlist: ["unknown_tool"]
    )

    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.dynamic_tool_allowlist"

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_dynamic_tool_allowlist: [],
      codex_required_dynamic_tools: ["linear_graphql"]
    )

    assert :ok = Config.validate!()

    assert {:error, {:missing_required_dynamic_tools, ["linear_graphql"]}} =
             Config.validate_runtime_capabilities()

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_required_dynamic_tools: ["unknown_tool"]
    )

    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.required_dynamic_tools"

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_mcp_tool_auto_approve_allowlist: ["missing-separator"]
    )

    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.mcp_tool_auto_approve_allowlist"

    write_workflow_file!(Workflow.workflow_file_path(),
      codex_mcp_elicitation_auto_approve_allowlist: [" "]
    )

    assert {:error, {:invalid_workflow_config, message}} = Config.validate!()
    assert message =~ "codex.mcp_elicitation_auto_approve_allowlist"

    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "123")
    assert {:error, {:unsupported_tracker_kind, "123"}} = Config.validate!()
  end

  test "current WORKFLOW.md file is valid and complete" do
    original_workflow_path = Workflow.workflow_file_path()
    on_exit(fn -> Workflow.set_workflow_file_path(original_workflow_path) end)
    Workflow.clear_workflow_file_path()

    assert {:ok, %{config: config, prompt: prompt}} = Workflow.load()
    assert is_map(config)

    tracker = Map.get(config, "tracker", %{})
    assert is_map(tracker)
    assert Map.get(tracker, "kind") == "linear"
    assert is_binary(Map.get(tracker, "project_slug"))
    assert is_list(Map.get(tracker, "active_states"))
    assert is_list(Map.get(tracker, "terminal_states"))

    workflow = Map.get(config, "workflow", %{})
    assert Map.get(workflow, "runtime_prompt_mode") == "managed"

    codex = Map.get(config, "codex", %{})
    assert Map.get(codex, "dynamic_tool_allowlist") == ["linear_graphql"]
    assert Map.get(codex, "required_dynamic_tools") == ["linear_graphql"]

    hooks = Map.get(config, "hooks", %{})
    assert is_map(hooks)
    assert Map.get(hooks, "after_create") =~ "git clone --depth 1 https://github.com/openai/symphony ."
    assert Map.get(hooks, "after_create") =~ "cd elixir && mise trust"
    assert Map.get(hooks, "after_create") =~ "mise exec -- mix deps.get"
    assert Map.get(hooks, "before_remove") =~ "cd elixir && mise exec -- mix workspace.before_remove"

    assert String.trim(prompt) != ""
    assert prompt =~ "This preamble is operator-only."
    assert is_binary(Config.workflow_prompt())
    assert String.starts_with?(Config.workflow_prompt(), "## Symphony Runtime Prompt")
    refute Config.workflow_prompt() =~ "This preamble is operator-only."
    refute Config.workflow_prompt() =~ "Symphony Operator Contract"
    assert :ok = Config.validate_runtime_capabilities()

    worker_prompt =
      PromptBuilder.build_prompt(%Issue{
        id: "managed-boundary",
        identifier: "MT-BOUNDARY",
        title: "Managed prompt boundary",
        state: "Todo",
        labels: []
      })

    assert worker_prompt =~ "## Symphony Runtime Prompt"
    refute worker_prompt =~ "This preamble is operator-only."
    refute worker_prompt =~ "Symphony Operator Contract"
  end

  test "linear api token resolves from LINEAR_API_KEY env var" do
    previous_linear_api_key = System.get_env("LINEAR_API_KEY")
    env_api_key = "test-linear-api-key"

    on_exit(fn -> restore_env("LINEAR_API_KEY", previous_linear_api_key) end)
    System.put_env("LINEAR_API_KEY", env_api_key)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_api_token: nil,
      tracker_project_slug: "project",
      codex_command: "/bin/sh app-server"
    )

    assert Config.settings!().tracker.api_key == env_api_key
    assert Config.settings!().tracker.project_slug == "project"
    assert :ok = Config.validate!()
  end

  test "linear assignee resolves from LINEAR_ASSIGNEE env var" do
    previous_linear_assignee = System.get_env("LINEAR_ASSIGNEE")
    env_assignee = "dev@example.com"

    on_exit(fn -> restore_env("LINEAR_ASSIGNEE", previous_linear_assignee) end)
    System.put_env("LINEAR_ASSIGNEE", env_assignee)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_assignee: nil,
      tracker_project_slug: "project",
      codex_command: "/bin/sh app-server"
    )

    assert Config.settings!().tracker.assignee == env_assignee
  end

  test "workflow file path defaults to WORKFLOW.md in the current working directory when app env is unset" do
    original_workflow_path = Workflow.workflow_file_path()

    on_exit(fn ->
      Workflow.set_workflow_file_path(original_workflow_path)
    end)

    Workflow.clear_workflow_file_path()

    assert Workflow.workflow_file_path() == Path.join(File.cwd!(), "WORKFLOW.md")
  end

  test "workflow file path resolves from app env when set" do
    app_workflow_path = "/tmp/app/WORKFLOW.md"

    on_exit(fn ->
      Workflow.clear_workflow_file_path()
    end)

    Workflow.set_workflow_file_path(app_workflow_path)

    assert Workflow.workflow_file_path() == app_workflow_path
  end

  test "managed workflow rejects prompt-only files without a runtime heading" do
    workflow_path = Path.join(Path.dirname(Workflow.workflow_file_path()), "PROMPT_ONLY_WORKFLOW.md")
    File.write!(workflow_path, "Prompt only\n")

    assert {:error, {:workflow_parse_error, :missing_runtime_prompt_heading}} =
             Workflow.load(workflow_path)
  end

  test "workflow full-prompt fallback requires explicit compatibility mode" do
    workflow_path = Path.join(Path.dirname(Workflow.workflow_file_path()), "COMPAT_WORKFLOW.md")

    File.write!(workflow_path, """
    ---
    workflow:
      runtime_prompt_mode: full_prompt_compat
    ---

    Compatibility worker prompt.
    """)

    assert {:ok,
            %{
              prompt: "Compatibility worker prompt.",
              prompt_template: "Compatibility worker prompt.",
              runtime_prompt_mode: "full_prompt_compat"
            }} = Workflow.load(workflow_path)
  end

  test "workflow load uses Symphony Runtime Prompt section as worker prompt template" do
    workflow_path = Path.join(Path.dirname(Workflow.workflow_file_path()), "RUNTIME_PROMPT_WORKFLOW.md")

    File.write!(workflow_path, """
    ---
    tracker:
      kind: linear
    ---

    # Operator workflow

    Operator-only instructions:
    - Start the Supervisor Watch Loop after runner pickup.
    - Mention `## Symphony Runtime Prompt` inline before the real worker section.

    ## Symphony Runtime Prompt

    You are working on `{{ issue.identifier }}`.

    ## Worker Status Map

    - `Todo` -> start work.
    """)

    assert {:ok, %{prompt: prompt, prompt_template: prompt_template}} = Workflow.load(workflow_path)

    assert prompt =~ "Operator-only instructions"
    assert prompt =~ "Supervisor Watch Loop"
    assert prompt_template =~ "## Symphony Runtime Prompt"
    assert prompt_template =~ "You are working on `{{ issue.identifier }}`."
    assert prompt_template =~ "## Worker Status Map"
    refute prompt_template =~ "Operator-only instructions"
    refute prompt_template =~ "inline before the real worker section"
    refute prompt_template =~ "Supervisor Watch Loop"
  end

  test "workflow load uses the last Symphony Runtime Prompt section when duplicate headings exist" do
    workflow_path = Path.join(Path.dirname(Workflow.workflow_file_path()), "DUPLICATE_RUNTIME_PROMPT_WORKFLOW.md")

    File.write!(workflow_path, """
    ---
    tracker:
      kind: linear
    ---

    ## Symphony Runtime Prompt

    Stale worker prompt from an earlier append.

    # Operator workflow

    Runtime assembly source is `## Symphony Runtime Prompt` plus the exact Linear issue.

    ## Symphony Runtime Prompt

    You are the worker for `{{ issue.identifier }}`.
    Start from the exact Linear issue only.
    """)

    assert {:ok, %{prompt_template: prompt_template}} = Workflow.load(workflow_path)

    assert prompt_template =~ "You are the worker for `{{ issue.identifier }}`."
    assert prompt_template =~ "Start from the exact Linear issue only."
    refute prompt_template =~ "Stale worker prompt"
    refute prompt_template =~ "Operator workflow"
  end

  test "managed workflow rejects a non-exact runtime prompt heading" do
    workflow_path = Path.join(Path.dirname(Workflow.workflow_file_path()), "INEXACT_RUNTIME_PROMPT_WORKFLOW.md")

    File.write!(workflow_path, """
    ---
    workflow:
      runtime_prompt_mode: managed
    ---

    ### Symphony Runtime Prompt

    This heading has the wrong level.
    """)

    assert {:error, {:workflow_parse_error, :missing_runtime_prompt_heading}} =
             Workflow.load(workflow_path)
  end

  test "managed workflow rejects unterminated front matter with no runtime heading" do
    workflow_path = Path.join(Path.dirname(Workflow.workflow_file_path()), "UNTERMINATED_WORKFLOW.md")
    File.write!(workflow_path, "---\ntracker:\n  kind: linear\n")

    assert {:error, {:workflow_parse_error, :missing_runtime_prompt_heading}} =
             Workflow.load(workflow_path)
  end

  test "workflow load rejects non-map front matter" do
    workflow_path = Path.join(Path.dirname(Workflow.workflow_file_path()), "INVALID_FRONT_MATTER_WORKFLOW.md")
    File.write!(workflow_path, "---\n- not-a-map\n---\nPrompt body\n")

    assert {:error, :workflow_front_matter_not_a_map} = Workflow.load(workflow_path)
  end

  test "SymphonyElixir.start_link delegates to the orchestrator" do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory")

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    orchestrator_pid = Process.whereis(SymphonyElixir.Orchestrator)

    on_exit(fn ->
      if is_nil(Process.whereis(SymphonyElixir.Orchestrator)) do
        case Supervisor.restart_child(SymphonyElixir.Supervisor, SymphonyElixir.Orchestrator) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
        end
      end
    end)

    if is_pid(orchestrator_pid) do
      assert :ok = Supervisor.terminate_child(SymphonyElixir.Supervisor, SymphonyElixir.Orchestrator)
    end

    assert {:ok, pid} = SymphonyElixir.start_link()
    assert Process.whereis(SymphonyElixir.Orchestrator) == pid

    GenServer.stop(pid)
  end

  test "linear issue state reconciliation fetch with no running issues is a no-op" do
    assert {:ok, []} = Client.fetch_issue_states_by_ids([])
  end

  test "orchestrator startup aborts when a parked wait cannot be restored" do
    ledger_path = ledger_path("restore-wait-failure")

    assert {:stop, {:operator_wait_restore_failed, :forced_restore_failure}} =
             Orchestrator.init(
               run_ledger_path: ledger_path,
               restore_parked_waits_fn: fn _parked -> {:error, :forced_restore_failure} end
             )
  end

  test "non-active issue state stops running agent without cleaning workspace" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-nonactive-reconcile-#{System.unique_integer([:positive])}"
      )

    issue_id = "issue-1"
    issue_identifier = "MT-555"
    workspace = Path.join(test_root, issue_identifier)

    try do
      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: test_root,
        tracker_active_states: ["Todo", "In Progress", "In Review"],
        tracker_terminal_states: ["Closed", "Cancelled", "Canceled", "Duplicate"]
      )

      File.mkdir_p!(test_root)
      File.mkdir_p!(workspace)

      agent_pid =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      state = %Orchestrator.State{
        running: %{
          issue_id => %{
            pid: agent_pid,
            ref: nil,
            identifier: issue_identifier,
            issue: %Issue{id: issue_id, state: "Todo", identifier: issue_identifier},
            started_at: DateTime.utc_now()
          }
        },
        claimed: MapSet.new([issue_id]),
        codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
        retry_attempts: %{}
      }

      issue = %Issue{
        id: issue_id,
        identifier: issue_identifier,
        state: "Backlog",
        title: "Queued",
        description: "Not started",
        labels: []
      }

      updated_state = Orchestrator.reconcile_issue_states_for_test([issue], state)

      refute Map.has_key?(updated_state.running, issue_id)
      refute MapSet.member?(updated_state.claimed, issue_id)
      refute Process.alive?(agent_pid)
      assert File.exists?(workspace)
    after
      File.rm_rf(test_root)
    end
  end

  test "human review durably parks a running issue without scheduling retry" do
    issue_id = "issue-human-review"
    issue_identifier = "MT-555-PARK"

    ledger_path =
      Path.join(
        System.tmp_dir!(),
        "symphony-operator-wait-#{RunLedger.new_id("test")}/events.jsonl"
      )

    agent_pid =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    state = %Orchestrator.State{
      run_ledger_path: ledger_path,
      runner_generation: "runner-test",
      running: %{
        issue_id => %{
          pid: agent_pid,
          ref: nil,
          run_id: "run-human-review",
          retry_attempt: 2,
          identifier: issue_identifier,
          issue: %Issue{id: issue_id, state: "In Progress", identifier: issue_identifier},
          started_at: DateTime.utc_now()
        }
      },
      claimed: MapSet.new([issue_id]),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
      retry_attempts: %{}
    }

    seed_running_ledger!(ledger_path, state.running[issue_id])

    issue = %Issue{
      id: issue_id,
      identifier: issue_identifier,
      state: "Human Review",
      title: "Owner review",
      description: "Waiting for approval",
      labels: []
    }

    updated_state = Orchestrator.reconcile_issue_states_for_test([issue], state)

    refute Map.has_key?(updated_state.running, issue_id)
    refute MapSet.member?(updated_state.claimed, issue_id)
    refute Map.has_key?(updated_state.retry_attempts, issue_id)
    refute Process.alive?(agent_pid)

    assert %{
             wait_id: "wait_" <> _,
             reason: "waiting_owner",
             run_id: "run-human-review",
             attempt: 2,
             tracker_state: "Human Review",
             allowed_actions: ["approve", "reject"]
           } = updated_state.parked[issue_id]

    assert {:ok, events} = RunLedger.read_events(ledger_path)

    assert Enum.any?(events, fn event ->
             event["transition"] == "run_parked" and
               event["issue_id"] == issue_id and
               event["parked_reason"] == "waiting_owner"
           end)
  end

  test "parked issues are excluded from dispatch even when Linear is active" do
    issue = %Issue{
      id: "issue-parked-active",
      identifier: "MT-PARKED-ACTIVE",
      state: "Todo",
      title: "Still parked",
      description: "Explicit resolution is required",
      labels: [],
      assigned_to_worker: true
    }

    state = %Orchestrator.State{
      max_concurrent_agents: 1,
      running: %{},
      parked: %{issue.id => %{wait_id: "wait-active"}},
      claimed: MapSet.new()
    }

    refute Orchestrator.should_dispatch_issue_for_test(issue, state)
  end

  test "global pause is durable, idempotent, and blocks new dispatch" do
    ledger_path =
      Path.join(
        System.tmp_dir!(),
        "symphony-global-pause-#{RunLedger.new_id("test")}/events.jsonl"
      )

    state = %Orchestrator.State{
      poll_interval_ms: 30_000,
      max_concurrent_agents: 1,
      run_ledger_path: ledger_path,
      runner_generation: "runner-pause",
      running: %{},
      parked: %{},
      claimed: MapSet.new(),
      retry_attempts: %{},
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    issue = %Issue{
      id: "issue-paused",
      identifier: "MT-PAUSED",
      state: "Todo",
      title: "Do not dispatch",
      assigned_to_worker: true
    }

    assert {:reply, {:ok, %{dispatch_paused: true, changed: true}}, paused_state} =
             Orchestrator.handle_call(
               {:set_dispatch_paused, true},
               {self(), make_ref()},
               state
             )

    refute Orchestrator.should_dispatch_issue_for_test(issue, paused_state)

    assert {:reply, {:ok, %{dispatch_paused: true, changed: false}}, same_state} =
             Orchestrator.handle_call(
               {:set_dispatch_paused, true},
               {self(), make_ref()},
               paused_state
             )

    assert {:reply, {:ok, %{dispatch_paused: false, changed: true}}, resumed_state} =
             Orchestrator.handle_call(
               {:set_dispatch_paused, false},
               {self(), make_ref()},
               same_state
             )

    refute resumed_state.dispatch_paused
    assert is_reference(resumed_state.tick_timer_ref)
    Process.cancel_timer(resumed_state.tick_timer_ref)

    assert {:ok, events} = RunLedger.read_events(ledger_path)
    assert Enum.count(events, &(&1["transition"] == "dispatch_paused")) == 1
    assert Enum.count(events, &(&1["transition"] == "dispatch_resumed")) == 1
  end

  test "global pause defers queued retries without consuming their attempt" do
    issue_id = "issue-paused-retry"
    retry_token = make_ref()

    state = %Orchestrator.State{
      poll_interval_ms: 30_000,
      dispatch_paused: true,
      retry_attempts: %{
        issue_id => %{
          attempt: 3,
          timer_ref: nil,
          retry_token: retry_token,
          due_at_ms: System.monotonic_time(:millisecond),
          identifier: "MT-PAUSED-RETRY",
          error: "agent_exit"
        }
      }
    }

    assert {:noreply, deferred_state} =
             Orchestrator.handle_info({:retry_issue, issue_id, retry_token}, state)

    deferred_retry = deferred_state.retry_attempts[issue_id]
    assert deferred_retry.attempt == 3
    refute deferred_retry.retry_token == retry_token
    assert is_reference(deferred_retry.timer_ref)

    assert {:reply, {:ok, %{dispatch_paused: false, changed: true}}, resumed_state} =
             Orchestrator.handle_call(
               {:set_dispatch_paused, false},
               {self(), make_ref()},
               %{deferred_state | run_ledger_path: nil}
             )

    resumed_retry = resumed_state.retry_attempts[issue_id]
    assert resumed_retry.attempt == 3
    assert resumed_retry.due_at_ms <= System.monotonic_time(:millisecond)
    assert is_reference(resumed_retry.timer_ref)
    assert is_reference(resumed_state.tick_timer_ref)

    Process.cancel_timer(resumed_retry.timer_ref)
    Process.cancel_timer(resumed_state.tick_timer_ref)
  end

  test "queued resumes stay visible while pause, capacity, tracker, or legacy affinity blocks dispatch" do
    issue_id = "issue-blocked-resume"

    queued_resumes = %{
      issue_id => %{
        issue_id: issue_id,
        identifier: "MT-BLOCKED-RESUME",
        run_id: "run-blocked-resume-source",
        wait_id: "wait-blocked-resume",
        attempt: 3,
        stage: "resume_queued",
        queued_at: DateTime.utc_now()
      }
    }

    base_state = %Orchestrator.State{
      poll_interval_ms: 30_000,
      max_concurrent_agents: 1,
      run_ledger_path: nil,
      queued_resumes: queued_resumes,
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    assert {:noreply, paused_state} =
             Orchestrator.handle_info(:run_poll_cycle, %{base_state | dispatch_paused: true})

    assert paused_state.queued_resumes == queued_resumes

    if is_reference(paused_state.tick_timer_ref), do: Process.cancel_timer(paused_state.tick_timer_ref)

    dummy_issue = %Issue{
      id: "issue-capacity-holder",
      identifier: "MT-CAPACITY-HOLDER",
      title: "Occupy the only slot",
      state: "In Progress",
      assigned_to_worker: true
    }

    resumed_issue = %Issue{
      id: issue_id,
      identifier: "MT-BLOCKED-RESUME",
      title: "Remain queued",
      state: "In Progress",
      assigned_to_worker: true
    }

    dummy_pid = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> if Process.alive?(dummy_pid), do: Process.exit(dummy_pid, :kill) end)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      max_concurrent_agents: 1
    )

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [dummy_issue, resumed_issue])

    capacity_state = %{
      base_state
      | running: %{
          dummy_issue.id => %{
            pid: dummy_pid,
            ref: nil,
            run_id: "run-capacity-holder",
            retry_attempt: 0,
            identifier: dummy_issue.identifier,
            issue: dummy_issue,
            started_at: DateTime.utc_now()
          }
        },
        claimed: MapSet.new([dummy_issue.id])
    }

    assert {:noreply, capacity_blocked_state} =
             Orchestrator.handle_info(:run_poll_cycle, capacity_state)

    assert capacity_blocked_state.queued_resumes == queued_resumes

    if is_reference(capacity_blocked_state.tick_timer_ref),
      do: Process.cancel_timer(capacity_blocked_state.tick_timer_ref)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "linear",
      tracker_api_token: nil
    )

    assert {:noreply, tracker_blocked_state} =
             Orchestrator.handle_info(:run_poll_cycle, base_state)

    assert tracker_blocked_state.queued_resumes == queued_resumes

    assert {:reply, snapshot, ^tracker_blocked_state} =
             Orchestrator.handle_call(:snapshot, {self(), make_ref()}, tracker_blocked_state)

    assert [%{issue_id: ^issue_id, attempt: 3, stage: "resume_queued"}] = snapshot.retrying

    if is_reference(tracker_blocked_state.tick_timer_ref),
      do: Process.cancel_timer(tracker_blocked_state.tick_timer_ref)

    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory")
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [resumed_issue])

    assert {:noreply, affinity_blocked_state} =
             Orchestrator.handle_info(:run_poll_cycle, base_state)

    assert affinity_blocked_state.queued_resumes == queued_resumes
    assert affinity_blocked_state.running == %{}
    assert affinity_blocked_state.claimed == MapSet.new()

    assert {:reply, affinity_snapshot, ^affinity_blocked_state} =
             Orchestrator.handle_call(:snapshot, {self(), make_ref()}, affinity_blocked_state)

    assert [%{stage: "resume_queued", error: "workspace_affinity_missing"}] =
             affinity_snapshot.retrying

    if is_reference(affinity_blocked_state.tick_timer_ref),
      do: Process.cancel_timer(affinity_blocked_state.tick_timer_ref)
  end

  test "typed waits require matching ids and allowed actions before resume" do
    issue_id = "issue-secret-wait"

    ledger_path =
      Path.join(
        System.tmp_dir!(),
        "symphony-resolve-wait-#{RunLedger.new_id("test")}/events.jsonl"
      )

    agent_pid = spawn(fn -> Process.sleep(:infinity) end)

    state = %Orchestrator.State{
      run_ledger_path: ledger_path,
      runner_generation: "runner-resolve",
      running: %{
        issue_id => %{
          pid: agent_pid,
          ref: nil,
          run_id: "run-secret",
          retry_attempt: 1,
          identifier: "MT-SECRET",
          issue: %Issue{id: issue_id, identifier: "MT-SECRET", state: "In Progress"},
          worker_host: "worker-a",
          workspace_path: "/srv/symphony/MT-SECRET",
          started_at: DateTime.utc_now()
        }
      },
      claimed: MapSet.new([issue_id]),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    seed_running_ledger!(ledger_path, state.running[issue_id])

    assert {:reply, {:ok, wait}, parked_state} =
             Orchestrator.handle_call(
               {:park_issue, issue_id, "waiting_secret"},
               {self(), make_ref()},
               state
             )

    assert wait.reason == "waiting_secret"
    assert wait.allowed_actions == ["retry", "reject"]
    assert wait.worker_host == "worker-a"
    assert wait.workspace_path == "/srv/symphony/MT-SECRET"

    assert {:reply, {:error, :wait_id_mismatch}, ^parked_state} =
             Orchestrator.handle_call(
               {:resolve_wait, issue_id, "wrong-wait", "retry"},
               {self(), make_ref()},
               parked_state
             )

    assert {:reply, {:error, :action_not_allowed}, ^parked_state} =
             Orchestrator.handle_call(
               {:resolve_wait, issue_id, wait.wait_id, "approve"},
               {self(), make_ref()},
               parked_state
             )

    assert {:reply, {:ok, %{action: "reject", resumed: false}}, rejected_state} =
             Orchestrator.handle_call(
               {:resolve_wait, issue_id, wait.wait_id, "reject"},
               {self(), make_ref()},
               parked_state
             )

    assert rejected_state.parked[issue_id].wait_id == wait.wait_id

    assert {:reply, {:ok, %{action: "retry", resumed: true}}, resumed_state} =
             Orchestrator.handle_call(
               {:resolve_wait, issue_id, wait.wait_id, "retry"},
               {self(), make_ref()},
               parked_state
             )

    refute Map.has_key?(resumed_state.parked, issue_id)

    assert %{
             attempt: 2,
             stage: "resume_queued",
             run_id: "run-secret",
             wait_id: wait_id,
             worker_host: "worker-a",
             workspace_path: "/srv/symphony/MT-SECRET"
           } = resumed_state.queued_resumes[issue_id]

    assert wait_id == wait.wait_id

    assert {:reply, snapshot, snapshotted_state} =
             Orchestrator.handle_call(:snapshot, {self(), make_ref()}, resumed_state)

    assert [%{issue_id: ^issue_id, attempt: 2, stage: "resume_queued"}] = snapshot.retrying

    assert {:ok, events} = RunLedger.read_events(ledger_path)
    assert Enum.count(events, &(&1["transition"] == "run_parked")) == 1
    assert Enum.count(events, &(&1["transition"] == "wait_rejected")) == 1
    assert Enum.count(events, &(&1["transition"] == "resume_queued")) == 1

    assert Enum.any?(events, fn event ->
             event["transition"] == "resume_queued" and event["attempt"] == 2 and
               event["worker_host"] == "worker-a" and
               event["workspace_path"] == "/srv/symphony/MT-SECRET"
           end)

    assert {:ok, recovery} = RunLedger.reconcile_startup(ledger_path, "runner-after-resume")
    assert recovery.queued_resumes[issue_id]["attempt"] == 2
    assert recovery.queued_resumes[issue_id]["worker_host"] == "worker-a"
    assert recovery.queued_resumes[issue_id]["workspace_path"] == "/srv/symphony/MT-SECRET"

    if is_reference(snapshotted_state.tick_timer_ref),
      do: Process.cancel_timer(snapshotted_state.tick_timer_ref)
  end

  test "terminal parked issue releases durably and removes its local recorded workspace" do
    root = parked_workspace_root("local-terminal")
    workspace = Path.join(root, "MT-PARKED-LOCAL-TERMINAL")
    File.mkdir_p!(workspace)
    File.write!(Path.join(workspace, "remove-me"), "old")

    {state, wait} = parked_reconcile_state("local-terminal", workspace, root, nil)

    reconciled =
      Orchestrator.reconcile_parked_issue_for_test(
        %Issue{id: wait.issue_id, identifier: wait.identifier, state: "Closed"},
        state
      )

    refute File.exists?(workspace)
    refute Map.has_key?(reconciled.parked, wait.issue_id)
    refute Map.has_key?(reconciled.cleanup_pending, wait.issue_id)

    assert {:ok, events} = RunLedger.read_events(state.run_ledger_path)
    assert Enum.any?(events, &(&1["transition"] == "wait_released" and &1["release_reason"] == "tracker_terminal"))
    assert Enum.any?(events, &(&1["transition"] == "workspace_cleanup_completed"))
  end

  test "unrouted parked issue releases durably but preserves its local workspace" do
    root = parked_workspace_root("local-unrouted")
    workspace = Path.join(root, "MT-PARKED-LOCAL-UNROUTED")
    sentinel = Path.join(workspace, "must-survive")
    File.mkdir_p!(workspace)
    File.write!(sentinel, "kept")

    {state, wait} = parked_reconcile_state("local-unrouted", workspace, root, nil)

    reconciled =
      Orchestrator.reconcile_parked_issue_for_test(
        %Issue{
          id: wait.issue_id,
          identifier: wait.identifier,
          state: "In Progress",
          assigned_to_worker: false
        },
        state
      )

    assert File.read!(sentinel) == "kept"
    refute Map.has_key?(reconciled.parked, wait.issue_id)
    refute Map.has_key?(reconciled.cleanup_pending, wait.issue_id)

    assert {:ok, events} = RunLedger.read_events(state.run_ledger_path)

    assert Enum.any?(events, fn event ->
             event["transition"] == "wait_released" and
               event["release_reason"] == "worker_route_removed"
           end)

    refute Enum.any?(events, &String.starts_with?(&1["transition"], "workspace_cleanup_"))
  end

  test "terminal parked issue releases durably and removes its remote recorded workspace" do
    {remote_root, workspace, trace_file} = install_fake_parked_cleanup_ssh!("remote-terminal")
    {state, wait} = parked_reconcile_state("remote-terminal", workspace, remote_root, "worker-a")

    reconciled =
      Orchestrator.reconcile_parked_issue_for_test(
        %Issue{id: wait.issue_id, identifier: wait.identifier, state: "Closed"},
        state
      )

    refute Map.has_key?(reconciled.parked, wait.issue_id)
    refute Map.has_key?(reconciled.cleanup_pending, wait.issue_id)

    trace = File.read!(trace_file)
    assert trace =~ "worker-a bash -lc"
    assert trace =~ "rm -rf"
    assert trace =~ workspace
  end

  test "unrouted parked issue releases durably without touching its remote workspace" do
    {remote_root, workspace, trace_file} = install_fake_parked_cleanup_ssh!("remote-unrouted")
    {state, wait} = parked_reconcile_state("remote-unrouted", workspace, remote_root, "worker-a")

    reconciled =
      Orchestrator.reconcile_parked_issue_for_test(
        %Issue{
          id: wait.issue_id,
          identifier: wait.identifier,
          state: "In Progress",
          assigned_to_worker: false
        },
        state
      )

    refute Map.has_key?(reconciled.parked, wait.issue_id)
    refute Map.has_key?(reconciled.cleanup_pending, wait.issue_id)
    refute File.exists?(trace_file)

    assert {:ok, events} = RunLedger.read_events(state.run_ledger_path)
    refute Enum.any?(events, &String.starts_with?(&1["transition"], "workspace_cleanup_"))
  end

  test "observed token budget exhaustion parks the run without scheduling retry" do
    issue_id = "issue-token-budget"

    ledger_path =
      Path.join(
        System.tmp_dir!(),
        "symphony-token-budget-#{RunLedger.new_id("test")}/events.jsonl"
      )

    agent_pid = spawn(fn -> Process.sleep(:infinity) end)

    running_entry = %{
      pid: agent_pid,
      ref: nil,
      run_id: "run-token-budget",
      retry_attempt: 1,
      identifier: "MT-TOKENS",
      issue: %Issue{id: issue_id, identifier: "MT-TOKENS", state: "In Progress"},
      started_at: DateTime.utc_now(),
      session_id: nil,
      session_title: nil,
      last_codex_message: nil,
      last_codex_timestamp: nil,
      last_codex_event: nil,
      codex_app_server_pid: nil,
      codex_input_tokens: 0,
      codex_output_tokens: 0,
      codex_total_tokens: 0,
      codex_last_reported_input_tokens: 0,
      codex_last_reported_output_tokens: 0,
      codex_last_reported_total_tokens: 0,
      codex_token_telemetry_observed: false,
      turn_count: 0,
      run_budget: %{max_turns: 20, max_tokens: 100, max_seconds: nil},
      run_budget_timer_ref: nil
    }

    state = %Orchestrator.State{
      run_ledger_path: ledger_path,
      runner_generation: "runner-token-budget",
      running: %{issue_id => running_entry},
      claimed: MapSet.new([issue_id]),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    seed_running_ledger!(ledger_path, running_entry)

    update = %{
      event: :notification,
      timestamp: DateTime.utc_now(),
      run_id: "run-token-budget",
      payload: %{
        "method" => "turn/completed",
        "usage" => %{
          "input_tokens" => 80,
          "output_tokens" => 20,
          "total_tokens" => 100
        }
      }
    }

    assert {:noreply, parked_state} =
             Orchestrator.handle_info(
               {:codex_worker_update, issue_id, update},
               state
             )

    refute Map.has_key?(parked_state.running, issue_id)
    refute Map.has_key?(parked_state.retry_attempts, issue_id)
    refute MapSet.member?(parked_state.claimed, issue_id)
    refute Process.alive?(agent_pid)

    assert %{
             reason: "run_budget_exhausted",
             terminal_reason: "token_budget_exhausted",
             allowed_actions: ["retry", "reject"]
           } = parked_state.parked[issue_id]

    assert {:ok, events} = RunLedger.read_events(ledger_path)

    assert Enum.any?(events, fn event ->
             event["transition"] == "run_parked" and
               event["terminal_reason"] == "token_budget_exhausted"
           end)
  end

  test "turn and time budget signals park only the matching active run" do
    Enum.each(
      [
        {"turn_budget_exhausted",
         fn issue_id, run_id ->
           {:worker_budget_exhausted, issue_id, %{run_id: run_id, terminal_reason: "turn_budget_exhausted"}}
         end},
        {"time_budget_exhausted", fn issue_id, run_id -> {:run_budget_timeout, issue_id, run_id} end}
      ],
      fn {terminal_reason, message_builder} ->
        issue_id = "issue-#{terminal_reason}"
        run_id = "run-#{terminal_reason}"
        agent_pid = spawn(fn -> Process.sleep(:infinity) end)

        state = %Orchestrator.State{
          running: %{
            issue_id => %{
              pid: agent_pid,
              ref: nil,
              run_id: run_id,
              retry_attempt: 1,
              identifier: "MT-BUDGET",
              issue: %Issue{id: issue_id, identifier: "MT-BUDGET", state: "In Progress"},
              started_at: DateTime.utc_now(),
              run_budget_timer_ref: nil
            }
          },
          claimed: MapSet.new([issue_id]),
          codex_totals: %{
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            seconds_running: 0
          }
        }

        assert {:noreply, parked_state} =
                 Orchestrator.handle_info(message_builder.(issue_id, run_id), state)

        assert parked_state.parked[issue_id].terminal_reason == terminal_reason
        assert parked_state.parked[issue_id].reason == "run_budget_exhausted"
        refute Map.has_key?(parked_state.retry_attempts, issue_id)
        refute Process.alive?(agent_pid)
      end
    )
  end

  test "terminal issue state stops running agent and cleans workspace" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-terminal-reconcile-#{System.unique_integer([:positive])}"
      )

    issue_id = "issue-2"
    issue_identifier = "MT-556"
    workspace = Path.join(test_root, issue_identifier)

    try do
      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: test_root,
        tracker_active_states: ["Todo", "In Progress", "In Review"],
        tracker_terminal_states: ["Closed", "Cancelled", "Canceled", "Duplicate"]
      )

      File.mkdir_p!(test_root)
      File.mkdir_p!(workspace)

      parent = self()
      shutdown_marker = Path.join(workspace, "worker-shutdown-established")

      agent_pid =
        spawn(fn ->
          Process.flag(:trap_exit, true)

          receive do
            {:EXIT, _from, :shutdown} ->
              send(parent, {:worker_shutdown_write, File.write(shutdown_marker, "stopped")})
          end
        end)

      state = %Orchestrator.State{
        running: %{
          issue_id => %{
            pid: agent_pid,
            ref: nil,
            run_id: "run-terminal-cleanup",
            retry_attempt: 0,
            identifier: issue_identifier,
            issue: %Issue{id: issue_id, state: "In Progress", identifier: issue_identifier},
            worker_host: nil,
            workspace_path: workspace,
            workspace_root: test_root,
            started_at: DateTime.utc_now()
          }
        },
        claimed: MapSet.new([issue_id]),
        codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
        retry_attempts: %{}
      }

      issue = %Issue{
        id: issue_id,
        identifier: issue_identifier,
        state: "Closed",
        title: "Done",
        description: "Completed",
        labels: []
      }

      updated_state = Orchestrator.reconcile_issue_states_for_test([issue], state)

      assert_receive {:worker_shutdown_write, :ok}
      refute Map.has_key?(updated_state.running, issue_id)
      refute MapSet.member?(updated_state.claimed, issue_id)
      refute Process.alive?(agent_pid)
      refute File.exists?(workspace)
    after
      File.rm_rf(test_root)
    end
  end

  test "terminal cleanup removes only the captured root path after config changes" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-exact-cleanup-#{System.unique_integer([:positive])}"
      )

    old_root = Path.join(test_root, "old-root")
    new_root = Path.join(test_root, "new-root")
    identifier = "MT-ROOT-CHANGE"
    old_workspace = Path.join(old_root, identifier)
    new_workspace = Path.join(new_root, identifier)
    new_sentinel = Path.join(new_workspace, "must-survive")
    issue_id = "issue-root-change"

    on_exit(fn -> File.rm_rf(test_root) end)
    File.mkdir_p!(old_workspace)
    File.mkdir_p!(new_workspace)
    File.write!(Path.join(old_workspace, "remove-me"), "old")
    File.write!(new_sentinel, "new")

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: new_root,
      tracker_terminal_states: ["Closed"]
    )

    worker_pid = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> if Process.alive?(worker_pid), do: Process.exit(worker_pid, :kill) end)

    running_entry = %{
      pid: worker_pid,
      ref: nil,
      run_id: "run-root-change",
      retry_attempt: 0,
      identifier: identifier,
      issue: %Issue{id: issue_id, identifier: identifier, state: "In Progress"},
      worker_host: nil,
      workspace_path: old_workspace,
      workspace_root: old_root,
      started_at: DateTime.utc_now()
    }

    state = %Orchestrator.State{
      run_ledger_path: nil,
      running: %{issue_id => running_entry},
      claimed: MapSet.new([issue_id]),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    terminal_issue = %Issue{id: issue_id, identifier: identifier, state: "Closed"}
    cleaned_state = Orchestrator.reconcile_issue_states_for_test([terminal_issue], state)

    refute File.exists?(old_workspace)
    assert File.read!(new_sentinel) == "new"
    refute Map.has_key?(cleaned_state.cleanup_pending, issue_id)
    refute MapSet.member?(cleaned_state.claimed, issue_id)
  end

  test "terminal cleanup with missing affinity remains claimed and visible" do
    issue_id = "issue-cleanup-missing-affinity"
    identifier = "MT-CLEANUP-MISSING"
    worker_pid = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> if Process.alive?(worker_pid), do: Process.exit(worker_pid, :kill) end)

    running_entry = %{
      pid: worker_pid,
      ref: nil,
      run_id: "run-cleanup-missing-affinity",
      retry_attempt: 0,
      identifier: identifier,
      issue: %Issue{id: issue_id, identifier: identifier, state: "In Progress"},
      worker_host: nil,
      workspace_path: nil,
      workspace_root: nil,
      started_at: DateTime.utc_now()
    }

    state = %Orchestrator.State{
      run_ledger_path: nil,
      running: %{issue_id => running_entry},
      claimed: MapSet.new([issue_id]),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    terminal_issue = %Issue{id: issue_id, identifier: identifier, state: "Closed"}
    pending_state = Orchestrator.reconcile_issue_states_for_test([terminal_issue], state)

    assert MapSet.member?(pending_state.claimed, issue_id)
    assert pending_state.cleanup_pending[issue_id].cleanup_error == :workspace_affinity_missing

    assert {:reply, snapshot, _state} =
             Orchestrator.handle_call(:snapshot, {self(), make_ref()}, pending_state)

    assert Enum.any?(snapshot.retrying, fn row ->
             row.issue_id == issue_id and row.stage == "cleanup_pending" and
               row.error == "workspace_affinity_missing"
           end)
  end

  test "completion ledger failure retains claim and blocks continuation until persisted" do
    issue_id = "issue-terminal-completion"
    ref = make_ref()
    blocked_path = blocked_ledger_path()
    valid_path = ledger_path("completion-retry")
    state = terminal_transition_state(issue_id, ref, blocked_path, retry_attempt: 3)

    assert {:noreply, blocked_state} =
             Orchestrator.handle_info({:DOWN, ref, :process, self(), :normal}, state)

    assert blocked_state.running[issue_id].terminal_pending.transition == "run_completed"
    assert MapSet.member?(blocked_state.claimed, issue_id)
    refute MapSet.member?(blocked_state.completed, issue_id)
    refute Map.has_key?(blocked_state.retry_attempts, issue_id)

    seed_running_ledger!(valid_path, blocked_state.running[issue_id])

    recovered_state =
      blocked_state
      |> Map.put(:run_ledger_path, valid_path)
      |> Orchestrator.retry_pending_terminal_transitions_for_test()

    assert {:ok, events} = RunLedger.read_events(valid_path)

    assert Enum.map(events, & &1["transition"]) == [
             "run_claimed",
             "run_started",
             "run_completed",
             "retry_scheduled"
           ]

    refute Map.has_key?(recovered_state.running, issue_id)
    assert MapSet.member?(recovered_state.completed, issue_id)
    assert recovered_state.retry_attempts[issue_id].attempt == 4

    Process.cancel_timer(recovered_state.retry_attempts[issue_id].timer_ref)
  end

  test "failure ledger failure retains claim and blocks retry until persisted" do
    issue_id = "issue-terminal-failure"
    ref = make_ref()
    blocked_path = blocked_ledger_path()
    valid_path = ledger_path("failure-retry")
    state = terminal_transition_state(issue_id, ref, blocked_path, retry_attempt: 3)

    assert {:noreply, blocked_state} =
             Orchestrator.handle_info({:DOWN, ref, :process, self(), :worker_crashed}, state)

    assert blocked_state.running[issue_id].terminal_pending.transition == "run_failed"
    assert MapSet.member?(blocked_state.claimed, issue_id)
    refute Map.has_key?(blocked_state.retry_attempts, issue_id)

    seed_running_ledger!(valid_path, blocked_state.running[issue_id])

    recovered_state =
      blocked_state
      |> Map.put(:run_ledger_path, valid_path)
      |> Orchestrator.retry_pending_terminal_transitions_for_test()

    refute Map.has_key?(recovered_state.running, issue_id)
    assert recovered_state.retry_attempts[issue_id].attempt == 4

    assert {:ok, events} = RunLedger.read_events(valid_path)

    assert Enum.map(events, & &1["transition"]) == [
             "run_claimed",
             "run_started",
             "run_failed",
             "retry_scheduled"
           ]

    Process.cancel_timer(recovered_state.retry_attempts[issue_id].timer_ref)
  end

  test "retry append failure retains a claimed durable-retry pending state" do
    issue_id = "issue-retry-durability"
    ref = make_ref()
    path = ledger_path("retry-durability")
    state = terminal_transition_state(issue_id, ref, path, retry_attempt: 3)
    seed_running_ledger!(path, state.running[issue_id])

    append_fn = fn ledger_path, event ->
      if event.transition == "retry_scheduled" do
        {:error, :forced_retry_append_failure}
      else
        RunLedger.append(ledger_path, event)
      end
    end

    state = %{state | run_ledger_append_fn: append_fn}

    assert {:noreply, pending_state} =
             Orchestrator.handle_info({:DOWN, ref, :process, self(), :worker_crashed}, state)

    refute Map.has_key?(pending_state.running, issue_id)
    assert MapSet.member?(pending_state.claimed, issue_id)

    assert %{
             attempt: 4,
             status: :durability_pending,
             timer_ref: nil,
             retry_token: nil,
             persistence_error: :forced_retry_append_failure
           } = pending_state.retry_attempts[issue_id]

    candidate = %Issue{
      id: issue_id,
      identifier: "MT-TERMINAL",
      title: "Retry durability",
      state: "In Progress",
      assigned_to_worker: true
    }

    refute Orchestrator.should_dispatch_issue_for_test(candidate, pending_state)

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-retry-durability")
    assert recovery.recovered_attempts[issue_id] == 4

    scheduled_state =
      pending_state
      |> Map.put(:run_ledger_append_fn, &RunLedger.append/2)
      |> Orchestrator.retry_pending_durable_retries_for_test()

    assert scheduled_state.retry_attempts[issue_id].status == :scheduled
    assert is_reference(scheduled_state.retry_attempts[issue_id].timer_ref)
    assert MapSet.member?(scheduled_state.claimed, issue_id)

    assert {:ok, events} = RunLedger.read_events(path)
    assert Enum.count(events, &(&1["transition"] == "retry_scheduled")) == 1

    Process.cancel_timer(scheduled_state.retry_attempts[issue_id].timer_ref)
  end

  test "tracker terminal ledger failure blocks worker stop cleanup and claim release" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-terminal-ledger-block-#{System.unique_integer([:positive])}"
      )

    issue_id = "issue-terminal-ledger-block"
    issue_identifier = "MT-TERMINAL-BLOCK"
    workspace = Path.join(test_root, issue_identifier)
    blocked_path = blocked_ledger_path()
    valid_path = ledger_path("tracker-terminal-retry")

    try do
      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: test_root,
        tracker_active_states: ["Todo", "In Progress"],
        tracker_terminal_states: ["Closed"]
      )

      File.mkdir_p!(workspace)
      parent = self()
      shutdown_marker = Path.join(workspace, "worker-shutdown-established")

      agent_pid =
        spawn(fn ->
          Process.flag(:trap_exit, true)

          receive do
            {:EXIT, _from, :shutdown} ->
              send(parent, {:worker_shutdown_write, File.write(shutdown_marker, "stopped")})
          end
        end)

      running_entry = %{
        pid: agent_pid,
        ref: nil,
        run_id: "run-terminal-ledger-block",
        retry_attempt: 2,
        identifier: issue_identifier,
        issue: %Issue{id: issue_id, identifier: issue_identifier, state: "In Progress"},
        worker_host: nil,
        workspace_path: workspace,
        workspace_root: test_root,
        started_at: DateTime.utc_now()
      }

      state = %Orchestrator.State{
        run_ledger_path: blocked_path,
        runner_generation: "runner-terminal-ledger-block",
        running: %{issue_id => running_entry},
        claimed: MapSet.new([issue_id]),
        codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
      }

      terminal_issue = %Issue{id: issue_id, identifier: issue_identifier, state: "Closed"}
      blocked_state = Orchestrator.reconcile_issue_states_for_test([terminal_issue], state)

      assert blocked_state.running[issue_id].terminal_pending.transition == "run_stopped"
      assert MapSet.member?(blocked_state.claimed, issue_id)
      assert Process.alive?(agent_pid)
      assert File.exists?(workspace)

      seed_running_ledger!(valid_path, blocked_state.running[issue_id])

      recovered_state =
        blocked_state
        |> Map.put(:run_ledger_path, valid_path)
        |> Orchestrator.retry_pending_terminal_transitions_for_test()

      assert_receive {:worker_shutdown_write, :ok}
      refute Map.has_key?(recovered_state.running, issue_id)
      refute MapSet.member?(recovered_state.claimed, issue_id)
      refute Process.alive?(agent_pid)
      refute File.exists?(workspace)

      assert {:ok, [_claim, _started, event, cleanup_event]} = RunLedger.read_events(valid_path)
      assert event["transition"] == "run_stopped"
      assert event["terminal_reason"] == "tracker_terminal"
      assert cleanup_event["transition"] == "workspace_cleanup_completed"
      assert cleanup_event["workspace_path"] == workspace
    after
      File.rm_rf(test_root)
    end
  end

  test "missing running issues stop active agents without cleaning the workspace" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-missing-running-reconcile-#{System.unique_integer([:positive])}"
      )

    previous_memory_issues = Application.get_env(:symphony_elixir, :memory_tracker_issues)
    issue_id = "issue-missing"
    issue_identifier = "MT-557"

    try do
      write_workflow_file!(Workflow.workflow_file_path(),
        tracker_kind: "memory",
        workspace_root: test_root,
        tracker_active_states: ["Todo", "In Progress", "In Review"],
        tracker_terminal_states: ["Closed", "Cancelled", "Canceled", "Duplicate"],
        poll_interval_ms: 30_000
      )

      Application.put_env(:symphony_elixir, :memory_tracker_issues, [])

      orchestrator_name = Module.concat(__MODULE__, :MissingRunningIssueOrchestrator)
      {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

      on_exit(fn ->
        restore_app_env(:memory_tracker_issues, previous_memory_issues)

        if Process.alive?(pid) do
          Process.exit(pid, :normal)
        end
      end)

      Process.sleep(50)

      assert {:ok, workspace} =
               SymphonyElixir.PathSafety.canonicalize(Path.join(test_root, issue_identifier))

      File.mkdir_p!(workspace)

      agent_pid =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      initial_state = :sys.get_state(pid)

      running_entry = %{
        pid: agent_pid,
        ref: nil,
        run_id: "run-missing",
        retry_attempt: 0,
        identifier: issue_identifier,
        issue: %Issue{id: issue_id, state: "In Progress", identifier: issue_identifier},
        started_at: DateTime.utc_now()
      }

      :sys.replace_state(pid, fn _ ->
        initial_state
        |> Map.put(:running, %{issue_id => running_entry})
        |> Map.put(:claimed, MapSet.new([issue_id]))
        |> Map.put(:retry_attempts, %{})
      end)

      send(pid, :tick)
      Process.sleep(100)
      state = :sys.get_state(pid)

      refute Map.has_key?(state.running, issue_id)
      refute MapSet.member?(state.claimed, issue_id)
      refute Process.alive?(agent_pid)
      assert File.exists?(workspace)
    after
      restore_app_env(:memory_tracker_issues, previous_memory_issues)
      File.rm_rf(test_root)
    end
  end

  test "reconcile updates running issue state for active issues" do
    issue_id = "issue-3"

    state = %Orchestrator.State{
      running: %{
        issue_id => %{
          pid: self(),
          ref: nil,
          identifier: "MT-557",
          issue: %Issue{
            id: issue_id,
            identifier: "MT-557",
            state: "Todo"
          },
          started_at: DateTime.utc_now()
        }
      },
      claimed: MapSet.new([issue_id]),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
      retry_attempts: %{}
    }

    issue = %Issue{
      id: issue_id,
      identifier: "MT-557",
      state: "In Progress",
      title: "Active state refresh",
      description: "State should be refreshed",
      labels: []
    }

    updated_state = Orchestrator.reconcile_issue_states_for_test([issue], state)
    updated_entry = updated_state.running[issue_id]

    assert Map.has_key?(updated_state.running, issue_id)
    assert MapSet.member?(updated_state.claimed, issue_id)
    assert updated_entry.issue.state == "In Progress"
  end

  test "reconcile stops running issue when it is reassigned away from this worker" do
    issue_id = "issue-reassigned"

    agent_pid =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    state = %Orchestrator.State{
      running: %{
        issue_id => %{
          pid: agent_pid,
          ref: nil,
          identifier: "MT-561",
          issue: %Issue{
            id: issue_id,
            identifier: "MT-561",
            state: "In Progress",
            assigned_to_worker: true
          },
          started_at: DateTime.utc_now()
        }
      },
      claimed: MapSet.new([issue_id]),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
      retry_attempts: %{}
    }

    issue = %Issue{
      id: issue_id,
      identifier: "MT-561",
      state: "In Progress",
      title: "Reassigned active issue",
      description: "Worker should stop",
      labels: [],
      assigned_to_worker: false
    }

    updated_state = Orchestrator.reconcile_issue_states_for_test([issue], state)

    refute Map.has_key?(updated_state.running, issue_id)
    refute MapSet.member?(updated_state.claimed, issue_id)
    refute Process.alive?(agent_pid)
  end

  test "normal worker exit schedules active-state continuation retry" do
    issue_id = "issue-resume"
    ref = make_ref()
    orchestrator_name = Module.concat(__MODULE__, :ContinuationOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    initial_state = :sys.get_state(pid)

    running_entry = %{
      pid: self(),
      ref: ref,
      run_id: "run-resume",
      retry_attempt: 0,
      identifier: "MT-558",
      issue: %Issue{id: issue_id, identifier: "MT-558", state: "In Progress"},
      started_at: DateTime.utc_now()
    }

    :sys.replace_state(pid, fn _ ->
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.new([issue_id]))
      |> Map.put(:retry_attempts, %{})
    end)

    scheduled_from_ms = System.monotonic_time(:millisecond)
    send(pid, {:DOWN, ref, :process, self(), :normal})
    Process.sleep(50)
    state = :sys.get_state(pid)

    refute Map.has_key?(state.running, issue_id)
    assert MapSet.member?(state.completed, issue_id)
    assert %{attempt: 1, due_at_ms: due_at_ms} = state.retry_attempts[issue_id]
    assert is_integer(due_at_ms)
    assert_scheduled_delay(due_at_ms, scheduled_from_ms, 1_000)
  end

  test "abnormal worker exit increments retry attempt progressively" do
    issue_id = "issue-crash"
    ref = make_ref()
    orchestrator_name = Module.concat(__MODULE__, :CrashRetryOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    initial_state = :sys.get_state(pid)

    running_entry = %{
      pid: self(),
      ref: ref,
      run_id: "run-crash",
      identifier: "MT-559",
      retry_attempt: 2,
      issue: %Issue{id: issue_id, identifier: "MT-559", state: "In Progress"},
      started_at: DateTime.utc_now()
    }

    :sys.replace_state(pid, fn _ ->
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.new([issue_id]))
      |> Map.put(:retry_attempts, %{})
    end)

    scheduled_from_ms = System.monotonic_time(:millisecond)
    send(pid, {:DOWN, ref, :process, self(), :boom})
    Process.sleep(50)
    state = :sys.get_state(pid)

    assert %{attempt: 3, due_at_ms: due_at_ms, identifier: "MT-559", error: "agent_exit"} =
             state.retry_attempts[issue_id]

    assert_scheduled_delay(due_at_ms, scheduled_from_ms, 40_000)
  end

  test "first abnormal worker exit waits before retrying" do
    issue_id = "issue-crash-initial"
    ref = make_ref()
    orchestrator_name = Module.concat(__MODULE__, :InitialCrashRetryOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    initial_state = :sys.get_state(pid)

    running_entry = %{
      pid: self(),
      ref: ref,
      run_id: "run-crash-initial",
      retry_attempt: 0,
      identifier: "MT-560",
      issue: %Issue{id: issue_id, identifier: "MT-560", state: "In Progress"},
      started_at: DateTime.utc_now()
    }

    :sys.replace_state(pid, fn _ ->
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.new([issue_id]))
      |> Map.put(:retry_attempts, %{})
    end)

    scheduled_from_ms = System.monotonic_time(:millisecond)
    send(pid, {:DOWN, ref, :process, self(), :boom})
    Process.sleep(50)
    state = :sys.get_state(pid)

    assert %{attempt: 1, due_at_ms: due_at_ms, identifier: "MT-560", error: "agent_exit"} =
             state.retry_attempts[issue_id]

    assert_scheduled_delay(due_at_ms, scheduled_from_ms, 10_000)
  end

  test "stale retry timer messages do not consume newer retry entries" do
    issue_id = "issue-stale-retry"
    orchestrator_name = Module.concat(__MODULE__, :StaleRetryOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    initial_state = :sys.get_state(pid)
    current_retry_token = make_ref()
    stale_retry_token = make_ref()

    :sys.replace_state(pid, fn _ ->
      initial_state
      |> Map.put(:retry_attempts, %{
        issue_id => %{
          attempt: 2,
          timer_ref: nil,
          retry_token: current_retry_token,
          due_at_ms: System.monotonic_time(:millisecond) + 30_000,
          identifier: "MT-561",
          error: "agent_exit"
        }
      })
    end)

    send(pid, {:retry_issue, issue_id, stale_retry_token})
    Process.sleep(50)

    assert %{
             attempt: 2,
             retry_token: ^current_retry_token,
             identifier: "MT-561",
             error: "agent_exit"
           } = :sys.get_state(pid).retry_attempts[issue_id]
  end

  test "manual refresh coalesces repeated requests and ignores superseded ticks" do
    now_ms = System.monotonic_time(:millisecond)
    stale_tick_token = make_ref()

    state = %Orchestrator.State{
      poll_interval_ms: 30_000,
      max_concurrent_agents: 1,
      next_poll_due_at_ms: now_ms + 30_000,
      poll_check_in_progress: false,
      tick_timer_ref: nil,
      tick_token: stale_tick_token,
      running: %{"issue-existing" => %{run_id: "run-existing"}},
      claimed: MapSet.new(["issue-existing"]),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
      codex_rate_limits: nil
    }

    assert {:reply, %{queued: true, coalesced: false}, refreshed_state} =
             Orchestrator.handle_call(:request_refresh, {self(), make_ref()}, state)

    assert is_reference(refreshed_state.tick_timer_ref)
    assert is_reference(refreshed_state.tick_token)
    refute refreshed_state.tick_token == stale_tick_token
    assert refreshed_state.next_poll_due_at_ms <= System.monotonic_time(:millisecond)
    assert refreshed_state.running == state.running
    assert refreshed_state.claimed == state.claimed

    assert {:reply, %{queued: true, coalesced: true}, coalesced_state} =
             Orchestrator.handle_call(:request_refresh, {self(), make_ref()}, refreshed_state)

    assert coalesced_state.tick_token == refreshed_state.tick_token
    assert coalesced_state.running == state.running
    assert coalesced_state.claimed == state.claimed
    assert {:noreply, ^coalesced_state} = Orchestrator.handle_info({:tick, stale_tick_token}, coalesced_state)
  end

  test "empty operator allowlist disables comment reconciliation" do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory")

    issue_id = "issue-operator-disabled"

    assert {:ok, wait} =
             SymphonyElixir.OperatorWait.new("waiting_secret", %{
               issue_id: issue_id,
               identifier: "MT-OPERATOR-DISABLED",
               run_id: "run-operator-disabled",
               parked_at: ~U[2026-08-03 10:00:00Z]
             })

    Application.put_env(:symphony_elixir, :memory_tracker_comments, %{
      issue_id => [
        %SymphonyElixir.Linear.Comment{
          id: "disabled-retry",
          body: "$retry",
          created_at: ~U[2026-08-03 10:00:01Z],
          author_id: "operator-1"
        }
      ]
    })

    state = %Orchestrator.State{
      poll_interval_ms: 30_000,
      max_concurrent_agents: 1,
      run_ledger_path: nil,
      dispatch_paused: true,
      parked: %{issue_id => wait},
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    assert {:noreply, reconciled_state} = Orchestrator.handle_info(:run_poll_cycle, state)
    assert reconciled_state.parked[issue_id].wait_id == wait.wait_id
    assert reconciled_state.operator_comment_cursors == %{}

    if is_reference(reconciled_state.tick_timer_ref),
      do: Process.cancel_timer(reconciled_state.tick_timer_ref)
  end

  test "Linear retry command resumes a matching wait exactly once while globally paused" do
    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      tracker_operator_user_ids: ["operator-1"]
    )

    ledger_path =
      Path.join(
        System.tmp_dir!(),
        "symphony-operator-retry-#{RunLedger.new_id("test")}/events.jsonl"
      )

    issue_id = "issue-operator-retry"
    cursor_at = ~U[2026-08-03 10:00:00Z]
    command_at = ~U[2026-08-03 10:00:01Z]

    assert {:ok, wait} =
             SymphonyElixir.OperatorWait.new("waiting_secret", %{
               issue_id: issue_id,
               identifier: "MT-OPERATOR-RETRY",
               run_id: "run-operator-retry",
               parked_at: cursor_at
             })

    seed_parked_ledger!(ledger_path, wait)

    comment = %SymphonyElixir.Linear.Comment{
      id: "comment-retry",
      body: "$retry after reconnect",
      created_at: command_at,
      author_id: "operator-1"
    }

    Application.put_env(:symphony_elixir, :memory_tracker_comments, %{
      issue_id => [comment, :invalid_comment]
    })

    state = %Orchestrator.State{
      poll_interval_ms: 30_000,
      max_concurrent_agents: 1,
      run_ledger_path: ledger_path,
      runner_generation: "runner-operator-retry",
      dispatch_paused: true,
      parked: %{issue_id => wait},
      operator_comment_cursors: %{
        issue_id => %{created_at: cursor_at, comment_ids: MapSet.new()}
      },
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    assert {:noreply, resumed_state} = Orchestrator.handle_info(:run_poll_cycle, state)
    refute Map.has_key?(resumed_state.parked, issue_id)
    assert MapSet.member?(resumed_state.processed_operator_comment_ids, comment.id)

    assert {:noreply, repeated_state} =
             Orchestrator.handle_info(:run_poll_cycle, resumed_state)

    if is_reference(repeated_state.tick_timer_ref),
      do: Process.cancel_timer(repeated_state.tick_timer_ref)

    assert {:ok, events} = RunLedger.read_events(ledger_path)
    assert Enum.count(events, &(&1["transition"] == "resume_queued")) == 1
    assert Enum.count(events, &(&1["transition"] == "operator_command_applied")) == 1
  end

  test "first operator cursor does not execute historical comments for recovered waits" do
    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      tracker_operator_user_ids: ["operator-1"]
    )

    ledger_path =
      Path.join(
        System.tmp_dir!(),
        "symphony-operator-migration-#{RunLedger.new_id("test")}/events.jsonl"
      )

    issue_id = "issue-recovered-operator-wait"
    parked_at = DateTime.add(DateTime.utc_now(), -3_600, :second)
    historical_comment_at = DateTime.add(parked_at, 60, :second)

    assert {:ok, wait} =
             SymphonyElixir.OperatorWait.new("waiting_secret", %{
               issue_id: issue_id,
               identifier: "MT-RECOVERED-WAIT",
               run_id: "run-recovered-wait",
               parked_at: parked_at
             })

    Application.put_env(:symphony_elixir, :memory_tracker_comments, %{
      issue_id => [
        %SymphonyElixir.Linear.Comment{
          id: "historical-retry",
          body: "$retry",
          created_at: historical_comment_at,
          author_id: "operator-1"
        }
      ]
    })

    state = %Orchestrator.State{
      poll_interval_ms: 30_000,
      max_concurrent_agents: 1,
      run_ledger_path: ledger_path,
      runner_generation: "runner-operator-migration",
      dispatch_paused: true,
      parked: %{issue_id => wait},
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    assert {:noreply, reconciled_state} = Orchestrator.handle_info(:run_poll_cycle, state)
    assert reconciled_state.parked[issue_id].wait_id == wait.wait_id

    assert %{created_at: cursor_at, comment_ids: comment_ids} =
             reconciled_state.operator_comment_cursors[issue_id]

    assert DateTime.compare(cursor_at, historical_comment_at) == :gt
    assert MapSet.size(comment_ids) == 0

    if is_reference(reconciled_state.tick_timer_ref),
      do: Process.cancel_timer(reconciled_state.tick_timer_ref)

    assert {:ok, events} = RunLedger.read_events(ledger_path)
    refute Enum.any?(events, &(&1["transition"] == "resume_queued"))
    refute Enum.any?(events, &(&1["transition"] == "operator_command_applied"))
  end

  test "Linear stop command durably parks a running issue for operator retry" do
    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      tracker_operator_user_ids: ["operator-1"]
    )

    ledger_path =
      Path.join(
        System.tmp_dir!(),
        "symphony-operator-stop-#{RunLedger.new_id("test")}/events.jsonl"
      )

    issue_id = "issue-operator-stop"
    cursor_at = DateTime.utc_now()
    command_at = DateTime.add(cursor_at, 1, :second)

    issue = %Issue{
      id: issue_id,
      identifier: "MT-OPERATOR-STOP",
      state: "In Progress",
      title: "Stop this worker",
      assigned_to_worker: true
    }

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue])

    comment = %SymphonyElixir.Linear.Comment{
      id: "comment-stop",
      body: "$stop",
      created_at: command_at,
      author_id: "operator-1"
    }

    Application.put_env(:symphony_elixir, :memory_tracker_comments, %{
      issue_id => [comment]
    })

    assert Config.settings!().tracker.operator_user_ids == ["operator-1"]
    assert {:ok, [^comment]} = Tracker.fetch_comments_since(issue_id, cursor_at)

    agent_pid = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> if Process.alive?(agent_pid), do: Process.exit(agent_pid, :kill) end)

    running_entry = %{
      pid: agent_pid,
      ref: nil,
      run_id: "run-operator-stop",
      retry_attempt: 0,
      identifier: issue.identifier,
      issue: issue,
      started_at: cursor_at,
      codex_input_tokens: 0,
      codex_output_tokens: 0,
      codex_total_tokens: 0
    }

    state = %Orchestrator.State{
      poll_interval_ms: 30_000,
      max_concurrent_agents: 1,
      run_ledger_path: ledger_path,
      runner_generation: "runner-operator-stop",
      dispatch_paused: true,
      running: %{issue_id => running_entry},
      claimed: MapSet.new([issue_id]),
      operator_comment_cursors: %{
        issue_id => %{created_at: cursor_at, comment_ids: MapSet.new()}
      },
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    seed_running_ledger!(ledger_path, running_entry)

    assert {:noreply, stopped_state} = Orchestrator.handle_info(:run_poll_cycle, state)
    refute Map.has_key?(stopped_state.running, issue_id)

    assert Map.has_key?(stopped_state.parked, issue_id),
           "expected operator stop to park issue; ledger=#{inspect(RunLedger.read_events(ledger_path))}"

    assert stopped_state.parked[issue_id].reason == "operator_stopped"
    assert stopped_state.parked[issue_id].allowed_actions == ["retry", "reject"]
    refute Process.alive?(agent_pid)

    if is_reference(stopped_state.tick_timer_ref),
      do: Process.cancel_timer(stopped_state.tick_timer_ref)

    assert {:ok, events} = RunLedger.read_events(ledger_path)
    assert Enum.count(events, &(&1["transition"] == "run_parked")) == 1
    assert Enum.count(events, &(&1["transition"] == "operator_command_applied")) == 1
  end

  test "select_worker_host_for_test skips full ssh hosts under the shared per-host cap" do
    write_workflow_file!(Workflow.workflow_file_path(),
      worker_ssh_hosts: ["worker-a", "worker-b"],
      worker_max_concurrent_agents_per_host: 1
    )

    state = %Orchestrator.State{
      running: %{
        "issue-1" => %{worker_host: "worker-a"}
      }
    }

    assert Orchestrator.select_worker_host_for_test(state, nil) == "worker-b"
  end

  test "select_worker_host_for_test returns no_worker_capacity when every ssh host is full" do
    write_workflow_file!(Workflow.workflow_file_path(),
      worker_ssh_hosts: ["worker-a", "worker-b"],
      worker_max_concurrent_agents_per_host: 1
    )

    state = %Orchestrator.State{
      running: %{
        "issue-1" => %{worker_host: "worker-a"},
        "issue-2" => %{worker_host: "worker-b"}
      }
    }

    assert Orchestrator.select_worker_host_for_test(state, nil) == :no_worker_capacity
  end

  test "select_worker_host_for_test keeps the preferred ssh host when it still has capacity" do
    write_workflow_file!(Workflow.workflow_file_path(),
      worker_ssh_hosts: ["worker-a", "worker-b"],
      worker_max_concurrent_agents_per_host: 2
    )

    state = %Orchestrator.State{
      running: %{
        "issue-1" => %{worker_host: "worker-a"},
        "issue-2" => %{worker_host: "worker-b"}
      }
    }

    assert Orchestrator.select_worker_host_for_test(state, "worker-a") == "worker-a"
  end

  test "strict workspace affinity never hops to another ssh host" do
    write_workflow_file!(Workflow.workflow_file_path(),
      worker_ssh_hosts: ["worker-a", "worker-b"],
      worker_max_concurrent_agents_per_host: 1
    )

    state = %Orchestrator.State{
      running: %{
        "issue-1" => %{worker_host: "worker-a"}
      }
    }

    assert Orchestrator.select_worker_host_for_test(state, "worker-a") == "worker-b"

    assert Orchestrator.select_worker_host_for_test(state, "worker-a", true) ==
             :no_worker_capacity

    assert Orchestrator.select_worker_host_for_test(state, "retired-worker", true) ==
             :affinity_unavailable
  end

  test "local workspace affinity is validated before after-create hooks" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-workspace-affinity-#{System.unique_integer([:positive])}"
      )

    workspace_root = Path.join(test_root, "workspaces")
    hook_marker = Path.join(test_root, "after-create-ran")

    on_exit(fn -> File.rm_rf(test_root) end)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: workspace_root,
      hook_after_create: "touch #{hook_marker}"
    )

    issue = %Issue{id: "issue-affinity", identifier: "MT-AFFINITY", state: "In Progress"}
    expected_workspace = Path.join(workspace_root, issue.identifier)

    assert {:ok, actual_workspace} =
             Workspace.create_for_issue(issue, nil, expected_workspace_path: expected_workspace)

    assert {:ok, canonical_expected_workspace} =
             SymphonyElixir.PathSafety.canonicalize(expected_workspace)

    assert actual_workspace == canonical_expected_workspace

    File.rm_rf!(actual_workspace)
    File.rm(hook_marker)

    assert {:error, {:workspace_affinity_mismatch, _expected, ^actual_workspace, nil}} =
             Workspace.create_for_issue(issue, nil, expected_workspace_path: Path.join(workspace_root, "MT-OTHER"))

    refute File.exists?(hook_marker)
  end

  test "prepared affinity is acknowledged before workspace hooks or Codex" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-prepared-affinity-#{System.unique_integer([:positive])}"
      )

    workspace_root = Path.join(test_root, "workspaces")
    after_create_marker = Path.join(workspace_root, "MT-PREPARED/after-create.marker")
    before_run_marker = Path.join(workspace_root, "MT-PREPARED/before-run.marker")

    on_exit(fn -> File.rm_rf(test_root) end)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: workspace_root,
      hook_after_create: "touch after-create.marker",
      hook_before_run: "touch before-run.marker",
      codex_command: "/usr/bin/false"
    )

    issue = %Issue{
      id: "issue-prepared-affinity",
      identifier: "MT-PREPARED",
      title: "Prepared affinity",
      state: "In Progress"
    }

    assert {:ok, prepared} = Workspace.prepare_for_issue(issue, nil)
    assert {:ok, canonical_root} = SymphonyElixir.PathSafety.canonicalize(workspace_root)
    assert prepared.path == Path.join(canonical_root, issue.identifier)
    assert prepared.root == canonical_root
    refute File.exists?(after_create_marker)
    refute File.exists?(before_run_marker)

    parent = self()

    task =
      Task.async(fn ->
        try do
          AgentRunner.run(issue, parent,
            prepared_workspace: prepared,
            expected_workspace_path: prepared.path,
            runtime_ack_required: true,
            run_id: "run-prepared-affinity"
          )
        rescue
          RuntimeError -> :agent_failed_after_hooks
        end
      end)

    assert_receive {:worker_runtime_info, issue_id, runtime_info, worker_pid, acknowledgment_ref},
                   1_000

    assert issue_id == issue.id
    assert runtime_info.run_id == "run-prepared-affinity"
    assert runtime_info.workspace_path == prepared.path
    assert runtime_info.workspace_root == prepared.root
    refute File.exists?(after_create_marker)
    refute File.exists?(before_run_marker)

    send(worker_pid, {:worker_runtime_ack, acknowledgment_ref, :ok})
    assert Task.await(task, 5_000) == :agent_failed_after_hooks
    assert File.exists?(after_create_marker)
    assert File.exists?(before_run_marker)
  end

  defp assert_scheduled_delay(due_at_ms, scheduled_from_ms, expected_delay_ms) do
    scheduled_delay_ms = due_at_ms - scheduled_from_ms

    assert scheduled_delay_ms >= expected_delay_ms
    assert scheduled_delay_ms <= expected_delay_ms + 5_000
  end

  defp restore_app_env(key, nil), do: Application.delete_env(:symphony_elixir, key)
  defp restore_app_env(key, value), do: Application.put_env(:symphony_elixir, key, value)

  test "fetch issues by states with empty state set is a no-op" do
    assert {:ok, []} = Client.fetch_issues_by_states([])
  end

  test "prompt builder renders issue and attempt values from workflow template" do
    workflow_prompt =
      "Ticket {{ issue.identifier }} {{ issue.title }} labels={{ issue.labels }} attempt={{ attempt }}"

    write_workflow_file!(Workflow.workflow_file_path(), prompt: workflow_prompt)

    issue = %Issue{
      identifier: "S-1",
      title: "Refactor backend request path",
      description: "Replace transport layer",
      state: "Todo",
      url: "https://example.org/issues/S-1",
      labels: ["backend"]
    }

    prompt = PromptBuilder.build_prompt(issue, attempt: 3)

    assert prompt =~ "Ticket S-1 Refactor backend request path"
    assert prompt =~ "labels=backend"
    assert prompt =~ "attempt=3"
  end

  test "prompt builder exposes immutable run identity metadata" do
    write_workflow_file!(
      Workflow.workflow_file_path(),
      prompt: "run={{ run.id }} attempt={{ run.attempt }} stage={{ run.stage }} generation={{ run.runner_generation }}"
    )

    issue = %Issue{
      id: "issue-run-identity",
      identifier: "MT-IDENTITY",
      title: "Run identity",
      description: nil,
      state: "Todo"
    }

    prompt =
      PromptBuilder.build_prompt(issue,
        attempt: 4,
        run_id: "run-stable",
        stage: "running",
        runner_generation: "runner-generation"
      )

    assert prompt ==
             "run=run-stable attempt=4 stage=running generation=runner-generation"
  end

  test "prompt builder renders only the runtime prompt section when present" do
    workflow_prompt = """
    # Operator workflow

    The operator mentions `## Symphony Runtime Prompt` inline.

    Supervisor Watch Loop must observe runner pickup.

    ## Symphony Runtime Prompt

    Ticket {{ issue.identifier }} {{ issue.title }}
    """

    write_workflow_file!(Workflow.workflow_file_path(), prompt: workflow_prompt)

    issue = %Issue{
      identifier: "S-2",
      title: "Post canary window",
      description: "Worker should not inherit operator-only instructions",
      state: "Agent Ready",
      url: "https://example.org/issues/S-2",
      labels: ["production-risk"]
    }

    prompt = PromptBuilder.build_prompt(issue)

    assert prompt =~ "Ticket S-2 Post canary window"
    assert prompt =~ "## Symphony Runtime Prompt"
    refute prompt =~ "Supervisor Watch Loop"
    refute prompt =~ "operator mentions"
  end

  test "prompt builder renders issue datetime fields without crashing" do
    workflow_prompt = "Ticket {{ issue.identifier }} created={{ issue.created_at }} updated={{ issue.updated_at }}"

    write_workflow_file!(Workflow.workflow_file_path(), prompt: workflow_prompt)

    created_at = DateTime.from_naive!(~N[2026-02-26 18:06:48], "Etc/UTC")
    updated_at = DateTime.from_naive!(~N[2026-02-26 18:07:03], "Etc/UTC")

    issue = %Issue{
      identifier: "MT-697",
      title: "Live smoke",
      description: "Prompt should serialize datetimes",
      state: "Todo",
      url: "https://example.org/issues/MT-697",
      labels: [],
      created_at: created_at,
      updated_at: updated_at
    }

    prompt = PromptBuilder.build_prompt(issue)

    assert prompt =~ "Ticket MT-697"
    assert prompt =~ "created=2026-02-26T18:06:48Z"
    assert prompt =~ "updated=2026-02-26T18:07:03Z"
  end

  test "prompt builder normalizes nested date-like values, maps, and structs in issue fields" do
    write_workflow_file!(Workflow.workflow_file_path(), prompt: "Ticket {{ issue.identifier }}")

    issue = %Issue{
      identifier: "MT-701",
      title: "Serialize nested values",
      description: "Prompt builder should normalize nested terms",
      state: "Todo",
      url: "https://example.org/issues/MT-701",
      labels: [
        ~N[2026-02-27 12:34:56],
        ~D[2026-02-28],
        ~T[12:34:56],
        %{phase: "test"},
        URI.parse("https://example.org/issues/MT-701")
      ]
    }

    assert PromptBuilder.build_prompt(issue) == "Ticket MT-701"
  end

  test "prompt builder uses strict variable rendering" do
    workflow_prompt = "Work on ticket {{ missing.ticket_id }} and follow these steps."

    write_workflow_file!(Workflow.workflow_file_path(), prompt: workflow_prompt)

    issue = %Issue{
      identifier: "MT-123",
      title: "Investigate broken sync",
      description: "Reproduce and fix",
      state: "In Progress",
      url: "https://example.org/issues/MT-123",
      labels: ["bug"]
    }

    assert_raise Solid.RenderError, fn ->
      PromptBuilder.build_prompt(issue)
    end
  end

  test "prompt builder surfaces invalid template content with prompt context" do
    write_workflow_file!(Workflow.workflow_file_path(), prompt: "{% if issue.identifier %}")

    issue = %Issue{
      identifier: "MT-999",
      title: "Broken prompt",
      description: "Invalid template syntax",
      state: "Todo",
      url: "https://example.org/issues/MT-999",
      labels: []
    }

    assert_raise RuntimeError, ~r/template_parse_error:.*template="/s, fn ->
      PromptBuilder.build_prompt(issue)
    end
  end

  test "prompt builder uses a sensible default template when workflow prompt is blank" do
    write_workflow_file!(Workflow.workflow_file_path(), prompt: "   \n")

    issue = %Issue{
      identifier: "MT-777",
      title: "Make fallback prompt useful",
      description: "Include enough issue context to start working.",
      state: "In Progress",
      url: "https://example.org/issues/MT-777",
      labels: ["prompt"]
    }

    prompt = PromptBuilder.build_prompt(issue)

    assert prompt =~ "You are working on a Linear issue."
    assert prompt =~ "Identifier: MT-777"
    assert prompt =~ "Title: Make fallback prompt useful"
    assert prompt =~ "Body:"
    assert prompt =~ "Include enough issue context to start working."
    assert Config.workflow_prompt() =~ "{{ issue.identifier }}"
    assert Config.workflow_prompt() =~ "{{ issue.title }}"
    assert Config.workflow_prompt() =~ "{{ issue.description }}"
  end

  test "prompt builder default template handles missing issue body" do
    write_workflow_file!(Workflow.workflow_file_path(), prompt: "")

    issue = %Issue{
      identifier: "MT-778",
      title: "Handle empty body",
      description: nil,
      state: "Todo",
      url: "https://example.org/issues/MT-778",
      labels: []
    }

    prompt = PromptBuilder.build_prompt(issue)

    assert prompt =~ "Identifier: MT-778"
    assert prompt =~ "Title: Handle empty body"
    assert prompt =~ "No description provided."
  end

  test "prompt builder reports workflow load failures separately from template parse errors" do
    original_workflow_path = Workflow.workflow_file_path()
    workflow_store_pid = Process.whereis(SymphonyElixir.WorkflowStore)

    on_exit(fn ->
      Workflow.set_workflow_file_path(original_workflow_path)

      if is_pid(workflow_store_pid) and is_nil(Process.whereis(SymphonyElixir.WorkflowStore)) do
        Supervisor.restart_child(SymphonyElixir.Supervisor, SymphonyElixir.WorkflowStore)
      end
    end)

    assert :ok = Supervisor.terminate_child(SymphonyElixir.Supervisor, SymphonyElixir.WorkflowStore)

    Workflow.set_workflow_file_path(Path.join(System.tmp_dir!(), "missing-workflow-#{System.unique_integer([:positive])}.md"))

    issue = %Issue{
      identifier: "MT-780",
      title: "Workflow unavailable",
      description: "Missing workflow file",
      state: "Todo",
      url: "https://example.org/issues/MT-780",
      labels: []
    }

    assert_raise RuntimeError, ~r/workflow_unavailable:/, fn ->
      PromptBuilder.build_prompt(issue)
    end
  end

  test "in-repo WORKFLOW.md renders correctly" do
    workflow_path = Workflow.workflow_file_path()
    Workflow.set_workflow_file_path(Path.expand("WORKFLOW.md", File.cwd!()))

    issue = %Issue{
      identifier: "MT-616",
      title: "Use rich templates for WORKFLOW.md",
      description: "Render with rich template variables",
      state: "In Progress",
      url: "https://example.org/issues/MT-616/use-rich-templates-for-workflowmd",
      labels: ["templating", "workflow"]
    }

    on_exit(fn -> Workflow.set_workflow_file_path(workflow_path) end)

    prompt = PromptBuilder.build_prompt(issue, attempt: 2)

    assert prompt =~ "You are working on a Linear ticket `MT-616`"
    assert prompt =~ "Issue context:"
    assert prompt =~ "Identifier: MT-616"
    assert prompt =~ "Title: Use rich templates for WORKFLOW.md"
    assert prompt =~ "Current status: In Progress"
    assert prompt =~ "https://example.org/issues/MT-616/use-rich-templates-for-workflowmd"
    assert prompt =~ "This is an unattended orchestration session."
    assert prompt =~ "Only stop early for a true blocker"
    assert prompt =~ "Do not include \"next steps for user\""
    assert prompt =~ "open and follow `.codex/skills/land/SKILL.md`"
    assert prompt =~ "Do not call `gh pr merge` directly"
    assert prompt =~ "Continuation context:"
    assert prompt =~ "retry attempt #2"
  end

  test "prompt builder adds continuation guidance for retries" do
    workflow_prompt = "{% if attempt %}Retry #" <> "{{ attempt }}" <> "{% endif %}"
    write_workflow_file!(Workflow.workflow_file_path(), prompt: workflow_prompt)

    issue = %Issue{
      identifier: "MT-201",
      title: "Continue autonomous ticket",
      description: "Retry flow",
      state: "In Progress",
      url: "https://example.org/issues/MT-201",
      labels: []
    }

    prompt = PromptBuilder.build_prompt(issue, attempt: 2)

    assert prompt == "Retry #2"
  end

  test "agent runner keeps workspace after successful codex run" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-agent-runner-retain-workspace-#{System.unique_integer([:positive])}"
      )

    try do
      template_repo = Path.join(test_root, "source")
      workspace_root = Path.join(test_root, "workspaces")
      codex_binary = Path.join(test_root, "fake-codex")

      File.mkdir_p!(template_repo)
      File.mkdir_p!(workspace_root)
      File.write!(Path.join(template_repo, "README.md"), "# test")
      System.cmd("git", ["-C", template_repo, "init", "-b", "main"])
      System.cmd("git", ["-C", template_repo, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", template_repo, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", template_repo, "add", "README.md"])
      System.cmd("git", ["-C", template_repo, "commit", "-m", "initial"])

      File.write!(codex_binary, """
      #!/bin/sh
      count=0
      while IFS= read -r line; do
        count=$((count + 1))
        case "$count" in
          1)
            printf '%s\\n' '{\"id\":1,\"result\":{}}'
            ;;
          2)
            ;;
          3)
            printf '%s\\n' '{\"id\":2,\"result\":{\"thread\":{\"id\":\"thread-1\"}}}'
            ;;
          4)
            printf '%s\\n' '{\"id\":3,\"result\":{\"turn\":{\"id\":\"turn-1\"}}}'
            printf '%s\\n' '{\"method\":\"turn/completed\"}'
            exit 0
            ;;
          *)
            ;;
        esac
      done
      """)

      File.chmod!(codex_binary, 0o755)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        hook_after_create: "cp #{Path.join(template_repo, "README.md")} README.md",
        codex_command: "#{codex_binary} app-server"
      )

      issue = %Issue{
        identifier: "S-99",
        title: "Smoke test",
        description: "Run and keep workspace",
        state: "In Progress",
        url: "https://example.org/issues/S-99",
        labels: ["backend"]
      }

      before = MapSet.new(File.ls!(workspace_root))
      assert :ok = AgentRunner.run(issue)
      entries_after = MapSet.new(File.ls!(workspace_root))

      created =
        MapSet.difference(entries_after, before) |> Enum.filter(&(&1 == "S-99"))

      created = MapSet.new(created)

      assert MapSet.size(created) == 1
      workspace_name = created |> Enum.to_list() |> List.first()
      assert workspace_name == "S-99"

      workspace = Path.join(workspace_root, workspace_name)
      assert File.exists?(workspace)
      assert File.exists?(Path.join(workspace, "README.md"))
    after
      File.rm_rf(test_root)
    end
  end

  test "agent runner forwards timestamped codex updates to recipient" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-agent-runner-updates-#{System.unique_integer([:positive])}"
      )

    try do
      template_repo = Path.join(test_root, "source")
      workspace_root = Path.join(test_root, "workspaces")
      codex_binary = Path.join(test_root, "fake-codex")

      File.mkdir_p!(template_repo)
      File.write!(Path.join(template_repo, "README.md"), "# test")
      System.cmd("git", ["-C", template_repo, "init", "-b", "main"])
      System.cmd("git", ["-C", template_repo, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", template_repo, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", template_repo, "add", "README.md"])
      System.cmd("git", ["-C", template_repo, "commit", "-m", "initial"])

      File.write!(
        codex_binary,
        """
        #!/bin/sh
        count=0
        while IFS= read -r line; do
          count=$((count + 1))
          case "$count" in
            1)
              printf '%s\\n' '{\"id\":1,\"result\":{}}'
              ;;
            2)
              printf '%s\\n' '{\"id\":2,\"result\":{\"thread\":{\"id\":\"thread-live\"}}}'
              ;;
            3)
              printf '%s\\n' '{\"id\":3,\"result\":{\"turn\":{\"id\":\"turn-live\"}}}'
              ;;
            4)
              printf '%s\\n' '{\"method\":\"turn/completed\"}'
              ;;
            *)
              ;;
          esac
        done
        """
      )

      File.chmod!(codex_binary, 0o755)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        hook_after_create: "cp #{Path.join(template_repo, "README.md")} README.md",
        codex_command: "#{codex_binary} app-server"
      )

      issue = %Issue{
        id: "issue-live-updates",
        identifier: "MT-99",
        title: "Smoke test",
        description: "Capture codex updates",
        state: "In Progress",
        url: "https://example.org/issues/MT-99",
        labels: ["backend"]
      }

      test_pid = self()

      assert :ok =
               AgentRunner.run(
                 issue,
                 test_pid,
                 issue_state_fetcher: fn [_issue_id] -> {:ok, [%{issue | state: "Done"}]} end
               )

      assert_receive {:codex_worker_update, "issue-live-updates",
                      %{
                        event: :session_started,
                        timestamp: %DateTime{},
                        session_id: session_id
                      }},
                     500

      assert session_id == "thread-live-turn-live"
    after
      File.rm_rf(test_root)
    end
  end

  test "agent runner surfaces ssh startup failures instead of silently hopping hosts" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-agent-runner-single-host-#{System.unique_integer([:positive])}"
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

      File.mkdir_p!(test_root)
      System.put_env("SYMP_TEST_SSH_TRACE", trace_file)
      System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

      File.write!(fake_ssh, """
      #!/bin/sh
      trace_file="${SYMP_TEST_SSH_TRACE:-/tmp/symphony-fake-ssh.trace}"
      printf 'ARGV:%s\\n' "$*" >> "$trace_file"

      case "$*" in
        *worker-a*"__SYMPHONY_WORKSPACE__"*)
          printf '%s\\n' 'worker-a prepare failed' >&2
          exit 75
          ;;
        *worker-b*"__SYMPHONY_WORKSPACE__"*)
          printf '%s\\t%s\\t%s\\n' '__SYMPHONY_WORKSPACE__' '1' '/remote/home/.symphony-remote-workspaces/MT-SSH-FAILOVER'
          exit 0
          ;;
        *)
          exit 0
          ;;
      esac
      """)

      File.chmod!(fake_ssh, 0o755)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: "~/.symphony-remote-workspaces",
        worker_ssh_hosts: ["worker-a", "worker-b"]
      )

      issue = %Issue{
        id: "issue-ssh-failover",
        identifier: "MT-SSH-FAILOVER",
        title: "Do not fail over within a single worker run",
        description: "Surface the startup failure to the orchestrator",
        state: "In Progress"
      }

      assert_raise RuntimeError, ~r/workspace_prepare_failed/, fn ->
        AgentRunner.run(issue, nil, worker_host: "worker-a")
      end

      trace = File.read!(trace_file)
      assert trace =~ "worker-a bash -lc"
      refute trace =~ "worker-b bash -lc"
    after
      File.rm_rf(test_root)
    end
  end

  test "agent runner continues with a follow-up turn while the issue remains active" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-agent-runner-continuation-#{System.unique_integer([:positive])}"
      )

    try do
      template_repo = Path.join(test_root, "source")
      workspace_root = Path.join(test_root, "workspaces")
      codex_binary = Path.join(test_root, "fake-codex")
      trace_file = Path.join(test_root, "codex.trace")

      File.mkdir_p!(template_repo)
      File.write!(Path.join(template_repo, "README.md"), "# test")
      System.cmd("git", ["-C", template_repo, "init", "-b", "main"])
      System.cmd("git", ["-C", template_repo, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", template_repo, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", template_repo, "add", "README.md"])
      System.cmd("git", ["-C", template_repo, "commit", "-m", "initial"])

      File.write!(codex_binary, """
      #!/bin/sh
      trace_file="${SYMP_TEST_CODEx_TRACE:-/tmp/codex.trace}"
      run_id="$(date +%s%N)-$$"
      printf 'RUN:%s\\n' "$run_id" >> "$trace_file"
      count=0

      while IFS= read -r line; do
        count=$((count + 1))
        printf 'JSON:%s\\n' "$line" >> "$trace_file"
        case "$count" in
          1)
            printf '%s\\n' '{"id":1,"result":{}}'
            ;;
          2)
            ;;
          3)
            printf '%s\\n' '{"id":2,"result":{"thread":{"id":"thread-cont"}}}'
            ;;
          4)
            printf '%s\\n' '{"id":3,"result":{"turn":{"id":"turn-cont-1"}}}'
            printf '%s\\n' '{"method":"turn/completed"}'
            ;;
          5)
            printf '%s\\n' '{"id":3,"result":{"turn":{"id":"turn-cont-2"}}}'
            printf '%s\\n' '{"method":"turn/completed"}'
            ;;
        esac
      done
      """)

      File.chmod!(codex_binary, 0o755)
      System.put_env("SYMP_TEST_CODEx_TRACE", trace_file)

      on_exit(fn -> System.delete_env("SYMP_TEST_CODEx_TRACE") end)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        hook_after_create: "cp #{Path.join(template_repo, "README.md")} README.md",
        codex_command: "#{codex_binary} app-server",
        max_turns: 3
      )

      parent = self()

      state_fetcher = fn [_issue_id] ->
        attempt = Process.get(:agent_turn_fetch_count, 0) + 1
        Process.put(:agent_turn_fetch_count, attempt)
        send(parent, {:issue_state_fetch, attempt})

        state =
          if attempt == 1 do
            "In Progress"
          else
            "Done"
          end

        {:ok,
         [
           %Issue{
             id: "issue-continue",
             identifier: "MT-247",
             title: "Continue until done",
             description: "Still active after first turn",
             state: state
           }
         ]}
      end

      issue = %Issue{
        id: "issue-continue",
        identifier: "MT-247",
        title: "Continue until done",
        description: "Still active after first turn",
        state: "In Progress",
        url: "https://example.org/issues/MT-247",
        labels: []
      }

      assert :ok = AgentRunner.run(issue, nil, issue_state_fetcher: state_fetcher)
      assert_receive {:issue_state_fetch, 1}
      assert_receive {:issue_state_fetch, 2}

      lines = File.read!(trace_file) |> String.split("\n", trim: true)

      assert length(Enum.filter(lines, &String.starts_with?(&1, "RUN:"))) == 1
      assert length(Enum.filter(lines, &String.contains?(&1, "\"method\":\"thread/start\""))) == 1

      turn_texts =
        lines
        |> Enum.filter(&String.starts_with?(&1, "JSON:"))
        |> Enum.map(&String.trim_leading(&1, "JSON:"))
        |> Enum.map(&Jason.decode!/1)
        |> Enum.filter(&(&1["method"] == "turn/start"))
        |> Enum.map(fn payload ->
          get_in(payload, ["params", "input"])
          |> Enum.map_join("\n", &Map.get(&1, "text", ""))
        end)

      assert length(turn_texts) == 2
      assert String.starts_with?(Enum.at(turn_texts, 0), "Symphony - MT-247 - Continue until done")
      assert Enum.at(turn_texts, 0) =~ "You are an agent for this repository."
      refute Enum.at(turn_texts, 1) =~ "You are an agent for this repository."
      assert Enum.at(turn_texts, 1) =~ "Continuation guidance:"
      assert Enum.at(turn_texts, 1) =~ "continuation turn #2 of 3"
    after
      System.delete_env("SYMP_TEST_CODEx_TRACE")
      File.rm_rf(test_root)
    end
  end

  test "agent runner stops continuing once agent.max_turns is reached" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-agent-runner-max-turns-#{System.unique_integer([:positive])}"
      )

    try do
      template_repo = Path.join(test_root, "source")
      workspace_root = Path.join(test_root, "workspaces")
      codex_binary = Path.join(test_root, "fake-codex")
      trace_file = Path.join(test_root, "codex.trace")

      File.mkdir_p!(template_repo)
      File.write!(Path.join(template_repo, "README.md"), "# test")
      System.cmd("git", ["-C", template_repo, "init", "-b", "main"])
      System.cmd("git", ["-C", template_repo, "config", "user.name", "Test User"])
      System.cmd("git", ["-C", template_repo, "config", "user.email", "test@example.com"])
      System.cmd("git", ["-C", template_repo, "add", "README.md"])
      System.cmd("git", ["-C", template_repo, "commit", "-m", "initial"])

      File.write!(codex_binary, """
      #!/bin/sh
      trace_file="${SYMP_TEST_CODEx_TRACE:-/tmp/codex.trace}"
      printf 'RUN\\n' >> "$trace_file"
      count=0

      while IFS= read -r line; do
        count=$((count + 1))
        printf 'JSON:%s\\n' "$line" >> "$trace_file"
        case "$count" in
          1)
            printf '%s\\n' '{"id":1,"result":{}}'
            ;;
          2)
            ;;
          3)
            printf '%s\\n' '{"id":2,"result":{"thread":{"id":"thread-max"}}}'
            ;;
          4)
            printf '%s\\n' '{"id":3,"result":{"turn":{"id":"turn-max-1"}}}'
            printf '%s\\n' '{"method":"turn/completed"}'
            ;;
          5)
            printf '%s\\n' '{"id":3,"result":{"turn":{"id":"turn-max-2"}}}'
            printf '%s\\n' '{"method":"turn/completed"}'
            ;;
        esac
      done
      """)

      File.chmod!(codex_binary, 0o755)
      System.put_env("SYMP_TEST_CODEx_TRACE", trace_file)

      on_exit(fn -> System.delete_env("SYMP_TEST_CODEx_TRACE") end)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        hook_after_create: "cp #{Path.join(template_repo, "README.md")} README.md",
        codex_command: "#{codex_binary} app-server",
        max_turns: 2
      )

      state_fetcher = fn [_issue_id] ->
        {:ok,
         [
           %Issue{
             id: "issue-max-turns",
             identifier: "MT-248",
             title: "Stop at max turns",
             description: "Still active",
             state: "In Progress"
           }
         ]}
      end

      issue = %Issue{
        id: "issue-max-turns",
        identifier: "MT-248",
        title: "Stop at max turns",
        description: "Still active",
        state: "In Progress",
        url: "https://example.org/issues/MT-248",
        labels: []
      }

      assert :ok =
               AgentRunner.run(issue, self(),
                 issue_state_fetcher: state_fetcher,
                 run_id: "run-max-turns"
               )

      assert_receive {:worker_budget_exhausted, "issue-max-turns",
                      %{
                        run_id: "run-max-turns",
                        terminal_reason: "turn_budget_exhausted",
                        limit: 2
                      }}

      trace = File.read!(trace_file)
      assert length(String.split(trace, "RUN", trim: true)) == 1
      assert length(Regex.scan(~r/"method":"turn\/start"/, trace)) == 2
    after
      System.delete_env("SYMP_TEST_CODEx_TRACE")
      File.rm_rf(test_root)
    end
  end

  test "app server starts with workspace cwd and expected startup command" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-app-server-args-#{System.unique_integer([:positive])}"
      )

    try do
      workspace_root = Path.join(test_root, "workspaces")
      workspace = Path.join(workspace_root, "MT-77")
      codex_binary = Path.join(test_root, "fake-codex")
      trace_file = Path.join(test_root, "codex-args.trace")
      previous_trace = System.get_env("SYMP_TEST_CODex_TRACE")

      on_exit(fn ->
        if is_binary(previous_trace) do
          System.put_env("SYMP_TEST_CODex_TRACE", previous_trace)
        else
          System.delete_env("SYMP_TEST_CODex_TRACE")
        end
      end)

      System.put_env("SYMP_TEST_CODex_TRACE", trace_file)
      File.mkdir_p!(workspace)

      File.write!(codex_binary, """
      #!/bin/sh
      trace_file="${SYMP_TEST_CODex_TRACE:-/tmp/codex-args.trace}"
      count=0
      printf 'ARGV:%s\\n' \"$*\" >> \"$trace_file\"
      printf 'CWD:%s\\n' \"$PWD\" >> \"$trace_file\"

      while IFS= read -r line; do
        count=$((count + 1))
        printf 'JSON:%s\\n' \"$line\" >> \"$trace_file\"
        case \"$count\" in
          1)
            printf '%s\\n' '{\"id\":1,\"result\":{}}'
            ;;
          2)
            printf '%s\\n' '{\"id\":2,\"result\":{\"thread\":{\"id\":\"thread-77\"}}}'
            ;;
          3)
            printf '%s\\n' '{\"id\":3,\"result\":{\"turn\":{\"id\":\"turn-77\"}}}'
            ;;
          4)
            printf '%s\\n' '{\"method\":\"turn/completed\"}'
            exit 0
            ;;
          *)
            exit 0
            ;;
        esac
      done
      """)

      File.chmod!(codex_binary, 0o755)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        codex_command: "#{codex_binary} app-server"
      )

      issue = %Issue{
        id: "issue-args",
        identifier: "MT-77",
        title: "Validate codex args",
        description: "Check startup args and cwd",
        state: "In Progress",
        url: "https://example.org/issues/MT-77",
        labels: ["backend"]
      }

      assert {:ok, _result} = AppServer.run(workspace, "Fix workspace start args", issue)
      assert {:ok, canonical_workspace} = SymphonyElixir.PathSafety.canonicalize(workspace)

      trace = File.read!(trace_file)
      lines = String.split(trace, "\n", trim: true)

      assert argv_line = Enum.find(lines, fn line -> String.starts_with?(line, "ARGV:") end)
      assert String.contains?(argv_line, "app-server")
      refute Enum.any?(lines, &String.contains?(&1, "--yolo"))
      assert cwd_line = Enum.find(lines, fn line -> String.starts_with?(line, "CWD:") end)
      assert String.ends_with?(cwd_line, Path.basename(workspace))

      assert Enum.any?(lines, fn line ->
               if String.starts_with?(line, "JSON:") do
                 line
                 |> String.trim_leading("JSON:")
                 |> Jason.decode!()
                 |> then(fn payload ->
                   expected_approval_policy = %{
                     "reject" => %{
                       "sandbox_approval" => true,
                       "rules" => true,
                       "mcp_elicitations" => true
                     }
                   }

                   payload["method"] == "thread/start" &&
                     get_in(payload, ["params", "approvalPolicy"]) == expected_approval_policy &&
                     get_in(payload, ["params", "sandbox"]) == "workspace-write" &&
                     get_in(payload, ["params", "cwd"]) == canonical_workspace
                 end)
               else
                 false
               end
             end)

      expected_turn_sandbox_policy = %{
        "type" => "workspaceWrite",
        "writableRoots" => [canonical_workspace],
        "readOnlyAccess" => %{"type" => "fullAccess"},
        "networkAccess" => false,
        "excludeTmpdirEnvVar" => false,
        "excludeSlashTmp" => false
      }

      assert Enum.any?(lines, fn line ->
               if String.starts_with?(line, "JSON:") do
                 line
                 |> String.trim_leading("JSON:")
                 |> Jason.decode!()
                 |> then(fn payload ->
                   expected_approval_policy = %{
                     "reject" => %{
                       "sandbox_approval" => true,
                       "rules" => true,
                       "mcp_elicitations" => true
                     }
                   }

                   payload["method"] == "turn/start" &&
                     get_in(payload, ["params", "cwd"]) == canonical_workspace &&
                     get_in(payload, ["params", "approvalPolicy"]) == expected_approval_policy &&
                     get_in(payload, ["params", "sandboxPolicy"]) == expected_turn_sandbox_policy
                 end)
               else
                 false
               end
             end)
    after
      File.rm_rf(test_root)
    end
  end

  test "app server startup command supports codex args override from workflow config" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-app-server-custom-args-#{System.unique_integer([:positive])}"
      )

    try do
      workspace_root = Path.join(test_root, "workspaces")
      workspace = Path.join(workspace_root, "MT-88")
      codex_binary = Path.join(test_root, "fake-codex")
      trace_file = Path.join(test_root, "codex-custom-args.trace")
      previous_trace = System.get_env("SYMP_TEST_CODex_TRACE")

      on_exit(fn ->
        if is_binary(previous_trace) do
          System.put_env("SYMP_TEST_CODex_TRACE", previous_trace)
        else
          System.delete_env("SYMP_TEST_CODex_TRACE")
        end
      end)

      System.put_env("SYMP_TEST_CODex_TRACE", trace_file)
      File.mkdir_p!(workspace)

      File.write!(codex_binary, """
      #!/bin/sh
      trace_file="${SYMP_TEST_CODex_TRACE:-/tmp/codex-custom-args.trace}"
      count=0
      printf 'ARGV:%s\\n' \"$*\" >> \"$trace_file\"

      while IFS= read -r line; do
        count=$((count + 1))
        case \"$count\" in
          1)
            printf '%s\\n' '{\"id\":1,\"result\":{}}'
            ;;
          2)
            printf '%s\\n' '{\"id\":2,\"result\":{\"thread\":{\"id\":\"thread-88\"}}}'
            ;;
          3)
            printf '%s\\n' '{\"id\":3,\"result\":{\"turn\":{\"id\":\"turn-88\"}}}'
            ;;
          4)
            printf '%s\\n' '{\"method\":\"turn/completed\"}'
            exit 0
            ;;
          *)
            exit 0
            ;;
        esac
      done
      """)

      File.chmod!(codex_binary, 0o755)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        codex_command: "#{codex_binary} --config 'model=\"gpt-5.5\"' app-server"
      )

      issue = %Issue{
        id: "issue-custom-args",
        identifier: "MT-88",
        title: "Validate custom codex args",
        description: "Check startup args override",
        state: "In Progress",
        url: "https://example.org/issues/MT-88",
        labels: ["backend"]
      }

      assert {:ok, _result} = AppServer.run(workspace, "Fix workspace start args", issue)

      trace = File.read!(trace_file)
      lines = String.split(trace, "\n", trim: true)

      assert argv_line = Enum.find(lines, fn line -> String.starts_with?(line, "ARGV:") end)
      assert String.contains?(argv_line, "--config model=\"gpt-5.5\" app-server")
      refute String.contains?(argv_line, "--ask-for-approval never")
      refute String.contains?(argv_line, "--sandbox danger-full-access")
    after
      File.rm_rf(test_root)
    end
  end

  test "app server startup payload uses configurable approval and sandbox settings from workflow config" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-elixir-app-server-policy-overrides-#{System.unique_integer([:positive])}"
      )

    try do
      workspace_root = Path.join(test_root, "workspaces")
      workspace = Path.join(workspace_root, "MT-99")
      codex_binary = Path.join(test_root, "fake-codex")
      trace_file = Path.join(test_root, "codex-policy-overrides.trace")
      previous_trace = System.get_env("SYMP_TEST_CODex_TRACE")

      on_exit(fn ->
        if is_binary(previous_trace) do
          System.put_env("SYMP_TEST_CODex_TRACE", previous_trace)
        else
          System.delete_env("SYMP_TEST_CODex_TRACE")
        end
      end)

      System.put_env("SYMP_TEST_CODex_TRACE", trace_file)
      File.mkdir_p!(workspace)

      File.write!(codex_binary, """
      #!/bin/sh
      trace_file="${SYMP_TEST_CODex_TRACE:-/tmp/codex-policy-overrides.trace}"
      count=0

      while IFS= read -r line; do
        count=$((count + 1))
        printf 'JSON:%s\\n' "$line" >> "$trace_file"

        case "$count" in
          1)
            printf '%s\\n' '{"id":1,"result":{}}'
            ;;
          2)
            printf '%s\\n' '{"id":2,"result":{"thread":{"id":"thread-99"}}}'
            ;;
          3)
            printf '%s\\n' '{"id":3,"result":{"turn":{"id":"turn-99"}}}'
            ;;
          4)
            printf '%s\\n' '{"method":"turn/completed"}'
            exit 0
            ;;
          *)
            exit 0
            ;;
        esac
      done
      """)

      File.chmod!(codex_binary, 0o755)

      workspace_cache = Path.join(Path.expand(workspace), ".cache")
      File.mkdir_p!(workspace_cache)

      write_workflow_file!(Workflow.workflow_file_path(),
        workspace_root: workspace_root,
        codex_command: "#{codex_binary} app-server",
        codex_approval_policy: "on-request",
        codex_thread_sandbox: "workspace-write",
        codex_turn_sandbox_policy: %{
          type: "workspaceWrite",
          writableRoots: [Path.expand(workspace), workspace_cache]
        }
      )

      issue = %Issue{
        id: "issue-policy-overrides",
        identifier: "MT-99",
        title: "Validate codex policy overrides",
        description: "Check startup policy payload overrides",
        state: "In Progress",
        url: "https://example.org/issues/MT-99",
        labels: ["backend"]
      }

      assert {:ok, _result} = AppServer.run(workspace, "Fix workspace start args", issue)

      lines = File.read!(trace_file) |> String.split("\n", trim: true)

      assert Enum.any?(lines, fn line ->
               if String.starts_with?(line, "JSON:") do
                 line
                 |> String.trim_leading("JSON:")
                 |> Jason.decode!()
                 |> then(fn payload ->
                   payload["method"] == "thread/start" &&
                     get_in(payload, ["params", "approvalPolicy"]) == "on-request" &&
                     get_in(payload, ["params", "sandbox"]) == "workspace-write"
                 end)
               else
                 false
               end
             end)

      expected_turn_policy = %{
        "type" => "workspaceWrite",
        "writableRoots" => [Path.expand(workspace), workspace_cache]
      }

      assert Enum.any?(lines, fn line ->
               if String.starts_with?(line, "JSON:") do
                 line
                 |> String.trim_leading("JSON:")
                 |> Jason.decode!()
                 |> then(fn payload ->
                   payload["method"] == "turn/start" &&
                     get_in(payload, ["params", "approvalPolicy"]) == "on-request" &&
                     get_in(payload, ["params", "sandboxPolicy"]) == expected_turn_policy
                 end)
               else
                 false
               end
             end)
    after
      File.rm_rf(test_root)
    end
  end

  defp terminal_transition_state(issue_id, ref, run_ledger_path, opts) do
    issue = %Issue{
      id: issue_id,
      identifier: "MT-TERMINAL",
      state: "In Progress",
      title: "Terminal persistence test"
    }

    running_entry = %{
      pid: self(),
      ref: ref,
      run_id: "run-#{issue_id}",
      retry_attempt: Keyword.fetch!(opts, :retry_attempt),
      identifier: issue.identifier,
      issue: issue,
      worker_host: nil,
      workspace_path: nil,
      session_id: nil,
      started_at: DateTime.utc_now(),
      run_budget_timer_ref: nil
    }

    %Orchestrator.State{
      run_ledger_path: run_ledger_path,
      runner_generation: "runner-terminal-test",
      running: %{issue_id => running_entry},
      claimed: MapSet.new([issue_id]),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }
  end

  defp parked_reconcile_state(tag, workspace_path, workspace_root, worker_host) do
    issue_id = "issue-parked-#{tag}"

    wait = %{
      issue_id: issue_id,
      run_id: "run-parked-#{tag}",
      attempt: 2,
      identifier: "MT-PARKED-#{String.upcase(tag)}",
      wait_id: "wait-parked-#{tag}",
      reason: "waiting_owner",
      allowed_actions: ["approve", "reject"],
      stage: "parked",
      tracker_state: "Human Review",
      terminal_reason: nil,
      worker_host: worker_host,
      workspace_path: workspace_path,
      workspace_root: workspace_root,
      parked_at: DateTime.utc_now()
    }

    ledger_path = ledger_path("parked-#{tag}")
    :ok = seed_parked_ledger!(ledger_path, wait)

    state = %Orchestrator.State{
      run_ledger_path: ledger_path,
      runner_generation: "runner-parked-reconcile",
      parked: %{issue_id => wait},
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    {state, wait}
  end

  defp parked_workspace_root(tag) do
    root =
      Path.join(
        System.tmp_dir!(),
        "symphony-parked-reconcile-#{tag}-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf(root) end)
    root
  end

  defp install_fake_parked_cleanup_ssh!(tag) do
    test_root = parked_workspace_root("ssh-#{tag}")
    trace_file = Path.join(test_root, "ssh.trace")
    fake_ssh = Path.join(test_root, "ssh")
    previous_path = System.get_env("PATH")
    previous_trace = System.get_env("SYMP_TEST_SSH_TRACE")
    remote_root = "/remote/symphony/workspaces"
    workspace = Path.join(remote_root, "MT-PARKED-#{String.upcase(tag)}")

    File.mkdir_p!(test_root)
    System.put_env("SYMP_TEST_SSH_TRACE", trace_file)
    System.put_env("PATH", test_root <> ":" <> (previous_path || ""))

    File.write!(fake_ssh, """
    #!/bin/sh
    printf 'ARGV:%s\\n' "$*" >> "${SYMP_TEST_SSH_TRACE}"
    exit 0
    """)

    File.chmod!(fake_ssh, 0o755)

    on_exit(fn ->
      restore_env("PATH", previous_path)
      restore_env("SYMP_TEST_SSH_TRACE", previous_trace)
    end)

    {remote_root, workspace, trace_file}
  end

  defp blocked_ledger_path do
    root =
      Path.join(
        System.tmp_dir!(),
        "symphony-blocked-ledger-#{System.unique_integer([:positive])}"
      )

    blocking_file = Path.join(root, "not-a-directory")
    File.mkdir_p!(root)
    File.write!(blocking_file, "blocked")
    on_exit(fn -> File.rm_rf(root) end)
    Path.join(blocking_file, "events.jsonl")
  end

  defp ledger_path(tag) do
    root =
      Path.join(
        System.tmp_dir!(),
        "symphony-#{tag}-#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf(root) end)
    Path.join(root, "events.jsonl")
  end
end
