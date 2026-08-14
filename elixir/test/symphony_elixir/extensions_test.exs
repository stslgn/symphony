defmodule SymphonyElixir.ExtensionsTest do
  use SymphonyElixir.TestSupport

  import Phoenix.ConnTest
  import Phoenix.LiveViewTest

  alias SymphonyElixir.Linear.Adapter
  alias SymphonyElixir.{ParkedProjection, RunLedger}
  alias SymphonyElixir.Tracker.Memory

  @endpoint SymphonyElixirWeb.Endpoint

  defmodule FakeLinearClient do
    def fetch_candidate_issues do
      send(self(), :fetch_candidate_issues_called)
      {:ok, [:candidate]}
    end

    def fetch_candidate_issues(_context), do: fetch_candidate_issues()

    def fetch_issues_by_states(states) do
      send(self(), {:fetch_issues_by_states_called, states})
      {:ok, states}
    end

    def fetch_issues_by_states(states, _context), do: fetch_issues_by_states(states)

    def fetch_issue_states_by_ids(issue_ids) do
      send(self(), {:fetch_issue_states_by_ids_called, issue_ids})
      {:ok, issue_ids}
    end

    def fetch_issue_states_by_ids(issue_ids, _context), do: fetch_issue_states_by_ids(issue_ids)

    def fetch_comments_since(issue_id, created_after) do
      send(self(), {:fetch_comments_since_called, issue_id, created_after})
      {:ok, [:comment]}
    end

    def fetch_comments_since(issue_id, created_after, _context),
      do: fetch_comments_since(issue_id, created_after)

    def graphql(query, variables) do
      send(self(), {:graphql_called, query, variables})

      case Process.get({__MODULE__, :graphql_results}) do
        [result | rest] ->
          Process.put({__MODULE__, :graphql_results}, rest)
          result

        _ ->
          Process.get({__MODULE__, :graphql_result})
      end
    end

    def graphql(query, variables, _opts), do: graphql(query, variables)
  end

  defmodule SlowOrchestrator do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, :ok, opts)
    end

    def init(:ok), do: {:ok, :ok}

    def handle_call(:snapshot, _from, state) do
      Process.sleep(25)
      {:reply, %{}, state}
    end

    def handle_call(:request_refresh, _from, state) do
      {:reply, :unavailable, state}
    end
  end

  defmodule BlockingRefreshOrchestrator do
    use GenServer

    def start_link(opts) do
      name = Keyword.fetch!(opts, :name)
      GenServer.start_link(__MODULE__, :ok, name: name)
    end

    def init(:ok), do: {:ok, :ok}

    def handle_call(:request_refresh, _from, state) do
      Process.sleep(1_000)
      {:reply, %{queued: true, requested_at: DateTime.utc_now()}, state}
    end
  end

  defmodule StaticOrchestrator do
    use GenServer

    def start_link(opts) do
      name = Keyword.fetch!(opts, :name)
      GenServer.start_link(__MODULE__, opts, name: name)
    end

    def init(opts), do: {:ok, opts}

    def handle_call(:snapshot, _from, state) do
      {:reply, Keyword.fetch!(state, :snapshot), state}
    end

    def handle_call(:request_refresh, _from, state) do
      if recipient = Keyword.get(state, :refresh_recipient) do
        send(recipient, :refresh_requested)
      end

      {:reply, Keyword.get(state, :refresh, :unavailable), state}
    end

    def handle_call({:set_dispatch_paused, paused}, _from, state) do
      if recipient = Keyword.get(state, :pause_recipient) do
        send(recipient, {:dispatch_paused, paused})
      end

      snapshot =
        state
        |> Keyword.fetch!(:snapshot)
        |> Map.put(:control, %{dispatch_paused: paused})

      payload = %{
        dispatch_paused: paused,
        changed: true,
        requested_at: DateTime.utc_now()
      }

      {:reply, {:ok, payload}, Keyword.put(state, :snapshot, snapshot)}
    end
  end

  setup do
    linear_client_module = Application.get_env(:symphony_elixir, :linear_client_module)

    on_exit(fn ->
      if is_nil(linear_client_module) do
        Application.delete_env(:symphony_elixir, :linear_client_module)
      else
        Application.put_env(:symphony_elixir, :linear_client_module, linear_client_module)
      end
    end)

    :ok
  end

  setup do
    endpoint_config = Application.get_env(:symphony_elixir, SymphonyElixirWeb.Endpoint, [])

    on_exit(fn ->
      Application.put_env(:symphony_elixir, SymphonyElixirWeb.Endpoint, endpoint_config)
    end)

    :ok
  end

  test "workflow store reloads changes, keeps last good workflow, and falls back when stopped" do
    ensure_workflow_store_running()
    assert {:ok, %{prompt: "You are an agent for this repository."}} = Workflow.current()

    write_workflow_file!(Workflow.workflow_file_path(), prompt: "Second prompt")
    send(WorkflowStore, :poll)

    assert_eventually(fn ->
      match?({:ok, %{prompt: "Second prompt"}}, Workflow.current())
    end)

    File.write!(Workflow.workflow_file_path(), "---\ntracker: [\n---\nBroken prompt\n")

    workflow_store_pid = Process.whereis(WorkflowStore)
    snapshot = WorkflowStore.current_with_authority()

    assert {:ok, %{prompt: "Second prompt"}, authority, tracker_authority} = snapshot

    assert {^workflow_store_pid, _authority_epoch} = authority
    assert {^workflow_store_pid, _tracker_authority_epoch} = tracker_authority
    assert ^tracker_authority = WorkflowStore.tracker_authority_generation()

    assert {:error, _reason} = WorkflowStore.force_reload()
    assert {:ok, %{prompt: "Second prompt"}} = Workflow.current()

    third_workflow = Path.join(Path.dirname(Workflow.workflow_file_path()), "THIRD_WORKFLOW.md")
    write_workflow_file!(third_workflow, prompt: "Third prompt")
    Workflow.set_workflow_file_path(third_workflow)
    assert {:ok, %{prompt: "Third prompt"}} = Workflow.current()

    assert {:ok, %{prompt: "Third prompt"}, authority_generation, tracker_authority_generation} =
             WorkflowStore.current_with_authority()

    assert {^workflow_store_pid, _authority_epoch} = authority_generation
    assert {^workflow_store_pid, _tracker_authority_epoch} = tracker_authority_generation

    assert :ok = Supervisor.terminate_child(SymphonyElixir.Supervisor, WorkflowStore)
    assert {:ok, %{prompt: "Third prompt"}} = WorkflowStore.current()
    assert :ok = WorkflowStore.force_reload()

    assert {:ok, %{prompt: "Third prompt"}, {:standalone, _authority_contract}, {:standalone, _tracker_authority_contract}} =
             WorkflowStore.current_with_authority()

    assert {:standalone,
            %{
              kind: "linear",
              endpoint: "https://api.linear.app/graphql",
              api_key_selector: "token",
              webhook_secret_selector: nil,
              project_slug: "project",
              operator_user_ids: []
            }} = WorkflowStore.authority_generation()

    assert {:standalone,
            %{
              kind: "linear",
              endpoint: "https://api.linear.app/graphql",
              api_key_selector: "token",
              webhook_secret_selector: nil,
              project_slug: "project"
            }} = WorkflowStore.tracker_authority_generation()

    invalid_tracker_path =
      Path.join(Path.dirname(third_workflow), "INVALID_TRACKER_AUTHORITY_WORKFLOW.md")

    File.write!(
      invalid_tracker_path,
      "---\ntracker: []\n---\n## Symphony Runtime Prompt\nInvalid tracker authority fixture\n"
    )

    Workflow.set_workflow_file_path(invalid_tracker_path)
    assert {:standalone, {:invalid_tracker, []}} = WorkflowStore.authority_generation()
    assert {:standalone, {:invalid_tracker, []}} = WorkflowStore.tracker_authority_generation()

    missing_path = Path.join(Path.dirname(third_workflow), "MISSING_AUTHORITY_WORKFLOW.md")
    Workflow.set_workflow_file_path(missing_path)

    assert {:standalone, {:unavailable, {:missing_workflow_file, ^missing_path, :enoent}}} =
             WorkflowStore.authority_generation()

    assert {:standalone, {:unavailable, {:missing_workflow_file, ^missing_path, :enoent}}} =
             WorkflowStore.tracker_authority_generation()

    assert_raise ArgumentError, ~r/Missing WORKFLOW/, fn ->
      Config.settings_with_authority!()
    end

    Workflow.set_workflow_file_path(third_workflow)
    assert {:ok, _pid} = Supervisor.restart_child(SymphonyElixir.Supervisor, WorkflowStore)
  end

  test "workflow store init stops on missing workflow file" do
    missing_path = Path.join(Path.dirname(Workflow.workflow_file_path()), "MISSING_WORKFLOW.md")
    Workflow.set_workflow_file_path(missing_path)

    assert {:stop, {:missing_workflow_file, ^missing_path, :enoent}} = WorkflowStore.init([])
  end

  test "workflow store stamp hashes the exact bytes used for its parsed snapshot" do
    ensure_workflow_store_running()
    workflow_path = Workflow.workflow_file_path()

    write_workflow_file!(workflow_path, prompt: "Immutable snapshot B")
    assert :ok = WorkflowStore.force_reload()

    content = File.read!(workflow_path)
    state = :sys.get_state(WorkflowStore)

    assert state.stamp == :crypto.hash(:sha256, content)
    assert state.workflow.prompt == "Immutable snapshot B"
  end

  test "workflow store keeps last good authority across schema-invalid YAML reloads" do
    ensure_workflow_store_running()
    workflow_path = Workflow.workflow_file_path()

    write_workflow_file!(workflow_path,
      prompt: "Schema validated snapshot",
      tracker_operator_user_ids: ["operator-1"]
    )

    assert :ok = WorkflowStore.force_reload()

    assert {:ok, good_workflow, authority_generation, tracker_authority_generation} =
             WorkflowStore.current_with_authority()

    invalid_workflows = [
      """
      ---
      tracker:
        kind: linear
        operator_user_ids: not-a-list
      ---
      ## Symphony Runtime Prompt
      Invalid tracker schema
      """,
      """
      ---
      agent:
        max_run_tokens: 0
      ---
      ## Symphony Runtime Prompt
      Invalid unrelated schema
      """
    ]

    Enum.each(invalid_workflows, fn invalid_workflow ->
      File.write!(workflow_path, invalid_workflow)

      assert {:error, {:invalid_workflow_config, _message}} =
               WorkflowStore.force_reload()

      assert {:ok, ^good_workflow, ^authority_generation, ^tracker_authority_generation} =
               WorkflowStore.current_with_authority()

      assert Process.alive?(Process.whereis(WorkflowStore))
    end)
  end

  test "webhook secret selector changes invalidate pinned tracker authority" do
    ensure_workflow_store_running()
    workflow_path = Workflow.workflow_file_path()

    write_workflow_file!(workflow_path,
      tracker_kind: "memory",
      tracker_webhook_secret: "$SYMP_TEST_WEBHOOK_SECRET_A"
    )

    assert :ok = WorkflowStore.force_reload()
    context = Tracker.current_poll_context()
    initial_generation = WorkflowStore.tracker_authority_generation()

    write_workflow_file!(workflow_path,
      tracker_kind: "memory",
      tracker_webhook_secret: "$SYMP_TEST_WEBHOOK_SECRET_B"
    )

    assert :ok = WorkflowStore.force_reload()
    refute WorkflowStore.tracker_authority_generation() == initial_generation
    refute Tracker.authority_valid?(context)
  end

  test "workflow store start_link and poll callback cover missing-file error paths" do
    ensure_workflow_store_running()
    existing_path = Workflow.workflow_file_path()
    manual_path = Path.join(Path.dirname(existing_path), "MANUAL_WORKFLOW.md")
    missing_path = Path.join(Path.dirname(existing_path), "MANUAL_MISSING_WORKFLOW.md")

    assert :ok = Supervisor.terminate_child(SymphonyElixir.Supervisor, WorkflowStore)

    Workflow.set_workflow_file_path(missing_path)

    assert {:error, {:missing_workflow_file, ^missing_path, :enoent}} =
             WorkflowStore.force_reload()

    write_workflow_file!(manual_path, prompt: "Manual workflow prompt")
    Workflow.set_workflow_file_path(manual_path)

    assert {:ok, manual_pid} = WorkflowStore.start_link()
    assert Process.alive?(manual_pid)

    state = :sys.get_state(manual_pid)
    File.write!(manual_path, "---\ntracker: [\n---\nBroken prompt\n")
    assert {:noreply, returned_state} = WorkflowStore.handle_info(:poll, state)
    assert returned_state.workflow.prompt == "Manual workflow prompt"
    refute returned_state.stamp == nil
    assert_receive :poll, 1_100

    Workflow.set_workflow_file_path(missing_path)
    assert {:noreply, path_error_state} = WorkflowStore.handle_info(:poll, returned_state)
    assert path_error_state.workflow.prompt == "Manual workflow prompt"
    assert_receive :poll, 1_100

    Workflow.set_workflow_file_path(manual_path)
    File.rm!(manual_path)
    assert {:noreply, removed_state} = WorkflowStore.handle_info(:poll, path_error_state)
    assert removed_state.workflow.prompt == "Manual workflow prompt"
    assert_receive :poll, 1_100

    Process.exit(manual_pid, :normal)
    restart_result = Supervisor.restart_child(SymphonyElixir.Supervisor, WorkflowStore)

    assert match?({:ok, _pid}, restart_result) or
             match?({:error, {:already_started, _pid}}, restart_result)

    Workflow.set_workflow_file_path(existing_path)
    WorkflowStore.force_reload()
  end

  test "tracker delegates to memory and linear adapters" do
    issue = %Issue{id: "issue-1", identifier: "MT-1", state: "In Progress"}
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue, %{id: "ignored"}])
    Application.put_env(:symphony_elixir, :memory_tracker_recipient, self())
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory")

    assert Config.settings!().tracker.kind == "memory"
    context = SymphonyElixir.Tracker.current_poll_context()
    assert SymphonyElixir.Tracker.adapter(context) == Memory
    assert {:ok, [^issue]} = SymphonyElixir.Tracker.fetch_candidate_issues(context)

    assert {:ok, [^issue]} =
             SymphonyElixir.Tracker.fetch_issues_by_states([" in progress ", 42], context)

    assert {:ok, [^issue]} =
             SymphonyElixir.Tracker.fetch_issue_states_by_ids(["issue-1"], context)

    assert :ok = SymphonyElixir.Tracker.create_comment("issue-1", "comment", context)
    assert :ok = SymphonyElixir.Tracker.update_issue_state("issue-1", "Done", context)
    assert_receive {:memory_tracker_comment, "issue-1", "comment"}
    assert_receive {:memory_tracker_state_update, "issue-1", "Done"}

    Application.delete_env(:symphony_elixir, :memory_tracker_recipient)
    assert :ok = Memory.create_comment("issue-1", "quiet", context)
    assert :ok = Memory.update_issue_state("issue-1", "Quiet", context)

    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "linear")
    assert SymphonyElixir.Tracker.adapter(SymphonyElixir.Tracker.current_poll_context()) == Adapter
  end

  test "linear adapter delegates reads and validates mutation responses" do
    Application.put_env(:symphony_elixir, :linear_client_module, FakeLinearClient)
    context = SymphonyElixir.Tracker.current_poll_context()

    assert {:ok, [:candidate]} = Adapter.fetch_candidate_issues(context)
    assert_receive :fetch_candidate_issues_called

    assert {:ok, ["Todo"]} = Adapter.fetch_issues_by_states(["Todo"], context)
    assert_receive {:fetch_issues_by_states_called, ["Todo"]}

    assert {:ok, ["issue-1"]} = Adapter.fetch_issue_states_by_ids(["issue-1"], context)
    assert_receive {:fetch_issue_states_by_ids_called, ["issue-1"]}

    created_after = ~U[2026-08-03 10:00:00Z]
    assert {:ok, [:comment]} = Adapter.fetch_comments_since("issue-1", created_after, context)
    assert_receive {:fetch_comments_since_called, "issue-1", ^created_after}

    Process.put(
      {FakeLinearClient, :graphql_result},
      {:ok, %{"data" => %{"commentCreate" => %{"success" => true}}}}
    )

    assert :ok = Adapter.create_comment("issue-1", "hello", context)
    assert_receive {:graphql_called, create_comment_query, %{body: "hello", issueId: "issue-1"}}
    assert create_comment_query =~ "commentCreate"

    Process.put(
      {FakeLinearClient, :graphql_result},
      {:ok, %{"data" => %{"commentCreate" => %{"success" => false}}}}
    )

    assert {:error, :comment_create_failed} =
             Adapter.create_comment("issue-1", "broken", context)

    Process.put({FakeLinearClient, :graphql_result}, {:error, :boom})

    assert {:error, :boom} = Adapter.create_comment("issue-1", "boom", context)

    Process.put({FakeLinearClient, :graphql_result}, {:ok, %{"data" => %{}}})
    assert {:error, :comment_create_failed} = Adapter.create_comment("issue-1", "weird", context)

    Process.put({FakeLinearClient, :graphql_result}, :unexpected)
    assert {:error, :comment_create_failed} = Adapter.create_comment("issue-1", "odd", context)

    Process.put(
      {FakeLinearClient, :graphql_results},
      [
        {:ok,
         %{
           "data" => %{
             "issue" => %{"team" => %{"states" => %{"nodes" => [%{"id" => "state-1"}]}}}
           }
         }},
        {:ok, %{"data" => %{"issueUpdate" => %{"success" => true}}}}
      ]
    )

    assert :ok = Adapter.update_issue_state("issue-1", "Done", context)
    assert_receive {:graphql_called, state_lookup_query, %{issueId: "issue-1", stateName: "Done"}}
    assert state_lookup_query =~ "states"

    assert_receive {:graphql_called, update_issue_query, %{issueId: "issue-1", stateId: "state-1"}}

    assert update_issue_query =~ "issueUpdate"

    Process.put(
      {FakeLinearClient, :graphql_results},
      [
        {:ok,
         %{
           "data" => %{
             "issue" => %{"team" => %{"states" => %{"nodes" => [%{"id" => "state-1"}]}}}
           }
         }},
        {:ok, %{"data" => %{"issueUpdate" => %{"success" => false}}}}
      ]
    )

    assert {:error, :issue_update_failed} =
             Adapter.update_issue_state("issue-1", "Broken", context)

    Process.put({FakeLinearClient, :graphql_results}, [{:error, :boom}])

    assert {:error, :boom} = Adapter.update_issue_state("issue-1", "Boom", context)

    Process.put({FakeLinearClient, :graphql_results}, [{:ok, %{"data" => %{}}}])
    assert {:error, :state_not_found} = Adapter.update_issue_state("issue-1", "Missing", context)

    Process.put(
      {FakeLinearClient, :graphql_results},
      [
        {:ok,
         %{
           "data" => %{
             "issue" => %{"team" => %{"states" => %{"nodes" => [%{"id" => "state-1"}]}}}
           }
         }},
        {:ok, %{"data" => %{}}}
      ]
    )

    assert {:error, :issue_update_failed} = Adapter.update_issue_state("issue-1", "Weird", context)

    Process.put(
      {FakeLinearClient, :graphql_results},
      [
        {:ok,
         %{
           "data" => %{
             "issue" => %{"team" => %{"states" => %{"nodes" => [%{"id" => "state-1"}]}}}
           }
         }},
        :unexpected
      ]
    )

    assert {:error, :issue_update_failed} = Adapter.update_issue_state("issue-1", "Odd", context)
  end

  test "phoenix observability api preserves state, issue, and refresh responses" do
    snapshot = static_snapshot()
    orchestrator_name = Module.concat(__MODULE__, :ObservabilityApiOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: snapshot,
        refresh: %{
          queued: true,
          coalesced: false,
          requested_at: DateTime.utc_now(),
          operations: ["poll", "reconcile"]
        },
        pause_recipient: self()
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    conn = get(build_conn(), "/api/v1/state")
    state_payload = json_response(conn, 200)
    refute inspect(state_payload) =~ "SENSITIVE-BL10-DO-NOT-EXPOSE"

    assert state_payload == %{
             "generated_at" => state_payload["generated_at"],
             "counts" => %{
               "running" => 1,
               "retrying" => 1,
               "cleanup_pending" => 0,
               "parked" => 1
             },
             "control" => %{"dispatch_paused" => false},
             "capabilities" => %{
               "dynamic_tools" => ["linear_graphql"],
               "mcp_tool_auto_approve" => [],
               "mcp_elicitation_auto_approve" => []
             },
             "running" => [
               %{
                 "issue_id" => "issue-http",
                 "issue_identifier" => "MT-HTTP",
                 "state" => "In Progress",
                 "worker_host" => nil,
                 "workspace_path" => nil,
                 "session_id" => "thread-http",
                 "model" => %{
                   "resolved" => nil,
                   "reasoning_effort" => nil,
                   "catalog_source" => nil
                 },
                 "model_catalog" => nil,
                 "turn_count" => 7,
                 "last_event" => "notification",
                 "started_at" => state_payload["running"] |> List.first() |> Map.fetch!("started_at"),
                 "last_event_at" => nil,
                 "tokens" => %{"input_tokens" => 4, "output_tokens" => 8, "total_tokens" => 12},
                 "budget" => %{
                   "turns" => %{"limit" => 20, "used" => 7, "remaining" => 13},
                   "tokens" => %{
                     "limit" => 250_000,
                     "used" => 12,
                     "remaining" => 249_988,
                     "telemetry_observed" => true
                   },
                   "time" => %{"limit" => 7_200, "used" => 42, "remaining" => 7_158}
                 }
               }
             ],
             "retrying" => [
               %{
                 "issue_id" => "issue-retry",
                 "issue_identifier" => "MT-RETRY",
                 "attempt" => 2,
                 "due_at" => state_payload["retrying"] |> List.first() |> Map.fetch!("due_at"),
                 "error_code" => "worker_failure",
                 "worker_host" => nil,
                 "workspace_path" => nil
               }
             ],
             "cleanup_pending" => [],
             "parked" => [
               %{
                 "issue_id" => "issue-parked",
                 "issue_identifier" => "MT-PARKED",
                 "wait_id" => "wait-http",
                 "reason" => "waiting_owner",
                 "allowed_actions" => ["approve", "reject"],
                 "tracker_state" => "Human Review",
                 "run_id" => "run-http",
                 "attempt" => 1,
                 "stage" => "parked",
                 "terminal_reason" => "turn_budget_exhausted",
                 "worker_host" => nil,
                 "workspace_path" => nil,
                 "parked_at" => state_payload["parked"] |> List.first() |> Map.fetch!("parked_at"),
                 "truncated_fields" => []
               }
             ],
             "parked_meta" => %{
               "total_count" => 1,
               "returned_count" => 1,
               "omitted_count" => 0,
               "truncated" => false,
               "row_limit" => 100,
               "byte_limit" => 65_536,
               "returned_bytes" => state_payload["parked_meta"]["returned_bytes"]
             },
             "codex_totals" => %{
               "input_tokens" => 4,
               "output_tokens" => 8,
               "total_tokens" => 12,
               "seconds_running" => 42.5
             },
             "rate_limits" => %{
               "limit_id" => "codex",
               "primary" => %{"remaining" => 11}
             }
           }

    conn = get(build_conn(), "/api/v1/MT-HTTP")
    issue_payload = json_response(conn, 200)
    refute inspect(issue_payload) =~ "SENSITIVE-BL10-DO-NOT-EXPOSE"

    assert issue_payload == %{
             "issue_identifier" => "MT-HTTP",
             "issue_id" => "issue-http",
             "status" => "running",
             "workspace" => %{
               "path" => Path.join(Config.settings!().workspace.root, "MT-HTTP"),
               "host" => nil
             },
             "attempts" => %{"restart_count" => 0, "current_retry_attempt" => 0},
             "running" => %{
               "worker_host" => nil,
               "workspace_path" => nil,
               "session_id" => "thread-http",
               "model" => %{
                 "resolved" => nil,
                 "reasoning_effort" => nil,
                 "catalog_source" => nil
               },
               "model_catalog" => nil,
               "turn_count" => 7,
               "state" => "In Progress",
               "started_at" => issue_payload["running"]["started_at"],
               "last_event" => "notification",
               "last_event_at" => nil,
               "tokens" => %{"input_tokens" => 4, "output_tokens" => 8, "total_tokens" => 12},
               "budget" => %{
                 "turns" => %{"limit" => 20, "used" => 7, "remaining" => 13},
                 "tokens" => %{
                   "limit" => 250_000,
                   "used" => 12,
                   "remaining" => 249_988,
                   "telemetry_observed" => true
                 },
                 "time" => %{"limit" => 7_200, "used" => 42, "remaining" => 7_158}
               }
             },
             "retry" => nil,
             "parked" => nil,
             "logs" => %{"codex_session_logs" => []},
             "recent_events" => [],
             "last_error_code" => nil,
             "tracked" => %{}
           }

    conn = get(build_conn(), "/api/v1/MT-RETRY")

    assert %{
             "status" => "retrying",
             "retry" => %{"attempt" => 2, "error_code" => "worker_failure"}
           } =
             json_response(conn, 200)

    conn = get(build_conn(), "/api/v1/MT-PARKED")

    assert %{
             "status" => "parked",
             "parked" => %{
               "wait_id" => "wait-http",
               "reason" => "waiting_owner",
               "allowed_actions" => ["approve", "reject"],
               "tracker_state" => "Human Review"
             }
           } = json_response(conn, 200)

    conn = get(build_conn(), "/api/v1/MT-MISSING")

    assert json_response(conn, 404) == %{
             "error" => %{"code" => "issue_not_found", "message" => "Issue not found"}
           }

    conn = post(build_conn(), "/api/v1/refresh", %{})

    assert %{"queued" => true, "coalesced" => false, "operations" => ["poll", "reconcile"]} =
             json_response(conn, 202)

    assert json_response(get(build_conn(), "/api/v1/pause"), 200) == %{
             "dispatch_paused" => false
           }

    assert %{"dispatch_paused" => true, "changed" => true} =
             json_response(post(build_conn(), "/api/v1/pause", %{"paused" => true}), 202)

    assert_receive {:dispatch_paused, true}

    assert json_response(get(build_conn(), "/api/v1/pause"), 200) == %{
             "dispatch_paused" => true
           }
  end

  test "rate-limit telemetry is allowlisted across every observability surface" do
    sentinel = "SENSITIVE-RATE-LIMIT-DO-NOT-EXPOSE<script>"
    issue_id = "issue-rate-limit-boundary"

    issue = %Issue{
      id: issue_id,
      identifier: "MT-RATE",
      title: "Rate-limit boundary",
      description: "Exercise rate-limit observability projections",
      state: "In Progress",
      url: "https://example.org/issues/MT-RATE"
    }

    orchestrator_name = Module.concat(__MODULE__, :RateLimitBoundaryOrchestrator)
    {:ok, orchestrator_pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(orchestrator_pid), do: Process.exit(orchestrator_pid, :normal)
    end)

    await_orchestrator_poll_idle(orchestrator_name)
    initial_state = :sys.get_state(orchestrator_pid)

    running_entry = %{
      pid: self(),
      ref: make_ref(),
      identifier: issue.identifier,
      issue: issue,
      session_id: nil,
      last_codex_message: nil,
      last_codex_timestamp: nil,
      last_codex_event: nil,
      codex_input_tokens: 0,
      codex_output_tokens: 0,
      codex_total_tokens: 0,
      codex_last_reported_input_tokens: 0,
      codex_last_reported_output_tokens: 0,
      codex_last_reported_total_tokens: 0,
      started_at: DateTime.utc_now()
    }

    :sys.replace_state(orchestrator_pid, fn _state ->
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.put(initial_state.claimed, issue_id))
    end)

    raw_rate_limits = %{
      "limit_id" => "codex",
      "limit_name" => sentinel,
      "provider_payload" => sentinel,
      "primary" => %{
        "remaining" => 90,
        "limit" => 1_000_000_000_001,
        "reset_in_seconds" => 30,
        "resetAt" => sentinel,
        "provider_note" => sentinel
      },
      "secondary" => %{
        "usedPercent" => 12.5,
        "windowDurationMins" => 60,
        "resetsAt" => 2_000_000_000,
        "provider_note" => sentinel
      },
      "credits" => %{
        "hasCredits" => true,
        "unlimited" => false,
        "balance" => 42.5,
        "provider_note" => sentinel
      }
    }

    send(
      orchestrator_pid,
      {:codex_worker_update, issue_id,
       %{
         event: :notification,
         payload: %{
           "method" => "codex/event/token_count",
           "params" => %{
             "msg" => %{
               "type" => "event_msg",
               "payload" => %{
                 "type" => "token_count",
                 "provider_payload" => sentinel,
                 "rate_limits" => raw_rate_limits
               }
             }
           }
         },
         timestamp: DateTime.utc_now()
       }}
    )

    send(
      orchestrator_pid,
      {:codex_worker_update, issue_id,
       %{
         event: :notification,
         payload: %{
           "method" => "account/rateLimits/updated",
           "params" => %{
             "rateLimits" => %{
               "limit_id" => sentinel,
               "primary" => %{"remaining" => 1, "provider_note" => sentinel}
             }
           }
         },
         timestamp: DateTime.utc_now()
       }}
    )

    snapshot = Orchestrator.snapshot(orchestrator_name, 1_000)

    expected_rate_limits = %{
      limit_id: "codex",
      primary: %{remaining: 90, reset_in_seconds: 30},
      secondary: %{
        used_percent: 12.5,
        window_duration_mins: 60,
        reset_at: 2_000_000_000
      },
      credits: %{has_credits: true, unlimited: false, balance: 42.5}
    }

    assert snapshot.rate_limits == expected_rate_limits
    refute inspect(snapshot) =~ sentinel

    await_orchestrator_poll_idle(orchestrator_name)
    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 1_000)

    state_payload = json_response(get(build_conn(), "/api/v1/state"), 200)
    {:ok, _view, html} = live(build_conn(), "/")

    terminal =
      StatusDashboard.format_snapshot_content_for_test({:ok, snapshot}, 0.0, 115)

    assert state_payload["rate_limits"] == %{
             "limit_id" => "codex",
             "primary" => %{"remaining" => 90, "reset_in_seconds" => 30},
             "secondary" => %{
               "used_percent" => 12.5,
               "window_duration_mins" => 60,
               "reset_at" => 2_000_000_000
             },
             "credits" => %{"has_credits" => true, "unlimited" => false, "balance" => 42.5}
           }

    assert html =~ "limit_id: codex"
    assert terminal =~ "codex"

    for surface <- [Jason.encode!(state_payload), html, terminal] do
      refute surface =~ sentinel
    end
  end

  test "phoenix observability api preserves 405, 404, and unavailable behavior" do
    unavailable_orchestrator = Module.concat(__MODULE__, :UnavailableOrchestrator)
    start_test_endpoint(orchestrator: unavailable_orchestrator, snapshot_timeout_ms: 5)

    assert json_response(post(build_conn(), "/api/v1/state", %{}), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(get(build_conn(), "/api/v1/refresh"), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(put(build_conn(), "/api/v1/pause", %{}), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(get(build_conn(), "/api/v1/webhooks/linear"), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(post(build_conn(), "/", %{}), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(post(build_conn(), "/api/v1/MT-1", %{}), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(get(build_conn(), "/unknown"), 404) ==
             %{"error" => %{"code" => "not_found", "message" => "Route not found"}}

    state_payload = json_response(get(build_conn(), "/api/v1/state"), 200)

    assert state_payload ==
             %{
               "generated_at" => state_payload["generated_at"],
               "error" => %{"code" => "snapshot_unavailable", "message" => "Snapshot unavailable"}
             }

    assert json_response(post(build_conn(), "/api/v1/refresh", %{}), 503) ==
             %{
               "error" => %{
                 "code" => "orchestrator_unavailable",
                 "message" => "Orchestrator is unavailable"
               }
             }

    assert json_response(post(build_conn(), "/api/v1/pause", %{"paused" => true}), 503) ==
             %{
               "error" => %{
                 "code" => "orchestrator_unavailable",
                 "message" => "Orchestrator is unavailable"
               }
             }

    assert json_response(post(build_conn(), "/api/v1/pause", %{"paused" => "yes"}), 422) ==
             %{
               "error" => %{
                 "code" => "invalid_pause_request",
                 "message" => "paused must be a boolean"
               }
             }

    remote_conn = %{build_conn() | remote_ip: {203, 0, 113, 9}}

    assert json_response(post(remote_conn, "/api/v1/pause", %{"paused" => true}), 403) ==
             %{
               "error" => %{
                 "code" => "operator_access_denied",
                 "message" => "Operator controls require loopback access"
               }
             }
  end

  test "verified Linear Issue webhook wakes reconcile without forwarding payload" do
    webhook_secret = "synthetic-webhook-secret"
    configure_webhook_secret(webhook_secret)

    orchestrator_name = Module.concat(__MODULE__, :WebhookOrchestrator)

    refresh = %{
      queued: true,
      coalesced: false,
      requested_at: DateTime.utc_now(),
      operations: ["poll", "reconcile"]
    }

    orchestrator_opts = [
      name: orchestrator_name,
      snapshot: static_snapshot(),
      refresh: refresh,
      refresh_recipient: self()
    ]

    start_supervised!({StaticOrchestrator, orchestrator_opts})

    start_test_endpoint(orchestrator: orchestrator_name)

    body = linear_webhook_body("Issue", System.system_time(:millisecond), "update")
    conn = post_linear_webhook(body, webhook_secret, event: "Issue")

    assert %{
             "accepted" => true,
             "coalesced" => false,
             "operations" => ["poll", "reconcile"],
             "queued" => true,
             "source" => "linear_webhook"
           } = json_response(conn, 200)

    assert_receive :refresh_requested
  end

  test "blocked tracker poll coalesces duplicate webhooks while control paths stay responsive" do
    webhook_secret = "synthetic-webhook-secret"
    configure_webhook_secret(webhook_secret)
    parent = self()

    poll_work_fn = fn request ->
      send(parent, {:blocked_poll_started, self(), request})

      receive do
        {:release_blocked_poll, result} -> result
      end
    end

    orchestrator_name = Module.concat(__MODULE__, :AsyncPollOrchestrator)

    {:ok, orchestrator_pid} =
      Orchestrator.start_link(
        name: orchestrator_name,
        poll_work_fn: poll_work_fn,
        poll_task_timeout_ms: 2_000
      )

    on_exit(fn ->
      if Process.alive?(orchestrator_pid), do: Process.exit(orchestrator_pid, :normal)
    end)

    assert_receive {:blocked_poll_started, first_poll_pid, first_request}, 500

    assert Enum.any?(Supervisor.which_children(SymphonyElixir.TaskSupervisor), fn
             {_id, ^first_poll_pid, :worker, _modules} -> true
             _child -> false
           end)

    timer_issue_id = "issue-responsive-timer"
    completion_issue_id = "issue-responsive-completion"
    completion_ref = make_ref()

    timer_entry = responsive_running_entry(timer_issue_id, "run-responsive-timer")

    completion_entry =
      responsive_running_entry(completion_issue_id, "run-responsive-completion")
      |> Map.put(:pid, self())
      |> Map.put(:ref, completion_ref)

    :sys.replace_state(orchestrator_pid, fn state ->
      %{
        state
        | run_ledger_path: nil,
          running: %{
            timer_issue_id => timer_entry,
            completion_issue_id => completion_entry
          },
          claimed: MapSet.new([timer_issue_id, completion_issue_id])
      }
    end)

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 250)

    body = linear_webhook_body("Issue", System.system_time(:millisecond), "update")
    started_ms = System.monotonic_time(:millisecond)

    first_webhook = post_linear_webhook(body, webhook_secret, event: "Issue")
    second_webhook = post_linear_webhook(body, webhook_secret, event: "Issue")

    assert System.monotonic_time(:millisecond) - started_ms < 250
    assert %{"accepted" => true, "coalesced" => true} = json_response(first_webhook, 200)
    assert %{"accepted" => true, "coalesced" => true} = json_response(second_webhook, 200)

    assert %{"dispatch_paused" => true, "changed" => true} =
             json_response(post(build_conn(), "/api/v1/pause", %{"paused" => true}), 202)

    send(orchestrator_pid, {:run_budget_timeout, timer_issue_id, "run-responsive-timer"})
    send(orchestrator_pid, {:DOWN, completion_ref, :process, self(), :normal})

    status_started_ms = System.monotonic_time(:millisecond)
    status_payload = json_response(get(build_conn(), "/api/v1/state"), 200)
    assert System.monotonic_time(:millisecond) - status_started_ms < 250
    assert status_payload["counts"]["parked"] == 1
    assert status_payload["counts"]["retrying"] == 1

    assert %{polling: %{checking?: true}, parked: [parked], retrying: [retrying]} =
             Orchestrator.snapshot(orchestrator_name, 250)

    assert parked.issue_id == timer_issue_id
    assert parked.terminal_reason == "time_budget_exhausted"
    assert retrying.issue_id == completion_issue_id

    refute_receive {:blocked_poll_started, _pid, _request}, 100

    send(first_poll_pid, {:release_blocked_poll, successful_poll_result(first_request)})
    assert_receive {:blocked_poll_started, second_poll_pid, second_request}, 500
    refute first_poll_pid == second_poll_pid
    refute Process.alive?(first_poll_pid)

    send(second_poll_pid, {:release_blocked_poll, successful_poll_result(second_request)})
    await_orchestrator_poll_idle(orchestrator_name)
    refute_receive {:blocked_poll_started, _pid, _request}, 100
  end

  test "refresh and webhook timeouts return bounded unavailable responses without caller exits" do
    webhook_secret = "synthetic-webhook-secret"
    configure_webhook_secret(webhook_secret)

    refresh_name = Module.concat(__MODULE__, :BlockingRefreshApiOrchestrator)
    start_supervised!({BlockingRefreshOrchestrator, name: refresh_name})
    start_test_endpoint(orchestrator: refresh_name)

    started_ms = System.monotonic_time(:millisecond)

    assert json_response(post(build_conn(), "/api/v1/refresh", %{}), 503)["error"]["code"] ==
             "orchestrator_unavailable"

    assert System.monotonic_time(:millisecond) - started_ms < 900
    Process.sleep(550)

    body = linear_webhook_body("Issue", System.system_time(:millisecond), "update")
    webhook_started_ms = System.monotonic_time(:millisecond)
    webhook = post_linear_webhook(body, webhook_secret, event: "Issue")

    assert json_response(webhook, 503)["error"]["code"] == "orchestrator_unavailable"
    assert System.monotonic_time(:millisecond) - webhook_started_ms < 900
  end

  test "Linear webhook rejects invalid authentication and stale timestamps before wake-up" do
    webhook_secret = "synthetic-webhook-secret"
    configure_webhook_secret(webhook_secret)

    orchestrator_name = Module.concat(__MODULE__, :RejectedWebhookOrchestrator)

    start_supervised!(
      {StaticOrchestrator,
       name: orchestrator_name,
       snapshot: static_snapshot(),
       refresh: %{
         queued: true,
         coalesced: false,
         requested_at: DateTime.utc_now(),
         operations: ["poll", "reconcile"]
       },
       refresh_recipient: self()}
    )

    start_test_endpoint(orchestrator: orchestrator_name)

    current_body = linear_webhook_body("Issue", System.system_time(:millisecond), "update")
    invalid_signature = post_linear_webhook(current_body, "wrong-secret", event: "Issue")

    assert json_response(invalid_signature, 401)["error"]["code"] == "invalid_webhook"
    refute_receive :refresh_requested

    stale_body = linear_webhook_body("Issue", System.system_time(:millisecond) - 61_000, "update")
    stale = post_linear_webhook(stale_body, webhook_secret, event: "Issue")

    assert json_response(stale, 401)["error"]["code"] == "invalid_webhook"
    refute_receive :refresh_requested
  end

  test "verified Linear Comment webhook wakes operator-command reconciliation" do
    webhook_secret = "synthetic-webhook-secret"
    configure_webhook_secret(webhook_secret)

    orchestrator_name = Module.concat(__MODULE__, :IgnoredWebhookOrchestrator)

    orchestrator_opts = [
      name: orchestrator_name,
      snapshot: static_snapshot(),
      refresh: %{
        queued: true,
        coalesced: false,
        requested_at: DateTime.utc_now(),
        operations: ["poll", "reconcile"]
      },
      refresh_recipient: self()
    ]

    start_supervised!({StaticOrchestrator, orchestrator_opts})

    start_test_endpoint(orchestrator: orchestrator_name)

    body = linear_webhook_body("Comment", System.system_time(:millisecond), "create")
    conn = post_linear_webhook(body, webhook_secret, event: "Comment")

    assert %{
             "accepted" => true,
             "queued" => true,
             "source" => "linear_webhook"
           } = json_response(conn, 200)

    assert_receive :refresh_requested
  end

  test "Linear webhook fails closed when its secret is not configured" do
    configure_webhook_secret(nil)
    start_test_endpoint(orchestrator: Module.concat(__MODULE__, :UnusedWebhookOrchestrator))

    body = linear_webhook_body("Issue", System.system_time(:millisecond), "update")
    conn = post_linear_webhook(body, "synthetic-webhook-secret", event: "Issue")

    assert json_response(conn, 503)["error"]["code"] == "webhook_not_configured"
  end

  test "phoenix observability api preserves snapshot timeout behavior" do
    timeout_orchestrator = Module.concat(__MODULE__, :TimeoutOrchestrator)
    {:ok, _pid} = SlowOrchestrator.start_link(name: timeout_orchestrator)
    start_test_endpoint(orchestrator: timeout_orchestrator, snapshot_timeout_ms: 1)

    timeout_payload = json_response(get(build_conn(), "/api/v1/state"), 200)

    assert timeout_payload ==
             %{
               "generated_at" => timeout_payload["generated_at"],
               "error" => %{"code" => "snapshot_timeout", "message" => "Snapshot timed out"}
             }
  end

  test "dashboard bootstraps liveview from embedded static assets" do
    orchestrator_name = Module.concat(__MODULE__, :AssetOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: static_snapshot(),
        refresh: %{
          queued: true,
          coalesced: false,
          requested_at: DateTime.utc_now(),
          operations: ["poll"]
        }
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    html = html_response(get(build_conn(), "/"), 200)
    assert html =~ "/dashboard.css"
    assert html =~ "/vendor/phoenix_html/phoenix_html.js"
    assert html =~ "/vendor/phoenix/phoenix.js"
    assert html =~ "/vendor/phoenix_live_view/phoenix_live_view.js"
    refute html =~ "/assets/app.js"
    refute html =~ "<style>"

    dashboard_css = response(get(build_conn(), "/dashboard.css"), 200)
    assert dashboard_css =~ ":root {"
    assert dashboard_css =~ ".status-badge-live"
    assert dashboard_css =~ "[data-phx-main].phx-connected .status-badge-live"
    assert dashboard_css =~ "[data-phx-main].phx-connected .status-badge-offline"

    phoenix_html_js = response(get(build_conn(), "/vendor/phoenix_html/phoenix_html.js"), 200)
    assert phoenix_html_js =~ "phoenix.link.click"

    phoenix_js = response(get(build_conn(), "/vendor/phoenix/phoenix.js"), 200)
    assert phoenix_js =~ "var Phoenix = (() => {"

    live_view_js =
      response(get(build_conn(), "/vendor/phoenix_live_view/phoenix_live_view.js"), 200)

    assert live_view_js =~ "var LiveView = (() => {"
  end

  test "dashboard liveview renders and refreshes over pubsub" do
    orchestrator_name = Module.concat(__MODULE__, :DashboardOrchestrator)
    snapshot = static_snapshot()

    {:ok, orchestrator_pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: snapshot,
        refresh: %{
          queued: true,
          coalesced: true,
          requested_at: DateTime.utc_now(),
          operations: ["poll"]
        }
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    {:ok, view, html} = live(build_conn(), "/")
    assert html =~ "Operations Dashboard"
    assert html =~ "MT-HTTP"
    assert html =~ "MT-RETRY"
    assert html =~ "MT-PARKED"
    assert html =~ "wait-http"
    assert html =~ "Parked waits"
    refute html =~ "SENSITIVE-BL10-DO-NOT-EXPOSE"
    assert html =~ "Runtime"
    assert html =~ "Live"
    assert html =~ "Offline"
    assert html =~ "Copy ID"
    assert html =~ "Codex update"
    refute html =~ "data-runtime-clock="
    refute html =~ "setInterval(refreshRuntimeClocks"
    refute html =~ "Refresh now"
    refute html =~ "Transport"
    assert html =~ "status-badge-live"
    assert html =~ "status-badge-offline"

    updated_snapshot =
      put_in(snapshot.running, [
        %{
          issue_id: "issue-http",
          identifier: "MT-HTTP",
          state: "In Progress",
          session_id: "thread-http",
          turn_count: 8,
          last_codex_event: :turn_completed,
          last_codex_message: %{
            event: :notification,
            message: %{
              payload: %{
                "method" => "codex/event/agent_message_content_delta",
                "params" => %{
                  "msg" => %{
                    "content" => "SENSITIVE-BL10-DO-NOT-EXPOSE"
                  }
                }
              }
            }
          },
          last_codex_timestamp: DateTime.utc_now(),
          codex_input_tokens: 10,
          codex_output_tokens: 12,
          codex_total_tokens: 22,
          started_at: DateTime.utc_now()
        }
      ])

    :sys.replace_state(orchestrator_pid, fn state ->
      Keyword.put(state, :snapshot, updated_snapshot)
    end)

    StatusDashboard.notify_update()

    assert_eventually(fn ->
      rendered = render(view)

      rendered =~ "turn_completed" and
        not String.contains?(rendered, "SENSITIVE-BL10-DO-NOT-EXPOSE")
    end)
  end

  test "dashboard liveview keeps parked waits visible without running or retrying work" do
    orchestrator_name = Module.concat(__MODULE__, :ParkedOnlyDashboardOrchestrator)

    parked_snapshot =
      static_snapshot()
      |> Map.put(:running, [])
      |> Map.put(:retrying, [])
      |> put_in([:parked, Access.at(0), :worker_host], "worker-a")
      |> put_in([:parked, Access.at(0), :workspace_path], "/srv/symphony/MT-PARKED")

    {:ok, orchestrator_pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: parked_snapshot,
        refresh: :unavailable
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    {:ok, view, html} = live(build_conn(), "/")
    assert html =~ "No active sessions."
    assert html =~ "Unresolved parked waits are listed separately below."
    assert html =~ "No issues are currently backing off."
    assert html =~ ~r/metric-label[^>]*>Parked<\/p>\s*<p class="metric-value numeric">1<\/p>/
    assert html =~ "MT-PARKED"
    assert html =~ "wait-http"
    assert html =~ "waiting_owner"
    assert html =~ "approve, reject"
    assert html =~ "run-http"
    assert html =~ "attempt 1"
    assert html =~ "turn_budget_exhausted"
    assert html =~ "worker-a"
    assert html =~ "/srv/symphony/MT-PARKED"

    refreshed_snapshot =
      put_in(parked_snapshot.parked, [
        %{
          issue_id: "issue-refreshed-parked",
          identifier: "MT-PARKED-REFRESHED",
          wait_id: "wait-refreshed",
          reason: "waiting_infrastructure",
          allowed_actions: ["retry", "reject"],
          tracker_state: "Blocked",
          run_id: "run-refreshed",
          attempt: 3,
          stage: "parked",
          terminal_reason: "worker_start_failed",
          worker_host: "worker-b",
          workspace_path: "/srv/symphony/MT-PARKED-REFRESHED",
          parked_at: DateTime.utc_now()
        }
      ])

    :sys.replace_state(orchestrator_pid, fn state ->
      Keyword.put(state, :snapshot, refreshed_snapshot)
    end)

    StatusDashboard.notify_update()

    assert_eventually(fn ->
      rendered = render(view)

      rendered =~ "MT-PARKED-REFRESHED" and rendered =~ "wait-refreshed" and
        not String.contains?(rendered, "MT-PARKED</span>")
    end)
  end

  test "parked API and LiveView share bounded control-safe sorted collection projection" do
    orchestrator_name = Module.concat(__MODULE__, :BoundedParkedDashboardOrchestrator)
    long_path = "/srv/" <> String.duplicate("long-segment/", 80)

    regular_waits =
      for index <- 1..127 do
        %{
          issue_id: "issue-#{index}",
          identifier: "MT-#{String.pad_leading(Integer.to_string(index), 3, "0")}",
          wait_id: "wait-#{index}",
          reason: "waiting_owner",
          allowed_actions: ["approve", "reject"],
          tracker_state: "Human Review",
          run_id: "run-#{index}",
          attempt: index,
          stage: "parked",
          terminal_reason: nil,
          worker_host: "worker-a",
          workspace_path: "/srv/symphony/MT-#{index}",
          parked_at: DateTime.utc_now()
        }
      end

    adversarial_waits = [
      %{
        issue_id: "issue-control",
        identifier: "AB\nCONTROL",
        wait_id: "wait-control",
        reason: "free_form_reason",
        allowed_actions: ["destroy"],
        tracker_state: "Human\e]0;title",
        run_id: "run-control",
        attempt: -1,
        stage: "free_form_stage",
        terminal_reason: "free_form_terminal",
        worker_host: "worker\tcontrol",
        workspace_path: <<0xFF>>,
        parked_at: DateTime.utc_now()
      },
      %{
        issue_id: "issue-bounded",
        identifier: "AA-BOUNDED",
        wait_id: "wait-bounded",
        reason: "waiting_infrastructure",
        allowed_actions: ["retry", "reject"],
        tracker_state: "Blocked",
        run_id: "run-bounded",
        attempt: 2,
        stage: "parked",
        terminal_reason: "time_budget_exhausted",
        worker_host: "worker-b",
        workspace_path: long_path,
        parked_at: DateTime.utc_now()
      },
      %{
        issue_id: "issue-sort-first",
        identifier: "AA-000",
        wait_id: "wait-sort-first",
        reason: "waiting_secret",
        allowed_actions: ["retry", "reject"],
        tracker_state: "Blocked",
        run_id: "run-sort-first",
        attempt: 1,
        stage: "parked",
        terminal_reason: nil,
        parked_at: DateTime.utc_now()
      }
    ]

    snapshot =
      static_snapshot()
      |> Map.put(:running, [])
      |> Map.put(:retrying, [])
      |> Map.put(:parked, regular_waits ++ adversarial_waits)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: snapshot,
        refresh: :unavailable
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    state_payload = json_response(get(build_conn(), "/api/v1/state"), 200)
    assert state_payload["counts"]["parked"] == 130

    assert state_payload["parked_meta"] == %{
             "total_count" => 130,
             "returned_count" => 100,
             "omitted_count" => 30,
             "truncated" => true,
             "row_limit" => 100,
             "byte_limit" => 65_536,
             "returned_bytes" => state_payload["parked_meta"]["returned_bytes"]
           }

    assert state_payload["parked_meta"]["returned_bytes"] ==
             byte_size(Jason.encode!(state_payload["parked"]))

    assert state_payload["parked_meta"]["returned_bytes"] <= 65_536
    assert length(state_payload["parked"]) == 100
    assert hd(state_payload["parked"])["issue_identifier"] == "AA-000"

    control_row = Enum.find(state_payload["parked"], &(&1["wait_id"] == "wait-control"))
    assert control_row["issue_identifier"] == "AB\\nCONTROL"
    assert control_row["tracker_state"] == "Human\\u{1B}]0;title"
    assert control_row["worker_host"] == "worker\\tcontrol"
    assert control_row["workspace_path"] == "invalid-utf8"
    assert control_row["reason"] == nil
    assert control_row["allowed_actions"] == []
    assert control_row["attempt"] == nil
    assert control_row["stage"] == nil
    assert control_row["terminal_reason"] == nil

    bounded_issue = json_response(get(build_conn(), "/api/v1/AA-BOUNDED"), 200)
    assert byte_size(bounded_issue["parked"]["workspace_path"]) <= 512
    assert bounded_issue["workspace"]["path"] == bounded_issue["parked"]["workspace_path"]
    assert "workspace_path" in bounded_issue["parked"]["truncated_fields"]
    refute bounded_issue["parked"]["workspace_path"] == long_path

    {:ok, _view, html} = live(build_conn(), "/")
    assert html =~ "Showing 100 of 130 parked waits; 30 omitted by the bounded projection."
    assert html =~ "AA-000"
    assert html =~ "AB\\nCONTROL"
    assert html =~ "invalid-utf8"
    assert html =~ "display truncated"
    refute html =~ long_path
    refute html =~ <<0xFF>>
    refute html =~ "destroy"
    refute html =~ "free_form_reason"
  end

  test "parked collection accounts for exact JSON array bytes at and above the limit" do
    assert %{rows: [], metadata: %{returned_bytes: 2}} = ParkedProjection.collection([])

    exact_prefix = tuned_parked_prefix(65_536, 60)
    one_over_prefix = tuned_parked_prefix(65_537, 60)
    old_under_count_prefix = tuned_parked_prefix(65_538, 60)
    tail = for index <- 61..105, do: boundary_parked_wait(index, "ZZ")

    assert length(exact_prefix ++ tail) > 100
    assert encoded_projected_bytes(exact_prefix) == 65_536
    assert encoded_projected_bytes(one_over_prefix) == 65_537
    assert encoded_projected_bytes(old_under_count_prefix) == 65_538

    exact = ParkedProjection.collection(exact_prefix ++ tail)
    assert exact.metadata.returned_count == 60
    assert exact.metadata.omitted_count == 45
    assert exact.metadata.truncated
    assert exact.metadata.returned_bytes == 65_536
    assert exact.metadata.returned_bytes == byte_size(Jason.encode!(exact.rows))
    assert exact.metadata.returned_bytes <= exact.metadata.byte_limit

    for over_prefix <- [one_over_prefix, old_under_count_prefix] do
      over = ParkedProjection.collection(over_prefix ++ tail)

      assert over.metadata.returned_count == 59
      assert over.metadata.omitted_count == 46
      assert over.metadata.truncated
      assert over.metadata.returned_bytes == byte_size(Jason.encode!(over.rows))
      assert over.metadata.returned_bytes <= over.metadata.byte_limit
    end

    orchestrator_name = Module.concat(__MODULE__, :ExactParkedByteBoundaryOrchestrator)

    snapshot =
      static_snapshot()
      |> Map.put(:running, [])
      |> Map.put(:retrying, [])
      |> Map.put(:parked, exact_prefix ++ tail)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: snapshot,
        refresh: :unavailable
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    state_payload = json_response(get(build_conn(), "/api/v1/state"), 200)
    api_returned_bytes = byte_size(Jason.encode!(state_payload["parked"]))

    assert state_payload["parked_meta"]["returned_bytes"] == api_returned_bytes
    assert api_returned_bytes == 65_536
    assert api_returned_bytes <= state_payload["parked_meta"]["byte_limit"]

    {:ok, view, html} = live(build_conn(), "/")
    live_payload = :sys.get_state(view.pid).socket.assigns.payload
    live_returned_bytes = byte_size(Jason.encode!(live_payload.parked))

    assert live_payload.parked_meta == exact.metadata
    assert live_payload.parked_meta.returned_bytes == live_returned_bytes
    assert live_returned_bytes == api_returned_bytes
    assert Jason.decode!(Jason.encode!(live_payload.parked)) == state_payload["parked"]
    assert html =~ "Showing 60 of 105 parked waits; 45 omitted by the bounded projection."
  end

  test "cleanup failure remains durably owned and truthful after restart across every surface" do
    test_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-cleanup-pending-surfaces-#{System.unique_integer([:positive])}"
      )

    captured_root = Path.join(test_root, "captured-root")
    workspace = Path.join(test_root, "outside-root/MT-CLEANUP-RESTART")
    sentinel = Path.join(workspace, "must-survive")
    ledger_path = Path.join(test_root, "run-ledger.jsonl")
    issue_id = "issue-cleanup-restart"
    identifier = "MT-CLEANUP-RESTART"
    run_id = "run-cleanup-restart"

    base = %{
      run_id: run_id,
      issue_id: issue_id,
      issue_identifier: identifier,
      attempt: 2,
      worker_host: nil,
      workspace_path: workspace,
      workspace_root: captured_root
    }

    on_exit(fn -> File.rm_rf(test_root) end)
    File.mkdir_p!(workspace)
    File.write!(sentinel, "preserve")

    assert :ok = RunLedger.append(ledger_path, Map.merge(base, %{transition: "run_claimed", stage: "claimed"}))
    assert :ok = RunLedger.append(ledger_path, Map.merge(base, %{transition: "run_started", stage: "running"}))

    assert :ok =
             RunLedger.append(
               ledger_path,
               Map.merge(base, %{
                 transition: "run_stopped",
                 stage: "released",
                 terminal_reason: "tracker_terminal",
                 next_action: "none"
               })
             )

    assert :ok =
             RunLedger.append(
               ledger_path,
               Map.merge(base, %{
                 transition: "workspace_cleanup_requested",
                 stage: "cleanup",
                 terminal_reason: "tracker_terminal"
               })
             )

    assert {:ok, restarted} = Orchestrator.init(run_ledger_path: ledger_path)
    if is_reference(restarted.tick_timer_ref), do: Process.cancel_timer(restarted.tick_timer_ref)

    assert File.read!(sentinel) == "preserve"
    assert MapSet.member?(restarted.claimed, issue_id)

    assert %{
             status: :operator_required,
             cleanup_error: :workspace_cleanup_failed
           } = restarted.cleanup_pending[issue_id]

    assert {:ok, before_restart_events} = RunLedger.read_events(ledger_path)

    assert {:ok, restarted_again} = Orchestrator.init(run_ledger_path: ledger_path)

    if is_reference(restarted_again.tick_timer_ref),
      do: Process.cancel_timer(restarted_again.tick_timer_ref)

    assert restarted_again.cleanup_pending[issue_id].status == :operator_required
    assert File.read!(sentinel) == "preserve"
    assert {:ok, after_restart_events} = RunLedger.read_events(ledger_path)

    assert Enum.count(before_restart_events, &(&1["transition"] == "workspace_cleanup_io_started")) ==
             1

    assert Enum.count(after_restart_events, &(&1["transition"] == "workspace_cleanup_io_started")) ==
             1

    assert {:reply, raw_snapshot, _state} =
             Orchestrator.handle_call(:snapshot, {self(), make_ref()}, restarted)

    assert [cleanup_row] = Enum.filter(raw_snapshot.retrying, &(&1.stage == "cleanup_pending"))
    assert cleanup_row.error == "workspace_cleanup_failed"
    assert cleanup_row.due_in_ms == nil
    assert cleanup_row.workspace_path == workspace

    pending_entry = restarted.cleanup_pending[issue_id] |> Map.delete(:cleanup_error)
    pending_state = put_in(restarted.cleanup_pending[issue_id], pending_entry)

    missing_state =
      put_in(
        restarted.cleanup_pending[issue_id],
        Map.put(pending_entry, :cleanup_error, :workspace_affinity_missing)
      )

    assert {:reply, pending_snapshot, _state} =
             Orchestrator.handle_call(:snapshot, {self(), make_ref()}, pending_state)

    assert {:reply, missing_snapshot, _state} =
             Orchestrator.handle_call(:snapshot, {self(), make_ref()}, missing_state)

    assert Enum.find(pending_snapshot.retrying, &(&1.stage == "cleanup_pending")).error ==
             "workspace_cleanup_pending"

    assert Enum.find(missing_snapshot.retrying, &(&1.stage == "cleanup_pending")).error ==
             "workspace_affinity_missing"

    for code <- ~w(workspace_cleanup_pending workspace_cleanup_failed workspace_affinity_missing workspace_preservation_required) do
      assert SymphonyElixir.ObservabilitySanitizer.retry_error_code(code) == code
    end

    terminal = StatusDashboard.format_snapshot_content_for_test({:ok, raw_snapshot}, 0.0, 160)
    assert terminal =~ "Workspace cleanup pending"
    assert terminal =~ "cleanup_pending"
    assert terminal =~ "error_code=workspace_cleanup_failed"
    assert terminal =~ "host=local"
    assert terminal =~ "path=#{String.slice(workspace, 0, 16)}"
    refute terminal =~ " in 0.000s"

    orchestrator_name = Module.concat(__MODULE__, :CleanupPendingRestartOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: raw_snapshot,
        refresh: :unavailable
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    state_payload = json_response(get(build_conn(), "/api/v1/state"), 200)

    assert state_payload["counts"] == %{
             "running" => 0,
             "retrying" => 0,
             "cleanup_pending" => 1,
             "parked" => 0
           }

    assert state_payload["retrying"] == []

    assert [
             %{
               "issue_identifier" => ^identifier,
               "stage" => "cleanup_pending",
               "error_code" => "workspace_cleanup_failed",
               "due_at" => nil,
               "worker_host" => nil,
               "workspace_path" => ^workspace
             }
           ] = state_payload["cleanup_pending"]

    assert %{
             "status" => "cleanup_pending",
             "retry" => %{
               "stage" => "cleanup_pending",
               "error_code" => "workspace_cleanup_failed",
               "due_at" => nil,
               "workspace_path" => ^workspace
             },
             "last_error_code" => "workspace_cleanup_failed"
           } = json_response(get(build_conn(), "/api/v1/#{identifier}"), 200)

    {:ok, _view, html} = live(build_conn(), "/")
    assert html =~ "Workspace cleanup pending"
    assert html =~ identifier
    assert html =~ "cleanup_pending"
    assert html =~ "workspace_cleanup_failed"
    assert html =~ workspace
    assert html =~ "No issues are currently backing off."
    refute html =~ "0.000s"

    assert {:ok, recovery} = RunLedger.reconcile_startup(ledger_path, "runner-cleanup-audit")
    assert recovery.cleanup_pending[issue_id]["workspace_path"] == workspace
  end

  test "dashboard liveview renders an unavailable state without crashing" do
    start_test_endpoint(
      orchestrator: Module.concat(__MODULE__, :MissingDashboardOrchestrator),
      snapshot_timeout_ms: 5
    )

    {:ok, _view, html} = live(build_conn(), "/")
    assert html =~ "Snapshot unavailable"
    assert html =~ "snapshot_unavailable"
  end

  test "http server serves embedded assets, accepts form posts, and rejects invalid hosts" do
    spec = HttpServer.child_spec(port: 0)
    assert spec.id == HttpServer
    assert spec.start == {HttpServer, :start_link, [[port: 0]]}

    assert :ignore = HttpServer.start_link(port: nil)
    assert HttpServer.bound_port() == nil

    snapshot = static_snapshot()
    orchestrator_name = Module.concat(__MODULE__, :BoundPortOrchestrator)

    refresh = %{
      queued: true,
      coalesced: false,
      requested_at: DateTime.utc_now(),
      operations: ["poll"]
    }

    server_opts = [
      host: "127.0.0.1",
      port: 0,
      orchestrator: orchestrator_name,
      snapshot_timeout_ms: 50
    ]

    start_supervised!({StaticOrchestrator, name: orchestrator_name, snapshot: snapshot, refresh: refresh})

    start_supervised!({HttpServer, server_opts})

    port = wait_for_bound_port()
    assert port == HttpServer.bound_port()

    response = Req.get!("http://127.0.0.1:#{port}/api/v1/state")
    assert response.status == 200

    assert response.body["counts"] == %{
             "running" => 1,
             "retrying" => 1,
             "cleanup_pending" => 0,
             "parked" => 1
           }

    dashboard_css = Req.get!("http://127.0.0.1:#{port}/dashboard.css")
    assert dashboard_css.status == 200
    assert dashboard_css.body =~ ":root {"

    phoenix_js = Req.get!("http://127.0.0.1:#{port}/vendor/phoenix/phoenix.js")
    assert phoenix_js.status == 200
    assert phoenix_js.body =~ "var Phoenix = (() => {"

    refresh_response =
      Req.post!("http://127.0.0.1:#{port}/api/v1/refresh",
        headers: [{"content-type", "application/x-www-form-urlencoded"}],
        body: ""
      )

    assert refresh_response.status == 202
    assert refresh_response.body["queued"] == true

    method_not_allowed_response =
      Req.post!("http://127.0.0.1:#{port}/api/v1/state",
        headers: [{"content-type", "application/x-www-form-urlencoded"}],
        body: ""
      )

    assert method_not_allowed_response.status == 405
    assert method_not_allowed_response.body["error"]["code"] == "method_not_allowed"

    assert {:error, _reason} = HttpServer.start_link(host: "bad host", port: 0)
  end

  defp start_test_endpoint(overrides) do
    endpoint_config =
      :symphony_elixir
      |> Application.get_env(SymphonyElixirWeb.Endpoint, [])
      |> Keyword.merge(server: false, secret_key_base: String.duplicate("s", 64))
      |> Keyword.merge(overrides)

    Application.put_env(:symphony_elixir, SymphonyElixirWeb.Endpoint, endpoint_config)
    start_supervised!({SymphonyElixirWeb.Endpoint, []})
  end

  defp static_snapshot do
    %{
      running: [
        %{
          issue_id: "issue-http",
          identifier: "MT-HTTP",
          state: "In Progress",
          session_id: "thread-http",
          session_title: "SENSITIVE-BL10-DO-NOT-EXPOSE",
          turn_count: 7,
          codex_app_server_pid: nil,
          last_codex_message: "SENSITIVE-BL10-DO-NOT-EXPOSE",
          last_codex_timestamp: nil,
          last_codex_event: :notification,
          codex_input_tokens: 4,
          codex_output_tokens: 8,
          codex_total_tokens: 12,
          budget: %{
            turns: %{limit: 20, used: 7, remaining: 13},
            tokens: %{
              limit: 250_000,
              used: 12,
              remaining: 249_988,
              telemetry_observed: true
            },
            time: %{limit: 7_200, used: 42, remaining: 7_158}
          },
          started_at: DateTime.utc_now()
        }
      ],
      retrying: [
        %{
          issue_id: "issue-retry",
          identifier: "MT-RETRY",
          attempt: 2,
          due_in_ms: 2_000,
          error: "boom"
        }
      ],
      parked: [
        %{
          issue_id: "issue-parked",
          identifier: "MT-PARKED",
          wait_id: "wait-http",
          reason: "waiting_owner",
          allowed_actions: ["approve", "reject"],
          tracker_state: "Human Review",
          run_id: "run-http",
          attempt: 1,
          stage: "parked",
          terminal_reason: "turn_budget_exhausted",
          parked_at: DateTime.utc_now()
        }
      ],
      control: %{dispatch_paused: false},
      capabilities: %{
        dynamic_tools: ["linear_graphql"],
        mcp_tool_auto_approve: [],
        mcp_elicitation_auto_approve: []
      },
      codex_totals: %{input_tokens: 4, output_tokens: 8, total_tokens: 12, seconds_running: 42.5},
      rate_limits: %{limit_id: "codex", primary: %{remaining: 11}}
    }
  end

  defp tuned_parked_prefix(target_bytes, count) do
    waits = for index <- 1..count, do: boundary_parked_wait(index, "AA")
    padding_bytes = target_bytes - encoded_projected_bytes(waits)

    {waits, remaining_bytes} =
      Enum.map_reduce(waits, padding_bytes, fn wait, remaining_bytes ->
        add_boundary_padding(wait, remaining_bytes)
      end)

    if remaining_bytes != 0 or encoded_projected_bytes(waits) != target_bytes do
      raise "could not tune parked projection to #{target_bytes} encoded bytes"
    end

    waits
  end

  defp boundary_parked_wait(index, sort_prefix) do
    suffix = String.pad_leading(Integer.to_string(index), 3, "0")

    %{
      issue_id: "issue-#{suffix}",
      identifier: "#{sort_prefix}-#{suffix}",
      wait_id: "wait-#{suffix}",
      reason: "waiting_owner",
      tracker_state: "Blocked",
      run_id: "run-#{suffix}",
      attempt: 0,
      stage: "parked",
      terminal_reason: nil,
      worker_host: "worker",
      workspace_path: "/srv/boundary/#{suffix}",
      parked_at: ~U[2026-08-04 00:00:00Z]
    }
  end

  defp add_boundary_padding(wait, remaining_bytes) do
    [
      {:workspace_path, 512},
      {:worker_host, 128},
      {:tracker_state, 128},
      {:run_id, 128},
      {:wait_id, 128},
      {:issue_id, 128},
      {:identifier, 96}
    ]
    |> Enum.reduce({wait, remaining_bytes}, fn {field, limit}, {wait, remaining_bytes} ->
      value = Map.fetch!(wait, field)
      added_bytes = min(remaining_bytes, limit - byte_size(value))

      {
        Map.put(wait, field, value <> String.duplicate("x", added_bytes)),
        remaining_bytes - added_bytes
      }
    end)
  end

  defp encoded_projected_bytes(waits) do
    waits
    |> Enum.map(&ParkedProjection.row/1)
    |> Jason.encode!()
    |> byte_size()
  end

  defp wait_for_bound_port do
    assert_eventually(fn ->
      is_integer(HttpServer.bound_port())
    end)

    HttpServer.bound_port()
  end

  defp configure_webhook_secret(secret) do
    webhook_env = "SYMPHONY_TEST_LINEAR_WEBHOOK_SECRET"
    previous_webhook_env = System.get_env(webhook_env)
    on_exit(fn -> restore_env(webhook_env, previous_webhook_env) end)

    if secret do
      System.put_env(webhook_env, secret)
    else
      System.delete_env(webhook_env)
    end

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_webhook_secret: "$#{webhook_env}"
    )

    ensure_workflow_store_running()
    send(WorkflowStore, :poll)

    assert_eventually(fn ->
      Config.settings!().tracker.webhook_secret == secret
    end)
  end

  defp linear_webhook_body(type, timestamp, action) do
    Jason.encode!(%{
      "action" => action,
      "data" => %{"id" => "issue-webhook"},
      "type" => type,
      "webhookTimestamp" => timestamp
    })
  end

  defp post_linear_webhook(body, secret, opts) do
    signature = :crypto.mac(:hmac, :sha256, secret, body) |> Base.encode16(case: :lower)

    build_conn()
    |> Plug.Conn.put_req_header("content-type", "application/json")
    |> Plug.Conn.put_req_header("linear-delivery", "234d1a4e-b617-4388-90fe-adc3633d6b72")
    |> Plug.Conn.put_req_header("linear-event", Keyword.fetch!(opts, :event))
    |> Plug.Conn.put_req_header("linear-signature", signature)
    |> post("/api/v1/webhooks/linear", body)
  end

  defp assert_eventually(fun, attempts \\ 20)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(25)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition not met in time")

  defp await_orchestrator_poll_idle(orchestrator_name) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    await_orchestrator_poll_idle(orchestrator_name, deadline)
  end

  defp await_orchestrator_poll_idle(orchestrator_name, deadline) do
    snapshot = Orchestrator.snapshot(orchestrator_name, 250)

    case snapshot do
      %{polling: %{checking?: false, next_poll_in_ms: next_poll_in_ms}}
      when is_integer(next_poll_in_ms) and next_poll_in_ms > 0 ->
        :ok

      last_snapshot ->
        now = System.monotonic_time(:millisecond)

        if now >= deadline do
          flunk(
            "orchestrator poll did not become idle within 5000ms; " <>
              "last polling state: #{inspect(polling_state(last_snapshot))}"
          )
        else
          Process.sleep(min(25, deadline - now))
          await_orchestrator_poll_idle(orchestrator_name, deadline)
        end
    end
  end

  defp polling_state(%{polling: polling}), do: polling
  defp polling_state(other), do: other

  defp successful_poll_result(request) do
    %{
      request: request,
      running: {:ok, []},
      parked: {:ok, []},
      comments: %{},
      dispatch: {:ok, []}
    }
  end

  defp responsive_running_entry(issue_id, run_id) do
    agent_pid = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      if Process.alive?(agent_pid), do: Process.exit(agent_pid, :kill)
    end)

    %{
      pid: agent_pid,
      ref: nil,
      run_id: run_id,
      retry_attempt: 1,
      identifier: String.upcase(issue_id),
      issue: %Issue{id: issue_id, identifier: String.upcase(issue_id), state: "In Progress"},
      started_at: DateTime.utc_now(),
      session_id: nil,
      worker_host: nil,
      workspace_path: nil,
      workspace_root: nil,
      codex_input_tokens: 0,
      codex_output_tokens: 0,
      codex_total_tokens: 0,
      codex_token_telemetry_observed: false,
      turn_count: 0,
      run_budget: %{max_turns: 20, max_tokens: nil, max_seconds: 60},
      run_budget_timer_ref: nil
    }
  end

  defp ensure_workflow_store_running do
    if Process.whereis(WorkflowStore) do
      :ok
    else
      case Supervisor.restart_child(SymphonyElixir.Supervisor, WorkflowStore) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end
    end
  end
end
