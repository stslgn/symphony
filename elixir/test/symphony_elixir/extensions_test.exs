defmodule SymphonyElixir.ExtensionsTest do
  use SymphonyElixir.TestSupport

  import Phoenix.ConnTest
  import Phoenix.LiveViewTest

  alias SymphonyElixir.Linear.Adapter
  alias SymphonyElixir.Tracker.Memory

  @endpoint SymphonyElixirWeb.Endpoint

  defmodule FakeLinearClient do
    def fetch_candidate_issues do
      send(self(), :fetch_candidate_issues_called)
      {:ok, [:candidate]}
    end

    def fetch_issues_by_states(states) do
      send(self(), {:fetch_issues_by_states_called, states})
      {:ok, states}
    end

    def fetch_issue_states_by_ids(issue_ids) do
      send(self(), {:fetch_issue_states_by_ids_called, issue_ids})
      {:ok, issue_ids}
    end

    def fetch_comments_since(issue_id, created_after) do
      send(self(), {:fetch_comments_since_called, issue_id, created_after})
      {:ok, [:comment]}
    end

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
    assert {:error, _reason} = WorkflowStore.force_reload()
    assert {:ok, %{prompt: "Second prompt"}} = Workflow.current()

    third_workflow = Path.join(Path.dirname(Workflow.workflow_file_path()), "THIRD_WORKFLOW.md")
    write_workflow_file!(third_workflow, prompt: "Third prompt")
    Workflow.set_workflow_file_path(third_workflow)
    assert {:ok, %{prompt: "Third prompt"}} = Workflow.current()

    assert :ok = Supervisor.terminate_child(SymphonyElixir.Supervisor, WorkflowStore)
    assert {:ok, %{prompt: "Third prompt"}} = WorkflowStore.current()
    assert :ok = WorkflowStore.force_reload()
    assert {:ok, _pid} = Supervisor.restart_child(SymphonyElixir.Supervisor, WorkflowStore)
  end

  test "workflow store init stops on missing workflow file" do
    missing_path = Path.join(Path.dirname(Workflow.workflow_file_path()), "MISSING_WORKFLOW.md")
    Workflow.set_workflow_file_path(missing_path)

    assert {:stop, {:missing_workflow_file, ^missing_path, :enoent}} = WorkflowStore.init([])
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
    assert SymphonyElixir.Tracker.adapter() == Memory
    assert {:ok, [^issue]} = SymphonyElixir.Tracker.fetch_candidate_issues()
    assert {:ok, [^issue]} = SymphonyElixir.Tracker.fetch_issues_by_states([" in progress ", 42])
    assert {:ok, [^issue]} = SymphonyElixir.Tracker.fetch_issue_states_by_ids(["issue-1"])
    assert :ok = SymphonyElixir.Tracker.create_comment("issue-1", "comment")
    assert :ok = SymphonyElixir.Tracker.update_issue_state("issue-1", "Done")
    assert_receive {:memory_tracker_comment, "issue-1", "comment"}
    assert_receive {:memory_tracker_state_update, "issue-1", "Done"}

    Application.delete_env(:symphony_elixir, :memory_tracker_recipient)
    assert :ok = Memory.create_comment("issue-1", "quiet")
    assert :ok = Memory.update_issue_state("issue-1", "Quiet")

    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "linear")
    assert SymphonyElixir.Tracker.adapter() == Adapter
  end

  test "linear adapter delegates reads and validates mutation responses" do
    Application.put_env(:symphony_elixir, :linear_client_module, FakeLinearClient)

    assert {:ok, [:candidate]} = Adapter.fetch_candidate_issues()
    assert_receive :fetch_candidate_issues_called

    assert {:ok, ["Todo"]} = Adapter.fetch_issues_by_states(["Todo"])
    assert_receive {:fetch_issues_by_states_called, ["Todo"]}

    assert {:ok, ["issue-1"]} = Adapter.fetch_issue_states_by_ids(["issue-1"])
    assert_receive {:fetch_issue_states_by_ids_called, ["issue-1"]}

    created_after = ~U[2026-08-03 10:00:00Z]
    assert {:ok, [:comment]} = Adapter.fetch_comments_since("issue-1", created_after)
    assert_receive {:fetch_comments_since_called, "issue-1", ^created_after}

    Process.put(
      {FakeLinearClient, :graphql_result},
      {:ok, %{"data" => %{"commentCreate" => %{"success" => true}}}}
    )

    assert :ok = Adapter.create_comment("issue-1", "hello")
    assert_receive {:graphql_called, create_comment_query, %{body: "hello", issueId: "issue-1"}}
    assert create_comment_query =~ "commentCreate"

    Process.put(
      {FakeLinearClient, :graphql_result},
      {:ok, %{"data" => %{"commentCreate" => %{"success" => false}}}}
    )

    assert {:error, :comment_create_failed} =
             Adapter.create_comment("issue-1", "broken")

    Process.put({FakeLinearClient, :graphql_result}, {:error, :boom})

    assert {:error, :boom} = Adapter.create_comment("issue-1", "boom")

    Process.put({FakeLinearClient, :graphql_result}, {:ok, %{"data" => %{}}})
    assert {:error, :comment_create_failed} = Adapter.create_comment("issue-1", "weird")

    Process.put({FakeLinearClient, :graphql_result}, :unexpected)
    assert {:error, :comment_create_failed} = Adapter.create_comment("issue-1", "odd")

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

    assert :ok = Adapter.update_issue_state("issue-1", "Done")
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
             Adapter.update_issue_state("issue-1", "Broken")

    Process.put({FakeLinearClient, :graphql_results}, [{:error, :boom}])

    assert {:error, :boom} = Adapter.update_issue_state("issue-1", "Boom")

    Process.put({FakeLinearClient, :graphql_results}, [{:ok, %{"data" => %{}}}])
    assert {:error, :state_not_found} = Adapter.update_issue_state("issue-1", "Missing")

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

    assert {:error, :issue_update_failed} = Adapter.update_issue_state("issue-1", "Weird")

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

    assert {:error, :issue_update_failed} = Adapter.update_issue_state("issue-1", "Odd")
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
             "counts" => %{"running" => 1, "retrying" => 1, "parked" => 1},
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
                 "parked_at" => state_payload["parked"] |> List.first() |> Map.fetch!("parked_at")
               }
             ],
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
    assert response.body["counts"] == %{"running" => 1, "retrying" => 1, "parked" => 1}

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
    assert_eventually(
      fn ->
        case Orchestrator.snapshot(orchestrator_name, 250) do
          %{polling: %{checking?: false, next_poll_in_ms: next_poll_in_ms}}
          when is_integer(next_poll_in_ms) and next_poll_in_ms > 0 ->
            true

          _other ->
            false
        end
      end,
      40
    )
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
