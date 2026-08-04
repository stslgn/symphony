defmodule SymphonyElixir.OrchestratorStatusTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.RunLedger

  test "snapshot returns :timeout when snapshot server is unresponsive" do
    server_name = Module.concat(__MODULE__, :UnresponsiveSnapshotServer)
    parent = self()

    pid =
      spawn(fn ->
        Process.register(self(), server_name)
        send(parent, :snapshot_server_ready)

        receive do
          :stop -> :ok
        end
      end)

    assert_receive :snapshot_server_ready, 1_000
    assert Orchestrator.snapshot(server_name, 10) == :timeout

    send(pid, :stop)
  end

  test "orchestrator snapshot reflects last codex update and session id" do
    issue_id = "issue-snapshot"

    issue = %Issue{
      id: issue_id,
      identifier: "MT-188",
      title: "Snapshot test",
      description: "Capture codex state",
      state: "In Progress",
      url: "https://example.org/issues/MT-188"
    }

    orchestrator_name = Module.concat(__MODULE__, :SnapshotOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    initial_state = :sys.get_state(pid)
    started_at = DateTime.utc_now()

    running_entry = %{
      run_id: "run-model-snapshot",
      pid: self(),
      ref: make_ref(),
      identifier: issue.identifier,
      issue: issue,
      session_id: nil,
      turn_count: 0,
      last_codex_message: nil,
      last_codex_timestamp: nil,
      last_codex_event: nil,
      started_at: started_at
    }

    seed_running_ledger!(initial_state.run_ledger_path, running_entry)

    state_with_issue =
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.put(initial_state.claimed, issue_id))

    :sys.replace_state(pid, fn _ -> state_with_issue end)

    now = DateTime.utc_now()

    send(
      pid,
      {:codex_worker_update, issue_id,
       %{
         event: :session_started,
         session_id: "thread-live-turn-live",
         resolved_model: "gpt-live",
         reasoning_effort: "high",
         model_catalog_source: "live",
         model_catalog: %{
           source: "live",
           default_model: "gpt-live",
           fetched_at: "2026-08-04T07:00:00Z",
           error: nil,
           models: []
         },
         timestamp: now
       }}
    )

    send(
      pid,
      {:codex_worker_update, issue_id,
       %{
         event: :notification,
         payload: %{
           method: "item/commandExecution/requestApproval",
           params: %{parsedCmd: "SENSITIVE-BL10-DO-NOT-EXPOSE"}
         },
         timestamp: now
       }}
    )

    snapshot = GenServer.call(pid, :snapshot)
    assert %{running: [snapshot_entry]} = snapshot
    assert snapshot_entry.issue_id == issue_id
    assert snapshot_entry.session_id == "thread-live-turn-live"
    assert snapshot_entry.resolved_model == "gpt-live"
    assert snapshot_entry.reasoning_effort == "high"
    assert snapshot_entry.model_catalog_source == "live"
    assert snapshot_entry.model_catalog.source == "live"
    assert snapshot_entry.turn_count == 1
    assert snapshot_entry.last_codex_timestamp == now

    assert snapshot_entry.last_codex_message == %{
             event: :notification,
             message: %{method: "item/commandExecution/requestApproval"},
             timestamp: now
           }

    refute inspect(snapshot_entry) =~ "SENSITIVE-BL10-DO-NOT-EXPOSE"

    assert {:ok, events} = RunLedger.read_events(initial_state.run_ledger_path)

    assert Enum.any?(events, fn event ->
             event["transition"] == "model_resolved" and
               event["run_id"] == "run-model-snapshot" and
               event["resolved_model"] == "gpt-live" and
               event["reasoning_effort"] == "high" and
               event["model_catalog_source"] == "live"
           end)
  end

  test "orchestrator ignores stale worker updates from a previous run id" do
    issue_id = "issue-stale-update"

    issue = %Issue{
      id: issue_id,
      identifier: "MT-STALE",
      title: "Stale update",
      state: "In Progress"
    }

    orchestrator_name = Module.concat(__MODULE__, :StaleUpdateOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    initial_state = :sys.get_state(pid)

    running_entry = %{
      run_id: "run-current",
      pid: self(),
      ref: make_ref(),
      identifier: issue.identifier,
      issue: issue,
      session_id: nil,
      session_title: nil,
      last_codex_message: nil,
      last_codex_timestamp: nil,
      last_codex_event: nil,
      codex_app_server_pid: nil,
      codex_input_tokens: 0,
      codex_output_tokens: 0,
      codex_total_tokens: 0,
      turn_count: 0,
      retry_attempt: 0,
      worker_host: nil,
      workspace_path: nil,
      started_at: DateTime.utc_now()
    }

    :sys.replace_state(pid, fn _ ->
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.put(initial_state.claimed, issue_id))
    end)

    send(
      pid,
      {:worker_runtime_info, issue_id, %{run_id: "run-old", workspace_path: "/tmp/stale-workspace"}}
    )

    send(
      pid,
      {:codex_worker_update, issue_id,
       %{
         run_id: "run-old",
         event: :notification,
         payload: %{method: "stale-event"},
         timestamp: DateTime.utc_now()
       }}
    )

    snapshot = GenServer.call(pid, :snapshot)
    assert [%{run_id: "run-current", workspace_path: nil, last_codex_message: nil}] = snapshot.running
  end

  test "orchestrator snapshot tracks codex thread totals and app-server pid" do
    issue_id = "issue-usage-snapshot"

    issue = %Issue{
      id: issue_id,
      identifier: "MT-201",
      title: "Usage snapshot test",
      description: "Collect usage stats",
      state: "In Progress",
      url: "https://example.org/issues/MT-201"
    }

    orchestrator_name = Module.concat(__MODULE__, :UsageOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    initial_state = :sys.get_state(pid)
    process_ref = make_ref()
    started_at = DateTime.utc_now()

    running_entry = %{
      pid: self(),
      ref: process_ref,
      identifier: issue.identifier,
      issue: issue,
      session_id: nil,
      turn_count: 0,
      last_codex_message: nil,
      last_codex_timestamp: nil,
      last_codex_event: nil,
      codex_input_tokens: 0,
      codex_output_tokens: 0,
      codex_total_tokens: 0,
      codex_last_reported_input_tokens: 0,
      codex_last_reported_output_tokens: 0,
      codex_last_reported_total_tokens: 0,
      started_at: started_at
    }

    :sys.replace_state(pid, fn _ ->
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.put(initial_state.claimed, issue_id))
    end)

    now = DateTime.utc_now()

    send(
      pid,
      {:codex_worker_update, issue_id,
       %{
         event: :session_started,
         session_id: "thread-usage-turn-usage",
         timestamp: now
       }}
    )

    send(
      pid,
      {:codex_worker_update, issue_id,
       %{
         event: :notification,
         payload: %{
           "method" => "thread/tokenUsage/updated",
           "params" => %{
             "tokenUsage" => %{
               "total" => %{"inputTokens" => 12, "outputTokens" => 4, "totalTokens" => 16}
             }
           }
         },
         timestamp: now,
         codex_app_server_pid: "4242"
       }}
    )

    snapshot = GenServer.call(pid, :snapshot)
    assert %{running: [snapshot_entry]} = snapshot
    assert snapshot_entry.codex_app_server_pid == "4242"
    assert snapshot_entry.codex_input_tokens == 12
    assert snapshot_entry.codex_output_tokens == 4
    assert snapshot_entry.codex_total_tokens == 16
    assert snapshot_entry.turn_count == 1
    assert is_integer(snapshot_entry.runtime_seconds)

    send(pid, {:DOWN, process_ref, :process, self(), :normal})
    completed_state = :sys.get_state(pid)

    assert completed_state.codex_totals.input_tokens == 12
    assert completed_state.codex_totals.output_tokens == 4
    assert completed_state.codex_totals.total_tokens == 16
    assert is_integer(completed_state.codex_totals.seconds_running)
  end

  test "orchestrator snapshot tracks turn completed usage when present" do
    issue_id = "issue-turn-completed-usage"

    issue = %Issue{
      id: issue_id,
      identifier: "MT-202",
      title: "Turn completed usage test",
      description: "Track final turn usage",
      state: "In Progress",
      url: "https://example.org/issues/MT-202"
    }

    orchestrator_name = Module.concat(__MODULE__, :TurnCompletedUsageOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    initial_state = :sys.get_state(pid)
    process_ref = make_ref()
    started_at = DateTime.utc_now()

    running_entry = %{
      pid: self(),
      ref: process_ref,
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
      started_at: started_at
    }

    :sys.replace_state(pid, fn _ ->
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.put(initial_state.claimed, issue_id))
    end)

    send(
      pid,
      {:codex_worker_update, issue_id,
       %{
         event: :turn_completed,
         payload: %{
           method: "turn/completed",
           usage: %{"input_tokens" => "12", "output_tokens" => 4, "total_tokens" => 16}
         },
         timestamp: DateTime.utc_now()
       }}
    )

    snapshot = GenServer.call(pid, :snapshot)
    assert %{running: [snapshot_entry]} = snapshot
    assert snapshot_entry.codex_input_tokens == 12
    assert snapshot_entry.codex_output_tokens == 4
    assert snapshot_entry.codex_total_tokens == 16

    send(pid, {:DOWN, process_ref, :process, self(), :normal})
    completed_state = :sys.get_state(pid)
    assert completed_state.codex_totals.input_tokens == 12
    assert completed_state.codex_totals.output_tokens == 4
    assert completed_state.codex_totals.total_tokens == 16
  end

  test "orchestrator snapshot tracks codex token-count cumulative usage payloads" do
    issue_id = "issue-token-count-snapshot"

    issue = %Issue{
      id: issue_id,
      identifier: "MT-220",
      title: "Token count snapshot test",
      description: "Validate token-count style payloads",
      state: "In Progress",
      url: "https://example.org/issues/MT-220"
    }

    orchestrator_name = Module.concat(__MODULE__, :TokenCountOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    initial_state = :sys.get_state(pid)
    process_ref = make_ref()
    started_at = DateTime.utc_now()

    running_entry = %{
      pid: self(),
      ref: process_ref,
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
      started_at: started_at
    }

    :sys.replace_state(pid, fn _ ->
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.put(initial_state.claimed, issue_id))
    end)

    now = DateTime.utc_now()

    send(
      pid,
      {:codex_worker_update, issue_id,
       %{
         event: :notification,
         payload: %{
           "method" => "codex/event/token_count",
           "params" => %{
             "msg" => %{
               "type" => "token_count",
               "info" => %{
                 "total_token_usage" => %{
                   "input_tokens" => "2",
                   "output_tokens" => 2,
                   "total_tokens" => 4
                 }
               }
             }
           }
         },
         timestamp: now
       }}
    )

    send(
      pid,
      {:codex_worker_update, issue_id,
       %{
         event: :notification,
         payload: %{
           "method" => "codex/event/token_count",
           "params" => %{
             "msg" => %{
               "type" => "token_count",
               "info" => %{
                 "total_token_usage" => %{
                   "prompt_tokens" => 10,
                   "completion_tokens" => 5,
                   "total_tokens" => 15
                 }
               }
             }
           }
         },
         timestamp: DateTime.utc_now()
       }}
    )

    snapshot = GenServer.call(pid, :snapshot)
    assert %{running: [snapshot_entry]} = snapshot
    assert snapshot_entry.codex_input_tokens == 10
    assert snapshot_entry.codex_output_tokens == 5
    assert snapshot_entry.codex_total_tokens == 15

    send(pid, {:DOWN, process_ref, :process, self(), :normal})
    completed_state = :sys.get_state(pid)

    assert completed_state.codex_totals.input_tokens == 10
    assert completed_state.codex_totals.output_tokens == 5
    assert completed_state.codex_totals.total_tokens == 15
  end

  test "orchestrator snapshot tracks codex rate-limit payloads" do
    issue_id = "issue-rate-limit-snapshot"

    issue = %Issue{
      id: issue_id,
      identifier: "MT-221",
      title: "Rate limit snapshot test",
      description: "Capture codex rate limit state",
      state: "In Progress",
      url: "https://example.org/issues/MT-221"
    }

    orchestrator_name = Module.concat(__MODULE__, :RateLimitOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    initial_state = :sys.get_state(pid)
    process_ref = make_ref()
    started_at = DateTime.utc_now()

    running_entry = %{
      pid: self(),
      ref: process_ref,
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
      started_at: started_at
    }

    :sys.replace_state(pid, fn _ ->
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.put(initial_state.claimed, issue_id))
    end)

    rate_limits = %{
      "limit_id" => "codex",
      "primary" => %{"remaining" => 90, "limit" => 100},
      "secondary" => nil,
      "credits" => %{"has_credits" => false, "unlimited" => false, "balance" => nil}
    }

    send(
      pid,
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
                 "rate_limits" => rate_limits
               }
             }
           }
         },
         timestamp: DateTime.utc_now()
       }}
    )

    snapshot = GenServer.call(pid, :snapshot)

    assert snapshot.rate_limits == %{
             limit_id: "codex",
             primary: %{remaining: 90, limit: 100},
             credits: %{has_credits: false, unlimited: false}
           }
  end

  test "orchestrator token accounting prefers total_token_usage over last_token_usage in token_count payloads" do
    issue_id = "issue-token-precedence"

    issue = %Issue{
      id: issue_id,
      identifier: "MT-222",
      title: "Token precedence",
      description: "Prefer per-event deltas",
      state: "In Progress",
      url: "https://example.org/issues/MT-222"
    }

    orchestrator_name = Module.concat(__MODULE__, :TokenPrecedenceOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    initial_state = :sys.get_state(pid)
    process_ref = make_ref()
    started_at = DateTime.utc_now()

    running_entry = %{
      pid: self(),
      ref: process_ref,
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
      started_at: started_at
    }

    :sys.replace_state(pid, fn _ ->
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.put(initial_state.claimed, issue_id))
    end)

    send(
      pid,
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
                 "info" => %{
                   "last_token_usage" => %{
                     "input_tokens" => 2,
                     "output_tokens" => 1,
                     "total_tokens" => 3
                   },
                   "total_token_usage" => %{
                     "input_tokens" => 200,
                     "output_tokens" => 100,
                     "total_tokens" => 300
                   }
                 }
               }
             }
           }
         },
         timestamp: DateTime.utc_now()
       }}
    )

    snapshot = GenServer.call(pid, :snapshot)
    assert %{running: [snapshot_entry]} = snapshot
    assert snapshot_entry.codex_input_tokens == 200
    assert snapshot_entry.codex_output_tokens == 100
    assert snapshot_entry.codex_total_tokens == 300
  end

  test "orchestrator token accounting accumulates monotonic thread token usage totals" do
    issue_id = "issue-thread-token-usage"

    issue = %Issue{
      id: issue_id,
      identifier: "MT-223",
      title: "Thread token usage",
      description: "Accumulate absolute thread totals",
      state: "In Progress",
      url: "https://example.org/issues/MT-223"
    }

    orchestrator_name = Module.concat(__MODULE__, :ThreadTokenUsageOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    initial_state = :sys.get_state(pid)
    process_ref = make_ref()
    started_at = DateTime.utc_now()

    running_entry = %{
      pid: self(),
      ref: process_ref,
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
      started_at: started_at
    }

    :sys.replace_state(pid, fn _ ->
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.put(initial_state.claimed, issue_id))
    end)

    for usage <- [
          %{"input_tokens" => 8, "output_tokens" => 3, "total_tokens" => 11},
          %{"input_tokens" => 10, "output_tokens" => 4, "total_tokens" => 14}
        ] do
      send(
        pid,
        {:codex_worker_update, issue_id,
         %{
           event: :notification,
           payload: %{
             "method" => "thread/tokenUsage/updated",
             "params" => %{"tokenUsage" => %{"total" => usage}}
           },
           timestamp: DateTime.utc_now()
         }}
      )
    end

    snapshot = GenServer.call(pid, :snapshot)
    assert %{running: [snapshot_entry]} = snapshot
    assert snapshot_entry.codex_input_tokens == 10
    assert snapshot_entry.codex_output_tokens == 4
    assert snapshot_entry.codex_total_tokens == 14
  end

  test "canonical token accounting derives a monotonic total and rejects malformed counters" do
    {state, issue_id, run_id} = token_accounting_state("canonical")

    state = apply_token_usage(state, issue_id, run_id, %{"input_tokens" => 8, "output_tokens" => 3})
    assert_token_usage(state, issue_id, 8, 3, 11, true)

    duplicate = apply_token_usage(state, issue_id, run_id, %{"input_tokens" => 8, "output_tokens" => 3})
    assert_token_usage(duplicate, issue_id, 8, 3, 11, true)
    assert duplicate.codex_totals.total_tokens == 11

    increased =
      apply_token_usage(duplicate, issue_id, run_id, %{
        "input_tokens" => 10,
        "output_tokens" => 5
      })

    assert_token_usage(increased, issue_id, 10, 5, 15, true)

    reset = apply_token_usage(increased, issue_id, run_id, %{"input_tokens" => 2, "output_tokens" => 1})
    assert_token_usage(reset, issue_id, 10, 5, 15, true)

    mismatched =
      apply_token_usage(reset, issue_id, run_id, %{
        "input_tokens" => 20,
        "output_tokens" => 5,
        "total_tokens" => 12
      })

    assert_token_usage(mismatched, issue_id, 20, 5, 25, true)

    malformed =
      apply_token_usage(mismatched, issue_id, run_id, %{
        "input_tokens" => 30,
        "output_tokens" => 10,
        "total_tokens" => "40-trailing"
      })

    assert_token_usage(malformed, issue_id, 20, 5, 25, true)

    overflow =
      apply_token_usage(malformed, issue_id, run_id, %{
        "input_tokens" => 9_223_372_036_854_775_807,
        "output_tokens" => 1
      })

    assert_token_usage(overflow, issue_id, 20, 5, 25, true)
    assert overflow.codex_totals == mismatched.codex_totals
  end

  test "one-sided token telemetry stays unenforceable and attempts keep independent high-water marks" do
    {first_state, issue_id, first_run_id} = token_accounting_state("attempt-isolation")

    one_sided =
      apply_token_usage(first_state, issue_id, first_run_id, %{"input_tokens" => 40})

    assert_token_usage(one_sided, issue_id, 40, 0, 0, false)

    first_attempt =
      apply_token_usage(one_sided, issue_id, first_run_id, %{
        "input_tokens" => 40,
        "output_tokens" => 10
      })

    assert_token_usage(first_attempt, issue_id, 40, 10, 50, true)

    second_run_id = "run-attempt-isolation-2"
    second_entry = token_running_entry(issue_id, second_run_id, max_tokens: nil)

    second_state = %{
      first_attempt
      | running: %{issue_id => second_entry},
        claimed: MapSet.new([issue_id])
    }

    stale =
      apply_token_usage(second_state, issue_id, first_run_id, %{
        "input_tokens" => 90,
        "output_tokens" => 10
      })

    assert_token_usage(stale, issue_id, 0, 0, 0, false)

    current =
      apply_token_usage(stale, issue_id, second_run_id, %{
        "input_tokens" => 4,
        "output_tokens" => 6
      })

    assert_token_usage(current, issue_id, 4, 6, 10, true)
    assert current.codex_totals.total_tokens == 60
  end

  test "derived canonical totals enforce exact and coarse token-budget crossings" do
    for {tag, usage} <- [
          {"exact", %{"input_tokens" => 80, "output_tokens" => 20}},
          {"overshoot", %{"input_tokens" => 130, "output_tokens" => 70}}
        ] do
      {state, issue_id, run_id} = token_accounting_state(tag, max_tokens: 100)
      parked = apply_token_usage(state, issue_id, run_id, usage)

      refute Map.has_key?(parked.running, issue_id)

      assert %{
               reason: "run_budget_exhausted",
               terminal_reason: "token_budget_exhausted"
             } = parked.parked[issue_id]
    end
  end

  test "orchestrator token accounting ignores last_token_usage without cumulative totals" do
    issue_id = "issue-last-token-ignored"

    issue = %Issue{
      id: issue_id,
      identifier: "MT-224",
      title: "Last token ignored",
      description: "Ignore delta-only token reports",
      state: "In Progress",
      url: "https://example.org/issues/MT-224"
    }

    orchestrator_name = Module.concat(__MODULE__, :LastTokenIgnoredOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    initial_state = :sys.get_state(pid)
    process_ref = make_ref()
    started_at = DateTime.utc_now()

    running_entry = %{
      pid: self(),
      ref: process_ref,
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
      started_at: started_at
    }

    :sys.replace_state(pid, fn _ ->
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.put(initial_state.claimed, issue_id))
    end)

    send(
      pid,
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
                 "info" => %{
                   "last_token_usage" => %{
                     "input_tokens" => 8,
                     "output_tokens" => 3,
                     "total_tokens" => 11
                   }
                 }
               }
             }
           }
         },
         timestamp: DateTime.utc_now()
       }}
    )

    snapshot = GenServer.call(pid, :snapshot)
    assert %{running: [snapshot_entry]} = snapshot
    assert snapshot_entry.codex_input_tokens == 0
    assert snapshot_entry.codex_output_tokens == 0
    assert snapshot_entry.codex_total_tokens == 0
  end

  test "orchestrator snapshot includes retry backoff entries" do
    orchestrator_name = Module.concat(__MODULE__, :RetryOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    retry_entry = %{
      attempt: 2,
      timer_ref: nil,
      due_at_ms: System.monotonic_time(:millisecond) + 5_000,
      identifier: "MT-500",
      error: "agent_exit"
    }

    initial_state = :sys.get_state(pid)
    new_state = %{initial_state | retry_attempts: %{"mt-500" => retry_entry}}
    :sys.replace_state(pid, fn _ -> new_state end)

    snapshot = GenServer.call(pid, :snapshot)
    assert is_list(snapshot.retrying)

    assert [
             %{
               issue_id: "mt-500",
               attempt: 2,
               due_in_ms: due_in_ms,
               identifier: "MT-500",
               error: "agent_exit"
             }
           ] = snapshot.retrying

    assert due_in_ms > 0
  end

  test "orchestrator restart exposes a durable queued resume in status" do
    write_workflow_file!(Workflow.workflow_file_path(),
      worker_ssh_hosts: ["worker-a", "worker-b"]
    )

    root =
      Path.join(
        System.tmp_dir!(),
        "symphony-queued-resume-status-#{System.unique_integer([:positive])}"
      )

    ledger_path = Path.join(root, "run-ledger.jsonl")
    on_exit(fn -> File.rm_rf(root) end)

    for transition_stage <- [{"run_claimed", "claimed"}, {"run_started", "running"}] do
      {transition, stage} = transition_stage

      assert :ok =
               RunLedger.append(ledger_path, %{
                 transition: transition,
                 stage: stage,
                 run_id: "run-status-resume-source",
                 issue_id: "issue-status-resume",
                 issue_identifier: "MT-RESUME-STATUS",
                 attempt: 3,
                 worker_host: "worker-a",
                 workspace_path: "/srv/symphony/MT-RESUME-STATUS"
               })
    end

    assert :ok =
             RunLedger.append(ledger_path, %{
               transition: "run_parked",
               stage: "parked",
               run_id: "run-status-resume-source",
               issue_id: "issue-status-resume",
               issue_identifier: "MT-RESUME-STATUS",
               attempt: 3,
               wait_id: "wait-status-resume",
               parked_reason: "waiting_infrastructure",
               allowed_actions: ["retry", "reject"],
               worker_host: "worker-a",
               workspace_path: "/srv/symphony/MT-RESUME-STATUS"
             })

    assert :ok =
             RunLedger.append(ledger_path, %{
               transition: "resume_queued",
               stage: "resume_queued",
               run_id: "run-status-resume-source",
               issue_id: "issue-status-resume",
               issue_identifier: "MT-RESUME-STATUS",
               attempt: 4,
               wait_id: "wait-status-resume",
               parked_reason: "waiting_infrastructure",
               allowed_actions: ["retry", "reject"],
               worker_host: "worker-a",
               workspace_path: "/srv/symphony/MT-RESUME-STATUS"
             })

    assert :ok =
             RunLedger.append(ledger_path, %{
               transition: "dispatch_paused",
               stage: "operator",
               runner_generation: "runner-before-restart"
             })

    orchestrator_name = Module.concat(__MODULE__, :QueuedResumeRestartOrchestrator)

    assert {:ok, pid} =
             Orchestrator.start_link(name: orchestrator_name, run_ledger_path: ledger_path)

    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :normal) end)

    snapshot = GenServer.call(pid, :snapshot)

    assert snapshot.control.dispatch_paused
    assert snapshot.parked == []

    assert [
             %{
               issue_id: "issue-status-resume",
               identifier: "MT-RESUME-STATUS",
               run_id: "run-status-resume-source",
               wait_id: "wait-status-resume",
               attempt: 4,
               stage: "resume_queued",
               worker_host: "worker-a",
               workspace_path: "/srv/symphony/MT-RESUME-STATUS",
               due_in_ms: 0
             }
           ] = snapshot.retrying

    rendered =
      StatusDashboard.format_snapshot_content_for_test(
        {:ok,
         %{
           running: snapshot.running,
           retrying: snapshot.retrying,
           codex_totals: snapshot.codex_totals,
           control: snapshot.control
         }},
        0.0
      )

    assert rendered =~ "MT-RESUME-STATUS"
    assert rendered =~ "attempt=4"
    assert rendered =~ "resume queued"
  end

  test "orchestrator restart keeps stale remote recovery affinity visible and durable" do
    write_workflow_file!(Workflow.workflow_file_path(),
      worker_ssh_hosts: ["worker-a", "worker-b"]
    )

    root =
      Path.join(
        System.tmp_dir!(),
        "symphony-recovery-affinity-status-#{System.unique_integer([:positive])}"
      )

    ledger_path = Path.join(root, "run-ledger.jsonl")
    on_exit(fn -> File.rm_rf(root) end)

    assert :ok =
             RunLedger.append(ledger_path, %{
               transition: "run_claimed",
               stage: "claimed",
               run_id: "run-stale-affinity",
               issue_id: "issue-stale-affinity",
               issue_identifier: "MT-STALE-AFFINITY",
               attempt: 2,
               worker_host: "worker-a",
               workspace_path: "/srv/symphony/MT-STALE-AFFINITY"
             })

    assert :ok =
             RunLedger.append(ledger_path, %{
               transition: "run_started",
               stage: "running",
               run_id: "run-stale-affinity",
               issue_id: "issue-stale-affinity",
               issue_identifier: "MT-STALE-AFFINITY",
               attempt: 2,
               worker_host: "worker-a",
               workspace_path: "/srv/symphony/MT-STALE-AFFINITY"
             })

    assert :ok =
             RunLedger.append(ledger_path, %{
               transition: "run_runtime_ready",
               stage: "running",
               run_id: "run-stale-affinity",
               issue_id: "issue-stale-affinity",
               issue_identifier: "MT-STALE-AFFINITY",
               attempt: 2,
               worker_host: "worker-a",
               workspace_path: "/srv/symphony/MT-STALE-AFFINITY"
             })

    assert :ok =
             RunLedger.append(ledger_path, %{
               transition: "dispatch_paused",
               stage: "operator",
               runner_generation: "runner-stale-affinity"
             })

    orchestrator_name = Module.concat(__MODULE__, :RecoveryAffinityOrchestrator)

    assert {:ok, pid} =
             Orchestrator.start_link(name: orchestrator_name, run_ledger_path: ledger_path)

    snapshot = GenServer.call(pid, :snapshot)

    assert [
             %{
               issue_id: "issue-stale-affinity",
               identifier: "MT-STALE-AFFINITY",
               run_id: "run-stale-affinity",
               attempt: 3,
               stage: "recovery_queued",
               worker_host: "worker-a",
               workspace_path: "/srv/symphony/MT-STALE-AFFINITY",
               error: nil
             }
           ] = snapshot.retrying

    GenServer.stop(pid)

    second_name = Module.concat(__MODULE__, :SecondRecoveryAffinityOrchestrator)

    assert {:ok, second_pid} =
             Orchestrator.start_link(name: second_name, run_ledger_path: ledger_path)

    on_exit(fn -> if Process.alive?(second_pid), do: Process.exit(second_pid, :normal) end)

    assert [%{worker_host: "worker-a", workspace_path: "/srv/symphony/MT-STALE-AFFINITY"}] =
             GenServer.call(second_pid, :snapshot).retrying
  end

  test "orchestrator snapshot includes poll countdown and checking status" do
    orchestrator_name = Module.concat(__MODULE__, :PollingSnapshotOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    now_ms = System.monotonic_time(:millisecond)

    :sys.replace_state(pid, fn state ->
      %{
        state
        | poll_interval_ms: 30_000,
          tick_timer_ref: nil,
          tick_token: make_ref(),
          next_poll_due_at_ms: now_ms + 4_000,
          poll_check_in_progress: false
      }
    end)

    snapshot = GenServer.call(pid, :snapshot)

    assert %{
             polling: %{
               checking?: false,
               poll_interval_ms: 30_000,
               next_poll_in_ms: due_in_ms
             }
           } = snapshot

    assert is_integer(due_in_ms)
    assert due_in_ms >= 0
    assert due_in_ms <= 4_000

    :sys.replace_state(pid, fn state ->
      %{state | poll_check_in_progress: true, next_poll_due_at_ms: nil}
    end)

    snapshot = GenServer.call(pid, :snapshot)
    assert %{polling: %{checking?: true, next_poll_in_ms: nil}} = snapshot
  end

  test "orchestrator triggers an immediate poll cycle shortly after startup" do
    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_api_token: nil,
      poll_interval_ms: 5_000
    )

    orchestrator_name = Module.concat(__MODULE__, :ImmediateStartupOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    assert %{polling: %{checking?: true}} =
             wait_for_snapshot(
               pid,
               fn
                 %{polling: %{checking?: true}} ->
                   true

                 _ ->
                   false
               end,
               500
             )

    assert %{
             polling: %{
               checking?: false,
               next_poll_in_ms: next_poll_in_ms,
               poll_interval_ms: 5_000
             }
           } =
             wait_for_snapshot(
               pid,
               fn
                 %{polling: %{checking?: false, next_poll_in_ms: due_in_ms}}
                 when is_integer(due_in_ms) and due_in_ms <= 5_000 ->
                   true

                 _ ->
                   false
               end,
               500
             )

    assert is_integer(next_poll_in_ms)
    assert next_poll_in_ms >= 0
  end

  test "orchestrator poll cycle resets next refresh countdown after a check" do
    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_api_token: nil,
      poll_interval_ms: 50
    )

    orchestrator_name = Module.concat(__MODULE__, :PollCycleOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    :sys.replace_state(pid, fn state ->
      %{
        state
        | poll_interval_ms: 50,
          poll_check_in_progress: true,
          next_poll_due_at_ms: nil
      }
    end)

    send(pid, :run_poll_cycle)

    snapshot =
      wait_for_snapshot(pid, fn
        %{polling: %{checking?: false, poll_interval_ms: 50, next_poll_in_ms: next_poll_in_ms}}
        when is_integer(next_poll_in_ms) and next_poll_in_ms <= 50 ->
          true

        _ ->
          false
      end)

    assert %{
             polling: %{
               checking?: false,
               poll_interval_ms: 50,
               next_poll_in_ms: next_poll_in_ms
             }
           } = snapshot

    assert is_integer(next_poll_in_ms)
    assert next_poll_in_ms >= 0
    assert next_poll_in_ms <= 50
  end

  test "orchestrator restarts stalled workers with retry backoff" do
    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_api_token: nil,
      codex_stall_timeout_ms: 1_000
    )

    issue_id = "issue-stall"
    orchestrator_name = Module.concat(__MODULE__, :StallOrchestrator)
    {:ok, pid} = Orchestrator.start_link(name: orchestrator_name)

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    worker_pid =
      spawn(fn ->
        receive do
          :done -> :ok
        end
      end)

    stale_activity_at = DateTime.add(DateTime.utc_now(), -5, :second)
    initial_state = :sys.get_state(pid)

    running_entry = %{
      pid: worker_pid,
      ref: make_ref(),
      run_id: "run-stall",
      retry_attempt: 0,
      identifier: "MT-STALL",
      issue: %Issue{id: issue_id, identifier: "MT-STALL", state: "In Progress"},
      session_id: "thread-stall-turn-stall",
      last_codex_message: nil,
      last_codex_timestamp: stale_activity_at,
      last_codex_event: :notification,
      started_at: stale_activity_at
    }

    :sys.replace_state(pid, fn _ ->
      initial_state
      |> Map.put(:running, %{issue_id => running_entry})
      |> Map.put(:claimed, MapSet.put(initial_state.claimed, issue_id))
    end)

    send(pid, :tick)
    Process.sleep(100)
    state = :sys.get_state(pid)

    refute Process.alive?(worker_pid)
    refute Map.has_key?(state.running, issue_id)

    assert %{
             attempt: 1,
             due_at_ms: due_at_ms,
             identifier: "MT-STALL",
             error: "worker_stalled"
           } = state.retry_attempts[issue_id]

    assert is_integer(due_at_ms)
    remaining_ms = due_at_ms - System.monotonic_time(:millisecond)
    assert remaining_ms >= 9_500
    assert remaining_ms <= 10_500
  end

  test "status dashboard renders offline marker to terminal" do
    rendered =
      ExUnit.CaptureIO.capture_io(fn ->
        assert :ok = StatusDashboard.render_offline_status()
      end)

    assert rendered =~ "app_status=offline"
    refute rendered =~ "Timestamp:"
  end

  test "status dashboard renders linear project link in header" do
    snapshot_data =
      {:ok,
       %{
         running: [],
         retrying: [],
         codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
         rate_limits: nil
       }}

    rendered = StatusDashboard.format_snapshot_content_for_test(snapshot_data, 0.0)

    assert rendered =~ "https://linear.app/project/project/issues"
    assert rendered =~ "Dispatch:"
    assert rendered =~ "running"
    refute rendered =~ "Dashboard:"
  end

  test "status dashboard renders global dispatch pause" do
    snapshot_data =
      {:ok,
       %{
         running: [],
         retrying: [],
         codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
         rate_limits: nil,
         control: %{dispatch_paused: true}
       }}

    rendered = StatusDashboard.format_snapshot_content_for_test(snapshot_data, 0.0)
    assert rendered =~ "Dispatch:"
    assert rendered =~ "paused"
  end

  test "status dashboard renders dashboard url on its own line when server port is configured" do
    previous_port_override = Application.get_env(:symphony_elixir, :server_port_override)

    on_exit(fn ->
      if is_nil(previous_port_override) do
        Application.delete_env(:symphony_elixir, :server_port_override)
      else
        Application.put_env(:symphony_elixir, :server_port_override, previous_port_override)
      end
    end)

    Application.put_env(:symphony_elixir, :server_port_override, 4000)

    snapshot_data =
      {:ok,
       %{
         running: [],
         retrying: [],
         codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
         rate_limits: nil
       }}

    rendered = StatusDashboard.format_snapshot_content_for_test(snapshot_data, 0.0)

    assert rendered =~ "│ Project:"
    assert rendered =~ "https://linear.app/project/project/issues"
    assert rendered =~ "│ Dashboard:"
    assert rendered =~ "http://127.0.0.1:4000/"
  end

  test "status dashboard prefers the bound server port and normalizes wildcard hosts" do
    assert StatusDashboard.dashboard_url_for_test("0.0.0.0", 0, 43_123) ==
             "http://127.0.0.1:43123/"

    assert StatusDashboard.dashboard_url_for_test("::1", 4000, nil) ==
             "http://[::1]:4000/"
  end

  test "status dashboard renders next refresh countdown and checking marker" do
    waiting_snapshot =
      {:ok,
       %{
         running: [],
         retrying: [],
         codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
         rate_limits: nil,
         polling: %{checking?: false, next_poll_in_ms: 2_000, poll_interval_ms: 30_000}
       }}

    waiting_rendered = StatusDashboard.format_snapshot_content_for_test(waiting_snapshot, 0.0)
    assert waiting_rendered =~ "Next refresh:"
    assert waiting_rendered =~ "2s"

    checking_snapshot =
      {:ok,
       %{
         running: [],
         retrying: [],
         codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
         rate_limits: nil,
         polling: %{checking?: true, next_poll_in_ms: nil, poll_interval_ms: 30_000}
       }}

    checking_rendered = StatusDashboard.format_snapshot_content_for_test(checking_snapshot, 0.0)
    assert checking_rendered =~ "checking now…"
  end

  test "status dashboard adds a spacer line before backoff queue when no agents are active" do
    snapshot_data =
      {:ok,
       %{
         running: [],
         retrying: [],
         codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
         rate_limits: nil
       }}

    rendered = StatusDashboard.format_snapshot_content_for_test(snapshot_data, 0.0)
    plain = Regex.replace(~r/\e\[[0-9;]*m/, rendered, "")

    assert plain =~ ~r/No active agents\r?\n│\s*\r?\n├─ Retry \/ resume queue/
  end

  test "status dashboard adds a spacer line before backoff queue when agents are active" do
    snapshot_data =
      {:ok,
       %{
         running: [
           %{
             identifier: "MT-777",
             state: "running",
             session_id: "thread-1234567890",
             codex_app_server_pid: "4242",
             codex_total_tokens: 3_200,
             runtime_seconds: 75,
             turn_count: 7,
             last_codex_event: "turn_completed",
             last_codex_message: %{
               event: :notification,
               message: %{
                 "method" => "turn/completed",
                 "params" => %{"turn" => %{"status" => "completed"}}
               }
             }
           }
         ],
         retrying: [],
         codex_totals: %{
           input_tokens: 90,
           output_tokens: 12,
           total_tokens: 102,
           seconds_running: 75
         },
         rate_limits: nil
       }}

    rendered = StatusDashboard.format_snapshot_content_for_test(snapshot_data, 0.0)
    plain = Regex.replace(~r/\e\[[0-9;]*m/, rendered, "")

    assert plain =~ ~r/MT-777.*\r?\n│\s*\r?\n├─ Retry \/ resume queue/s
  end

  test "status dashboard renders an unstyled closing corner when the retry queue is empty" do
    snapshot_data =
      {:ok,
       %{
         running: [],
         retrying: [],
         codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
         rate_limits: nil
       }}

    rendered = StatusDashboard.format_snapshot_content_for_test(snapshot_data, 0.0)

    assert rendered |> String.split("\n") |> List.last() == "╰─"
  end

  test "status dashboard coalesces rapid updates to one render per interval" do
    dashboard_name = Module.concat(__MODULE__, :RenderDashboard)
    parent = self()
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

    {:ok, pid} =
      StatusDashboard.start_link(
        name: dashboard_name,
        enabled: true,
        refresh_ms: 60_000,
        render_interval_ms: 16,
        render_fun: fn content ->
          send(parent, {:render, System.monotonic_time(:millisecond), content})
        end
      )

    on_exit(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
    end)

    StatusDashboard.notify_update(dashboard_name)
    assert_receive {:render, first_render_ms, _content}, 200

    :sys.replace_state(pid, fn state ->
      %{state | last_snapshot_fingerprint: :force_next_change, last_rendered_content: nil}
    end)

    StatusDashboard.notify_update(dashboard_name)
    StatusDashboard.notify_update(dashboard_name)

    assert_receive {:render, second_render_ms, _content}, 200
    assert second_render_ms > first_render_ms
    refute_receive {:render, _third_render_ms, _content}, 60
  end

  test "status dashboard computes rolling 5-second token throughput" do
    assert StatusDashboard.rolling_tps([], 10_000, 0) == 0.0

    assert StatusDashboard.rolling_tps([{9_000, 20}], 10_000, 40) == 20.0

    # sample older than 5s is dropped from the window
    assert StatusDashboard.rolling_tps([{4_900, 10}], 10_000, 90) == 0.0

    tps =
      StatusDashboard.rolling_tps(
        [{9_500, 10}, {9_000, 40}, {8_000, 80}],
        10_000,
        95
      )

    assert tps == 7.5
  end

  test "status dashboard throttles tps updates to once per second" do
    {first_second, first_tps} =
      StatusDashboard.throttled_tps(nil, nil, 10_000, [{9_000, 20}], 40)

    {same_second, same_tps} =
      StatusDashboard.throttled_tps(first_second, first_tps, 10_500, [{9_000, 20}], 200)

    assert same_second == first_second
    assert same_tps == first_tps

    {next_second, next_tps} =
      StatusDashboard.throttled_tps(same_second, same_tps, 11_000, [{10_500, 200}], 260)

    assert next_second == 11
    refute next_tps == same_tps
  end

  test "status dashboard formats timestamps at second precision" do
    dt = ~U[2026-02-15 21:36:38.987654Z]
    assert StatusDashboard.format_timestamp_for_test(dt) == "2026-02-15 21:36:38Z"
  end

  test "status dashboard renders 10-minute TPS graph snapshot for steady throughput" do
    now_ms = 600_000
    current_tokens = 6_000

    samples =
      for timestamp <- 575_000..0//-25_000 do
        {timestamp, div(timestamp, 100)}
      end

    assert StatusDashboard.tps_graph_for_test(samples, now_ms, current_tokens) ==
             "████████████████████████"
  end

  test "status dashboard renders 10-minute TPS graph snapshot for ramping throughput" do
    now_ms = 600_000

    rates_per_bucket =
      1..24
      |> Enum.map(&(&1 * 2))

    {current_tokens, samples} = graph_samples_from_rates(rates_per_bucket)

    assert StatusDashboard.tps_graph_for_test(samples, now_ms, current_tokens) ==
             "▁▂▂▂▃▃▃▃▄▄▄▅▅▅▆▆▆▆▇▇▇██▅"
  end

  test "status dashboard keeps historical TPS bars stable within the active bucket" do
    now_ms = 600_000
    current_tokens = 74_400
    next_current_tokens = current_tokens + 120
    samples = graph_samples_for_stability_test(now_ms)

    graph_at_now = StatusDashboard.tps_graph_for_test(samples, now_ms, current_tokens)

    graph_next_second =
      StatusDashboard.tps_graph_for_test(samples, now_ms + 1_000, next_current_tokens)

    historical_changes =
      graph_at_now
      |> String.graphemes()
      |> Enum.zip(String.graphemes(graph_next_second))
      |> Enum.take(23)
      |> Enum.count(fn {left, right} -> left != right end)

    assert historical_changes == 0
  end

  test "application configures a rotating file logger handler" do
    assert {:ok, handler_config} = :logger.get_handler_config(:symphony_disk_log)
    assert handler_config.module == :logger_disk_log_h

    disk_config = handler_config.config
    assert disk_config.type == :wrap
    assert is_list(disk_config.file)
    assert disk_config.max_no_bytes > 0
    assert disk_config.max_no_files > 0
  end

  test "status dashboard renders last codex message in EVENT column" do
    row =
      StatusDashboard.format_running_summary_for_test(%{
        identifier: "MT-233",
        state: "running",
        session_id: "thread-1234567890",
        codex_app_server_pid: "4242",
        codex_total_tokens: 12,
        runtime_seconds: 15,
        last_codex_event: :notification,
        last_codex_message: %{
          event: :notification,
          message: %{
            "method" => "turn/completed",
            "params" => %{"turn" => %{"status" => "completed"}}
          }
        }
      })

    plain = Regex.replace(~r/\e\[[\\d;]*m/, row, "")

    assert plain =~ "turn completed (completed)"
    assert (String.split(plain, "turn completed (completed)") |> length()) - 1 == 1
    refute plain =~ " notification "
  end

  test "status dashboard fails closed for raw last codex message content" do
    payload =
      "cmd: " <>
        <<27>> <>
        "[31mRED" <>
        <<27>> <>
        "[0m" <>
        <<0>> <>
        " after\nline"

    row =
      StatusDashboard.format_running_summary_for_test(%{
        identifier: "MT-898",
        state: "running",
        session_id: "thread-1234567890",
        codex_app_server_pid: "4242",
        codex_total_tokens: 12,
        runtime_seconds: 15,
        last_codex_event: :notification,
        last_codex_message: payload
      })

    plain = Regex.replace(~r/\e\[[0-9;]*m/, row, "")

    assert plain =~ "codex event"
    refute plain =~ "RED"
    refute plain =~ <<27>>
    refute plain =~ <<0>>
  end

  test "status dashboard expands running row to requested terminal width" do
    terminal_columns = 140

    row =
      StatusDashboard.format_running_summary_for_test(
        %{
          identifier: "MT-598",
          state: "running",
          session_id: "thread-1234567890",
          codex_app_server_pid: "4242",
          codex_total_tokens: 123,
          runtime_seconds: 15,
          last_codex_event: :notification,
          last_codex_message: %{
            event: :notification,
            message: %{
              "method" => "turn/completed",
              "params" => %{"turn" => %{"status" => "completed"}}
            }
          }
        },
        terminal_columns
      )

    plain = Regex.replace(~r/\e\[[\d;]*m/, row, "")

    assert String.length(plain) == terminal_columns
    assert plain =~ "turn completed (completed)"
  end

  test "status dashboard humanizes full codex app-server event set" do
    event_cases = [
      {"turn/started", %{"params" => %{"turn" => %{"id" => "turn-1"}}}, "turn started"},
      {"turn/completed", %{"params" => %{"turn" => %{"status" => "completed"}}}, "turn completed"},
      {"turn/diff/updated", %{"params" => %{"diff" => "line1\nline2"}}, "turn diff updated"},
      {"turn/plan/updated", %{"params" => %{"plan" => [%{"step" => "a"}, %{"step" => "b"}]}}, "plan updated"},
      {"thread/tokenUsage/updated",
       %{
         "params" => %{
           "usage" => %{"input_tokens" => 8, "output_tokens" => 3, "total_tokens" => 11}
         }
       }, "thread token usage updated"},
      {"item/started",
       %{
         "params" => %{
           "item" => %{
             "id" => "item-1234567890abcdef",
             "type" => "commandExecution",
             "status" => "running"
           }
         }
       }, "item started: command execution"},
      {"item/completed", %{"params" => %{"item" => %{"type" => "fileChange", "status" => "completed"}}}, "item completed: file change"},
      {"item/agentMessage/delta", %{"params" => %{"delta" => "hello"}}, "agent message streaming"},
      {"item/plan/delta", %{"params" => %{"delta" => "step"}}, "plan streaming"},
      {"item/reasoning/summaryTextDelta", %{"params" => %{"summaryText" => "thinking"}}, "reasoning summary streaming"},
      {"item/reasoning/summaryPartAdded", %{"params" => %{"summaryText" => "section"}}, "reasoning summary section added"},
      {"item/reasoning/textDelta", %{"params" => %{"textDelta" => "reason"}}, "reasoning text streaming"},
      {"item/commandExecution/outputDelta", %{"params" => %{"outputDelta" => "ok"}}, "command output streaming"},
      {"item/fileChange/outputDelta", %{"params" => %{"outputDelta" => "changed"}}, "file change output streaming"},
      {"item/commandExecution/requestApproval", %{"params" => %{"parsedCmd" => "git status"}}, "command approval requested"},
      {"item/fileChange/requestApproval", %{"params" => %{"fileChangeCount" => 2}}, "file change approval requested (2 files)"},
      {"item/tool/call", %{"params" => %{"tool" => "linear_graphql"}}, "dynamic tool call requested (linear_graphql)"},
      {"item/tool/requestUserInput", %{"params" => %{"question" => "Continue?"}}, "tool requires user input"}
    ]

    Enum.each(event_cases, fn {method, payload, expected_fragment} ->
      message = Map.put(payload, "method", method)

      humanized =
        StatusDashboard.humanize_codex_message(%{event: :notification, message: message})

      assert humanized =~ expected_fragment
    end)
  end

  test "status dashboard humanizes dynamic tool wrapper events" do
    completed = %{
      event: :tool_call_completed,
      message: %{
        payload: %{"method" => "item/tool/call", "params" => %{"name" => "linear_graphql"}}
      }
    }

    failed = %{
      event: :tool_call_failed,
      message: %{
        payload: %{"method" => "item/tool/call", "params" => %{"tool" => "linear_graphql"}}
      }
    }

    unsupported = %{
      event: :unsupported_tool_call,
      message: %{
        payload: %{"method" => "item/tool/call", "params" => %{"tool" => "unknown_tool"}}
      }
    }

    denied = %{
      event: :capability_denied,
      message: %{
        payload: %{"method" => "item/tool/call", "params" => %{"tool" => "linear_graphql"}}
      }
    }

    assert StatusDashboard.humanize_codex_message(completed) =~
             "dynamic tool call completed (linear_graphql)"

    assert StatusDashboard.humanize_codex_message(failed) =~
             "dynamic tool call failed (linear_graphql)"

    assert StatusDashboard.humanize_codex_message(unsupported) =~
             "unsupported dynamic tool call rejected (unknown_tool)"

    assert StatusDashboard.humanize_codex_message(denied) =~
             "capability denied by Symphony policy (linear_graphql)"
  end

  test "status dashboard unwraps nested codex payload envelopes" do
    wrapped = %{
      event: :notification,
      message: %{
        payload: %{
          "method" => "turn/completed",
          "params" => %{
            "turn" => %{"status" => "completed"},
            "usage" => %{"input_tokens" => "10", "output_tokens" => 2, "total_tokens" => 12}
          }
        },
        raw: "{\"method\":\"turn/completed\"}"
      }
    }

    assert StatusDashboard.humanize_codex_message(wrapped) =~ "turn completed"
    assert StatusDashboard.humanize_codex_message(wrapped) =~ "in 10"
  end

  test "status dashboard humanizes terminal protocol errors" do
    message = %{
      event: :terminal_protocol_error,
      message: %{
        reason: {:terminal_protocol_error, :invalid_markup}
      }
    }

    humanized = StatusDashboard.humanize_codex_message(message)
    assert humanized =~ "terminal codex protocol error"
    assert humanized =~ "invalid_markup"
  end

  test "status dashboard humanizes app-server runtime errors" do
    message = %{
      event: :app_server_error,
      message: %{
        reason: {:app_server_error, "turn_error"}
      }
    }

    humanized = StatusDashboard.humanize_codex_message(message)
    assert humanized =~ "codex app-server error"
    assert humanized =~ "turn_error"
  end

  test "status dashboard omits shell command text from exec status" do
    message = %{
      event: :notification,
      message: %{
        "method" => "codex/event/exec_command_begin",
        "params" => %{"msg" => %{"command" => "git status --short"}}
      }
    }

    assert StatusDashboard.humanize_codex_message(message) == "command started"
    refute StatusDashboard.humanize_codex_message(message) =~ "git status --short"
  end

  test "status dashboard formats auto-approval updates from codex" do
    message = %{
      event: :approval_auto_approved,
      message: %{
        payload: %{
          "method" => "item/commandExecution/requestApproval",
          "params" => %{"parsedCmd" => "mix test"}
        },
        decision: "acceptForSession"
      }
    }

    humanized = StatusDashboard.humanize_codex_message(message)
    assert humanized =~ "command approval requested"
    assert humanized =~ "auto-approved"
  end

  test "status dashboard formats auto-answered tool input updates from codex" do
    message = %{
      event: :tool_input_auto_answered,
      message: %{
        payload: %{
          "method" => "item/tool/requestUserInput",
          "params" => %{"question" => "Continue?"}
        },
        answer: "This is a non-interactive session. Operator input is unavailable."
      }
    }

    humanized = StatusDashboard.humanize_codex_message(message)
    assert humanized =~ "tool requires user input"
    assert humanized =~ "auto-answered"
  end

  test "status dashboard keeps reasoning, message, command, and error content categorical" do
    reasoning_message = %{
      event: :notification,
      message: %{
        "method" => "codex/event/agent_reasoning",
        "params" => %{
          "msg" => %{
            "payload" => %{"summaryText" => "SENSITIVE-BL10-DO-NOT-EXPOSE"}
          }
        }
      }
    }

    message_delta = %{
      event: :notification,
      message: %{
        "method" => "codex/event/agent_message_delta",
        "params" => %{
          "msg" => %{
            "payload" => %{"delta" => "SENSITIVE-BL10-DO-NOT-EXPOSE"}
          }
        }
      }
    }

    fallback_reasoning = %{
      event: :notification,
      message: %{
        "method" => "codex/event/agent_reasoning",
        "params" => %{"msg" => %{"payload" => %{}}}
      }
    }

    assert StatusDashboard.humanize_codex_message(reasoning_message) == "reasoning update"
    assert StatusDashboard.humanize_codex_message(message_delta) == "agent message streaming"

    refute StatusDashboard.humanize_codex_message(reasoning_message) =~
             "SENSITIVE-BL10-DO-NOT-EXPOSE"

    refute StatusDashboard.humanize_codex_message(message_delta) =~
             "SENSITIVE-BL10-DO-NOT-EXPOSE"

    assert StatusDashboard.humanize_codex_message(fallback_reasoning) == "reasoning update"
  end

  test "application stop renders offline status" do
    rendered =
      ExUnit.CaptureIO.capture_io(fn ->
        assert :ok = SymphonyElixir.Application.stop(:normal)
      end)

    assert rendered =~ "app_status=offline"
    refute rendered =~ "Timestamp:"
  end

  defp wait_for_snapshot(pid, predicate, timeout_ms \\ 200) when is_function(predicate, 1) do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_snapshot(pid, predicate, deadline_ms)
  end

  defp do_wait_for_snapshot(pid, predicate, deadline_ms) do
    snapshot = GenServer.call(pid, :snapshot)

    if predicate.(snapshot) do
      snapshot
    else
      if System.monotonic_time(:millisecond) >= deadline_ms do
        flunk("timed out waiting for orchestrator snapshot state: #{inspect(snapshot)}")
      else
        Process.sleep(5)
        do_wait_for_snapshot(pid, predicate, deadline_ms)
      end
    end
  end

  defp graph_samples_from_rates(rates_per_bucket) do
    bucket_ms = 25_000

    {timestamp, tokens, samples} =
      Enum.reduce(rates_per_bucket, {0, 0, []}, fn rate, {timestamp, tokens, acc} ->
        next_timestamp = timestamp + bucket_ms
        next_tokens = tokens + trunc(rate * bucket_ms / 1000)
        {next_timestamp, next_tokens, [{timestamp, tokens} | acc]}
      end)

    {tokens, [{timestamp, tokens} | samples]}
  end

  defp graph_samples_for_stability_test(now_ms) do
    rates_per_bucket = Enum.map(1..24, &(&1 * 5))
    bucket_ms = 25_000

    rate_for_timestamp = fn timestamp ->
      bucket_idx = min(div(max(timestamp, 0), bucket_ms), 23)
      Enum.at(rates_per_bucket, bucket_idx, 0)
    end

    0..(now_ms - 1_000)//1_000
    |> Enum.reduce({0, []}, fn timestamp, {tokens, acc} ->
      next_tokens = tokens + rate_for_timestamp.(timestamp)
      {next_tokens, [{timestamp, next_tokens} | acc]}
    end)
    |> elem(1)
  end

  defp token_accounting_state(tag, opts \\ []) do
    issue_id = "issue-token-#{tag}"
    run_id = "run-token-#{tag}"
    running_entry = token_running_entry(issue_id, run_id, opts)

    state = %Orchestrator.State{
      runner_generation: "runner-token-accounting",
      running: %{issue_id => running_entry},
      claimed: MapSet.new([issue_id]),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    {state, issue_id, run_id}
  end

  defp token_running_entry(issue_id, run_id, opts) do
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
      run_budget: %{max_turns: 20, max_tokens: Keyword.get(opts, :max_tokens), max_seconds: nil},
      run_budget_timer_ref: nil,
      worker_host: nil,
      workspace_path: nil,
      workspace_root: nil
    }
  end

  defp apply_token_usage(state, issue_id, run_id, usage) do
    update = %{
      event: :notification,
      timestamp: DateTime.utc_now(),
      run_id: run_id,
      payload: %{
        "method" => "thread/tokenUsage/updated",
        "params" => %{"tokenUsage" => %{"total" => usage}}
      }
    }

    assert {:noreply, updated_state} =
             Orchestrator.handle_info({:codex_worker_update, issue_id, update}, state)

    updated_state
  end

  defp assert_token_usage(state, issue_id, input, output, total, telemetry_observed?) do
    entry = state.running[issue_id]
    assert entry.codex_input_tokens == input
    assert entry.codex_output_tokens == output
    assert entry.codex_total_tokens == total

    assert {:reply, snapshot, _state} =
             Orchestrator.handle_call(:snapshot, {self(), make_ref()}, state)

    [row] = snapshot.running
    assert row.budget.tokens.telemetry_observed == telemetry_observed?
    assert row.budget.tokens.used == if(telemetry_observed?, do: total)
  end
end
