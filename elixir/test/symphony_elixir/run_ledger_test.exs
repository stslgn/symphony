defmodule SymphonyElixir.RunLedgerTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RunLedger

  test "appends bounded events with restrictive file permissions" do
    path = ledger_path()

    assert :ok =
             append_claim!(path, "run-1", "issue-1", "DUD-1", 0)

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_started",
               stage: "running",
               run_id: "run-1",
               issue_id: "issue-1",
               issue_identifier: "DUD-1",
               attempt: 0,
               resolved_model: "gpt-live",
               reasoning_effort: "high",
               model_catalog_source: "live",
               error_summary: "provider response must not persist",
               raw_prompt: "must not persist"
             })

    assert {:ok, [_claim, event]} = RunLedger.read_events(path)
    assert event["transition"] == "run_started"
    assert event["resolved_model"] == "gpt-live"
    assert event["reasoning_effort"] == "high"
    assert event["model_catalog_source"] == "live"
    refute Map.has_key?(event, "error_summary")
    refute Map.has_key?(event, "raw_prompt")

    assert {:ok, stat} = File.stat(path)
    assert Bitwise.band(stat.mode, 0o777) == 0o600
  end

  test "startup reconciliation closes unfinished runs and returns incremented attempts" do
    path = ledger_path()

    assert :ok =
             append_claim!(path, "run-stale", "issue-stale", "DUD-2", 2,
               worker_host: "worker-a",
               workspace_path: "/tmp/workspaces/DUD-2"
             )

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_started",
               stage: "running",
               runner_generation: "runner-old",
               run_id: "run-stale",
               issue_id: "issue-stale",
               issue_identifier: "DUD-2",
               attempt: 2,
               worker_host: "worker-a",
               workspace_path: "/tmp/workspaces/DUD-2"
             })

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-new")
    assert recovery.recovered_attempts == %{"issue-stale" => 3}

    assert recovery.recovered_dispatches["issue-stale"] == %{
             attempt: 3,
             previous_run_id: "run-stale",
             identifier: "DUD-2",
             stage: "recovery_queued",
             worker_host: "worker-a",
             workspace_path: "/tmp/workspaces/DUD-2",
             workspace_root: nil
           }

    assert recovery.parked == %{}
    refute recovery.dispatch_paused
    assert recovery.processed_operator_comment_ids == MapSet.new()
    assert recovery.operator_comment_cursors == %{}

    assert {:ok, events} = RunLedger.read_events(path)

    assert Enum.any?(events, fn event ->
             event["run_id"] == "run-stale" and
               event["transition"] == "run_interrupted" and
               event["terminal_reason"] == "runner_restarted"
           end)

    assert Enum.any?(events, fn event ->
             event["transition"] == "runner_started" and
               event["runner_generation"] == "runner-new"
           end)

    assert {:ok, next_recovery} = RunLedger.reconcile_startup(path, "runner-next")
    assert next_recovery.recovered_attempts == %{"issue-stale" => 3}
    assert next_recovery.recovered_dispatches == recovery.recovered_dispatches
    assert next_recovery.parked == %{}
  end

  test "fails startup closed when a parked tail record is corrupted" do
    path = ledger_path()

    assert :ok =
             append_claim!(
               path,
               "run-parked-tail",
               "issue-parked-tail",
               "DUD-PARKED-TAIL",
               2
             )

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_started",
               stage: "running",
               run_id: "run-parked-tail",
               issue_id: "issue-parked-tail",
               issue_identifier: "DUD-PARKED-TAIL",
               attempt: 2
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_parked",
               run_id: "run-parked-tail",
               issue_id: "issue-parked-tail",
               attempt: 2,
               wait_id: "wait-parked-tail",
               issue_identifier: "DUD-PARKED-TAIL",
               stage: "parked",
               parked_reason: "waiting_owner",
               allowed_actions: ["approve", "reject"]
             })

    corrupt_last_record!(path)

    assert {:error, {:invalid_ledger_record, 3, :malformed_json}} =
             RunLedger.reconcile_startup(path, "runner-new")
  end

  test "rejects unsafe parked fields before append and during recovery" do
    invalid_events = [
      {:tracker_state, "tracker_state", <<0xFF>>},
      {:worker_host, "worker_host", "worker\ncontrol"},
      {:workspace_path, "workspace_path", String.duplicate("p", 4_097)},
      {:workspace_root, "workspace_root", "/tmp/root\e]0;title"}
    ]

    Enum.each(invalid_events, fn {field, persisted_field, value} ->
      path = ledger_path()
      append_parked_predecessors!(path, "run-unsafe", "issue-unsafe")

      event = Map.put(valid_parked_event("run-unsafe", "issue-unsafe"), field, value)
      assert {:error, {:invalid_field, ^persisted_field}} = RunLedger.append(path, event)
      assert {:ok, [_claim, _started]} = RunLedger.read_events(path)
    end)

    path = ledger_path()
    append_parked_run!(path, "run-oversized-recovery", "issue-oversized-recovery")
    [claim, started, parked] = valid_records(path)

    rewrite_records!(path, [
      claim,
      started,
      Map.put(parked, "workspace_path", String.duplicate("p", 4_097))
    ])

    assert {:error, {:invalid_ledger_record, 3, {:invalid_field, "workspace_path"}}} =
             RunLedger.reconcile_startup(path, "runner-new")
  end

  test "fails startup closed when a terminal tail record is corrupted" do
    path = ledger_path()

    assert :ok =
             append_claim!(
               path,
               "run-terminal-tail",
               "issue-terminal-tail",
               "DUD-TERMINAL-TAIL",
               1
             )

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_started",
               stage: "running",
               run_id: "run-terminal-tail",
               issue_id: "issue-terminal-tail",
               issue_identifier: "DUD-TERMINAL-TAIL",
               attempt: 1
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_completed",
               stage: "released",
               run_id: "run-terminal-tail",
               issue_id: "issue-terminal-tail",
               issue_identifier: "DUD-TERMINAL-TAIL",
               attempt: 1,
               terminal_reason: "worker_completed"
             })

    corrupt_last_record!(path)

    assert {:error, {:invalid_ledger_record, 3, :malformed_json}} =
             RunLedger.reconcile_startup(path, "runner-new")
  end

  test "fails startup closed on corruption in the middle of the ledger" do
    path = ledger_path()

    assert :ok =
             RunLedger.append(path, %{
               transition: "dispatch_paused",
               stage: "operator",
               runner_generation: "runner-old"
             })

    File.write!(path, "not-json\n", [:append])

    assert :ok =
             RunLedger.append(path, %{
               transition: "dispatch_resumed",
               stage: "operator",
               runner_generation: "runner-old"
             })

    assert {:error, {:invalid_ledger_record, 2, :malformed_json}} =
             RunLedger.reconcile_startup(path, "runner-new")
  end

  test "rejects non-object and semantically invalid records" do
    non_object_path = ledger_path()
    File.mkdir_p!(Path.dirname(non_object_path))
    File.write!(non_object_path, Jason.encode!([]) <> "\n")

    assert {:error, {:invalid_ledger_record, 1, :not_an_object}} =
             RunLedger.read_events(non_object_path)

    invalid_attempt_path = ledger_path()

    assert :ok =
             append_claim!(
               invalid_attempt_path,
               "run-invalid-attempt",
               "issue-invalid-attempt",
               "DUD-INVALID-ATTEMPT",
               1
             )

    assert :ok =
             RunLedger.append(invalid_attempt_path, %{
               transition: "run_started",
               stage: "running",
               run_id: "run-invalid-attempt",
               issue_id: "issue-invalid-attempt",
               issue_identifier: "DUD-INVALID-ATTEMPT",
               attempt: 1
             })

    [claim, event] = valid_records(invalid_attempt_path)

    File.write!(
      invalid_attempt_path,
      Enum.map_join([claim, %{event | "attempt" => "not-an-integer"}], "\n", &Jason.encode!/1) <>
        "\n"
    )

    assert {:error, {:invalid_ledger_record, 2, {:invalid_field, "attempt"}}} =
             RunLedger.reconcile_startup(invalid_attempt_path, "runner-new")
  end

  test "rejects valid JSON semantic corruption at the tail" do
    path = ledger_path()

    append_complete_run!(path, "run-semantic-tail", "issue-semantic-tail")
    [claim, started, completed] = valid_records(path)

    File.write!(
      path,
      Enum.map_join([claim, started, Map.delete(completed, "run_id")], "\n", &Jason.encode!/1) <>
        "\n"
    )

    assert {:error, {:invalid_ledger_record, 3, {:invalid_field, "run_id"}}} =
             RunLedger.reconcile_startup(path, "runner-new")
  end

  test "rejects valid JSON semantic corruption in the middle" do
    path = ledger_path()

    append_parked_run!(path, "run-semantic-middle", "issue-semantic-middle")

    assert :ok =
             RunLedger.append(path, %{
               transition: "dispatch_paused",
               stage: "operator",
               runner_generation: "runner-old"
             })

    [claim, started, parked, paused] = valid_records(path)

    File.write!(
      path,
      Enum.map_join(
        [claim, started, %{parked | "parked_reason" => "not-a-wait-reason"}, paused],
        "\n",
        &Jason.encode!/1
      ) <>
        "\n"
    )

    assert {:error, {:invalid_ledger_record, 3, {:invalid_field, "parked_reason"}}} =
             RunLedger.reconcile_startup(path, "runner-new")
  end

  test "fails startup on immutable run identity corruption in the middle" do
    path = ledger_path()
    append_complete_run!(path, "run-identity-middle", "issue-original")
    [claim, started, completed] = valid_records(path)

    rewrite_records!(path, [
      claim,
      started,
      %{completed | "issue_id" => "issue-corrupt", "attempt" => 99}
    ])

    assert {:error, {:invalid_ledger_record, 3, {:invalid_transition_sequence, "run_completed", :run_identity_mismatch}}} =
             RunLedger.reconcile_startup(path, "runner-new")
  end

  test "fails startup on forged wait identity at the tail" do
    path = ledger_path()
    append_parked_run!(path, "run-wait-tail", "issue-wait-tail")

    assert :ok =
             RunLedger.append(path, %{
               transition: "resume_queued",
               stage: "resume_queued",
               run_id: "run-wait-tail",
               issue_id: "issue-wait-tail",
               issue_identifier: "DUD-SEMANTIC-MIDDLE",
               attempt: 2,
               wait_id: "wait-forged",
               parked_reason: "waiting_owner",
               allowed_actions: ["approve", "reject"],
               worker_host: "worker-b",
               workspace_path: "/srv/forged"
             })

    assert {:error, {:invalid_ledger_record, 4, {:invalid_transition_sequence, "resume_queued", :wait_identity_mismatch}}} =
             RunLedger.reconcile_startup(path, "runner-new")
  end

  test "fails startup on orphan and duplicate terminal events" do
    orphan_path = ledger_path()

    assert :ok =
             RunLedger.append(orphan_path, %{
               transition: "run_interrupted",
               stage: "released",
               run_id: "run-orphan",
               issue_id: "issue-orphan",
               issue_identifier: "DUD-ORPHAN",
               attempt: 0,
               terminal_reason: "runner_restarted"
             })

    assert {:error, {:invalid_ledger_record, 1, {:invalid_transition_sequence, "run_interrupted", :missing_run_predecessor}}} =
             RunLedger.reconcile_startup(orphan_path, "runner-new")

    duplicate_path = ledger_path()
    append_complete_run!(duplicate_path, "run-duplicate", "issue-duplicate")

    assert :ok =
             RunLedger.append(duplicate_path, %{
               transition: "run_failed",
               stage: "released",
               run_id: "run-duplicate",
               issue_id: "issue-duplicate",
               issue_identifier: "DUD-SEMANTIC-TAIL",
               attempt: 1,
               terminal_reason: "worker_exit"
             })

    assert {:error, {:invalid_ledger_record, 4, {:invalid_transition_sequence, "run_failed", :illegal_predecessor}}} =
             RunLedger.reconcile_startup(duplicate_path, "runner-new")
  end

  test "rejects unknown stage and terminal reason values" do
    path = ledger_path()

    assert {:error, {:invalid_field, "stage"}} =
             RunLedger.append(path, %{
               transition: "run_started",
               stage: "semantically-open-stage",
               run_id: "run-stage",
               issue_id: "issue-stage",
               issue_identifier: "DUD-STAGE",
               attempt: 0
             })

    assert {:error, {:invalid_field, "terminal_reason"}} =
             RunLedger.append(path, %{
               transition: "run_interrupted",
               stage: "released",
               run_id: "run-reason",
               issue_id: "issue-reason",
               issue_identifier: "DUD-REASON",
               attempt: 0,
               terminal_reason: "semantically-open-reason"
             })
  end

  test "rejects transitions outside the closed vocabulary" do
    path = ledger_path()

    assert {:error, {:unknown_transition, "run_maybe"}} =
             RunLedger.append(path, %{transition: "run_maybe"})

    refute File.exists?(path)
  end

  test "startup reconciliation restores parked waits until they are resumed" do
    path = ledger_path()

    assert :ok = append_claim!(path, "run-parked", "issue-parked", "DUD-3", 1)

    assert :ok =
             append_started!(path, "run-parked", "issue-parked", "DUD-3", 1)

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_parked",
               stage: "parked",
               run_id: "run-parked",
               issue_id: "issue-parked",
               issue_identifier: "DUD-3",
               attempt: 1,
               wait_id: "wait-parked",
               parked_reason: "waiting_owner",
               allowed_actions: ["approve", "reject"],
               tracker_state: "Human Review"
             })

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-new")
    assert %{"issue-parked" => parked} = recovery.parked
    assert recovery.recovered_attempts == %{}
    assert recovery.queued_resumes == %{}

    assert parked["wait_id"] == "wait-parked"
    assert parked["parked_reason"] == "waiting_owner"

    assert :ok =
             RunLedger.append(path, %{
               transition: "wait_resumed",
               stage: "parked",
               run_id: "run-parked",
               issue_id: "issue-parked",
               issue_identifier: "DUD-3",
               attempt: 2,
               wait_id: "wait-parked",
               parked_reason: "waiting_owner",
               allowed_actions: ["approve", "reject"]
             })

    assert {:ok, next_recovery} = RunLedger.reconcile_startup(path, "runner-next")
    assert next_recovery.recovered_attempts == %{}
    assert next_recovery.queued_resumes["issue-parked"]["attempt"] == 2
    assert next_recovery.parked == %{}
  end

  test "accepts and restores a token telemetry integrity park" do
    path = ledger_path()
    run_id = "run-token-integrity"
    issue_id = "issue-token-integrity"
    identifier = "DUD-TOKEN-INTEGRITY"

    assert :ok = append_claim!(path, run_id, issue_id, identifier, 1)
    assert :ok = append_started!(path, run_id, issue_id, identifier, 1)

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_parked",
               stage: "parked",
               run_id: run_id,
               issue_id: issue_id,
               issue_identifier: identifier,
               attempt: 1,
               wait_id: "wait-token-integrity",
               parked_reason: "run_budget_exhausted",
               terminal_reason: "token_telemetry_integrity_failed",
               allowed_actions: ["retry", "reject"]
             })

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-token-integrity")
    assert recovery.parked[issue_id]["terminal_reason"] == "token_telemetry_integrity_failed"
  end

  test "durable claim atomically consumes a queued resume" do
    path = ledger_path()

    append_parked_run!(path, "run-resume-source", "issue-resume-claim")

    assert :ok =
             RunLedger.append(path, %{
               transition: "resume_queued",
               stage: "resume_queued",
               run_id: "run-resume-source",
               issue_id: "issue-resume-claim",
               issue_identifier: "DUD-SEMANTIC-MIDDLE",
               attempt: 2,
               wait_id: "wait-semantic-middle",
               parked_reason: "waiting_owner",
               allowed_actions: ["approve", "reject"]
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_claimed",
               stage: "claimed",
               run_id: "run-resume-dispatched",
               issue_id: "issue-resume-claim",
               issue_identifier: "DUD-SEMANTIC-MIDDLE",
               attempt: 2
             })

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-after-claim")
    assert recovery.queued_resumes == %{}
    assert recovery.recovered_attempts == %{"issue-resume-claim" => 3}
  end

  test "startup recovers terminal next-attempt intent before retry scheduling completes" do
    path = ledger_path()
    run_id = "run-terminal-retry-intent"
    issue_id = "issue-terminal-retry-intent"
    identifier = "DUD-RETRY-INTENT"

    assert :ok = append_claim!(path, run_id, issue_id, identifier, 3)
    assert :ok = append_started!(path, run_id, issue_id, identifier, 3)

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_failed",
               stage: "released",
               run_id: run_id,
               issue_id: issue_id,
               issue_identifier: identifier,
               attempt: 3,
               terminal_reason: "worker_exit",
               next_action: "retry",
               next_attempt: 4,
               worker_host: "worker-a",
               workspace_path: "/srv/symphony/DUD-RETRY-INTENT"
             })

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-retry-recovery")

    assert recovery.recovered_dispatches[issue_id] == %{
             attempt: 4,
             previous_run_id: run_id,
             identifier: identifier,
             worker_host: "worker-a",
             workspace_path: "/srv/symphony/DUD-RETRY-INTENT",
             workspace_root: nil,
             stage: "retry_queued"
           }

    assert :ok =
             RunLedger.append(path, %{
               transition: "retry_scheduled",
               stage: "retry_queued",
               run_id: run_id,
               issue_id: issue_id,
               issue_identifier: identifier,
               attempt: 3,
               next_attempt: 4,
               worker_host: "worker-a",
               workspace_path: "/srv/symphony/DUD-RETRY-INTENT"
             })

    assert {:ok, next_recovery} = RunLedger.reconcile_startup(path, "runner-retry-next")
    assert next_recovery.recovered_attempts[issue_id] == 4
  end

  test "retry scheduling is bound to exact terminal intent and immutable affinity" do
    path = ledger_path()
    run_id = "run-bound-retry"
    issue_id = "issue-bound-retry"
    identifier = "DUD-BOUND-RETRY"

    assert :ok =
             append_claim!(path, run_id, issue_id, identifier, 3,
               worker_host: "worker-a",
               workspace_path: "/srv/a/DUD-BOUND-RETRY",
               workspace_root: "/srv/a"
             )

    assert :ok = append_started!(path, run_id, issue_id, identifier, 3)
    assert :ok = append_retry_terminal!(path, run_id, issue_id, identifier, 3, 4)

    assert :ok =
             RunLedger.append(path, %{
               transition: "retry_scheduled",
               stage: "retry_queued",
               run_id: run_id,
               issue_id: issue_id,
               issue_identifier: identifier,
               attempt: 3,
               next_action: "retry",
               next_attempt: 9,
               worker_host: "worker-b",
               workspace_path: "/srv/b/DUD-BOUND-RETRY",
               workspace_root: "/srv/b"
             })

    expected_error =
      {:error, {:invalid_ledger_record, 4, {:invalid_transition_sequence, "retry_scheduled", :retry_terminal_intent_mismatch}}}

    assert expected_error == RunLedger.read_events(path)

    [claim, started, terminal, forged] = valid_records(path)
    rewrite_records!(path, [claim, started, terminal, %{forged | "next_attempt" => 4}])

    assert {:error, {:invalid_ledger_record, 4, {:invalid_transition_sequence, "retry_scheduled", :retry_affinity_mismatch}}} =
             RunLedger.read_events(path)
  end

  test "legacy retry affinity omission preserves established run affinity" do
    path = ledger_path()
    run_id = "run-legacy-affinity"
    issue_id = "issue-legacy-affinity"
    identifier = "DUD-LEGACY-AFFINITY"

    assert :ok =
             append_claim!(path, run_id, issue_id, identifier, 2,
               worker_host: "worker-a",
               workspace_path: "/srv/a/DUD-LEGACY-AFFINITY",
               workspace_root: "/srv/a"
             )

    assert :ok = append_started!(path, run_id, issue_id, identifier, 2)
    assert :ok = append_retry_terminal!(path, run_id, issue_id, identifier, 2, 3)

    assert :ok =
             RunLedger.append(path, %{
               transition: "retry_scheduled",
               stage: "retry_queued",
               run_id: run_id,
               issue_id: issue_id,
               issue_identifier: identifier,
               attempt: 2,
               next_attempt: 3
             })

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-legacy-affinity")

    assert recovery.recovered_dispatches[issue_id] == %{
             attempt: 3,
             previous_run_id: run_id,
             identifier: identifier,
             worker_host: "worker-a",
             workspace_path: "/srv/a/DUD-LEGACY-AFFINITY",
             workspace_root: "/srv/a",
             stage: "retry_queued"
           }
  end

  test "retry scheduling permits only an exact semantic duplicate" do
    path = ledger_path()
    run_id = "run-idempotent-retry"
    issue_id = "issue-idempotent-retry"
    identifier = "DUD-IDEMPOTENT-RETRY"

    assert :ok = append_claim!(path, run_id, issue_id, identifier, 1)
    assert :ok = append_started!(path, run_id, issue_id, identifier, 1)
    assert :ok = append_retry_terminal!(path, run_id, issue_id, identifier, 1, 2)

    event = %{
      transition: "retry_scheduled",
      stage: "retry_queued",
      run_id: run_id,
      issue_id: issue_id,
      issue_identifier: identifier,
      attempt: 1,
      next_action: "retry",
      next_attempt: 2
    }

    assert :ok = RunLedger.append(path, event)
    assert :ok = RunLedger.append(path, event)
    assert {:ok, _events} = RunLedger.read_events(path)

    assert :ok = RunLedger.append(path, %{event | next_action: "continuation"})

    expected_error =
      {:error, {:invalid_ledger_record, 6, {:invalid_transition_sequence, "retry_scheduled", :retry_terminal_intent_mismatch}}}

    assert expected_error == RunLedger.read_events(path)
  end

  test "workspace cleanup records require durable terminal cleanup intent" do
    path = ledger_path()
    append_complete_run!(path, "run-no-cleanup", "issue-no-cleanup")

    assert :ok =
             RunLedger.append(path, %{
               transition: "workspace_cleanup_completed",
               stage: "cleanup",
               run_id: "run-no-cleanup",
               issue_id: "issue-no-cleanup",
               issue_identifier: "DUD-SEMANTIC-TAIL",
               attempt: 1,
               workspace_path: "/srv/a/DUD-SEMANTIC-TAIL",
               workspace_root: "/srv/a"
             })

    expected_error =
      {:error, {:invalid_ledger_record, 4, {:invalid_transition_sequence, "workspace_cleanup_completed", :missing_cleanup_request}}}

    assert expected_error == RunLedger.read_events(path)
  end

  test "startup reconciliation restores global pause and operator command cursors" do
    path = ledger_path()

    assert :ok =
             RunLedger.append(path, %{
               transition: "dispatch_paused",
               stage: "operator",
               runner_generation: "runner-old"
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "operator_cursor_initialized",
               stage: "operator",
               issue_id: "issue-1",
               comment_created_at: "2026-08-03T09:59:59.000Z",
               runner_generation: "runner-old"
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "operator_cursor_advanced",
               stage: "operator",
               issue_id: "issue-1",
               comment_id: "comment-1",
               comment_created_at: "2026-08-03T10:00:00.000Z",
               runner_generation: "runner-old"
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "operator_cursor_advanced",
               stage: "operator",
               issue_id: "issue-1",
               comment_id: "comment-2",
               comment_created_at: "2026-08-03T10:00:00.000Z",
               runner_generation: "runner-old"
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "operator_cursor_advanced",
               stage: "operator",
               issue_id: "issue-1",
               comment_id: "comment-old",
               comment_created_at: "2026-08-03T09:59:58.000Z",
               runner_generation: "runner-old"
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "operator_command_applied",
               stage: "operator",
               issue_id: "issue-1",
               comment_id: "comment-1",
               comment_created_at: "2026-08-03T10:00:00.000Z",
               operator_command: "retry",
               runner_generation: "runner-old"
             })

    assert {:ok, paused_recovery} = RunLedger.reconcile_startup(path, "runner-paused")
    assert paused_recovery.dispatch_paused
    assert paused_recovery.processed_operator_comment_ids == MapSet.new(["comment-1"])

    assert paused_recovery.operator_comment_cursors == %{
             "issue-1" => %{
               created_at: "2026-08-03T10:00:00.000Z",
               comment_ids: MapSet.new(["comment-1", "comment-2"])
             }
           }

    assert :ok =
             RunLedger.append(path, %{
               transition: "dispatch_resumed",
               stage: "operator",
               runner_generation: "runner-old"
             })

    assert {:ok, resumed_recovery} = RunLedger.reconcile_startup(path, "runner-resumed")
    refute resumed_recovery.dispatch_paused
  end

  test "returns filesystem read and create errors" do
    assert {:ok, []} = RunLedger.read_events(ledger_path())

    directory_path = Path.dirname(ledger_path())
    File.mkdir_p!(directory_path)
    assert {:error, _reason} = RunLedger.read_events(directory_path)

    locked_directory = Path.join(System.tmp_dir!(), "symphony-ledger-locked")
    File.mkdir_p!(locked_directory)
    File.chmod!(locked_directory, 0o500)

    on_exit(fn ->
      File.chmod(locked_directory, 0o700)
      File.rm_rf(locked_directory)
    end)

    assert {:error, _reason} =
             RunLedger.append(Path.join(locked_directory, "events.jsonl"), %{
               transition: "runner_started"
             })
  end

  test "startup reconciliation stops when an interrupted event cannot be appended" do
    path = ledger_path()

    assert :ok = append_claim!(path, "run-stale", "issue-stale", "DUD-STALE", 1)

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_started",
               stage: "running",
               run_id: "run-stale",
               issue_id: "issue-stale",
               issue_identifier: "DUD-STALE",
               attempt: 1
             })

    append_fn = fn _path, _event -> {:error, :forced_write_failure} end

    assert {:error, :forced_write_failure} =
             RunLedger.reconcile_startup(path, "runner-new", append_fn: append_fn)
  end

  defp ledger_path do
    Path.join(
      System.tmp_dir!(),
      "symphony-run-ledger-#{RunLedger.new_id("test")}/events.jsonl"
    )
  end

  defp corrupt_last_record!(path) do
    records = valid_records(path)
    last = records |> List.last() |> Jason.encode!()
    truncated = binary_part(last, 0, byte_size(last) - 1)

    contents =
      records
      |> Enum.drop(-1)
      |> Enum.map_join("\n", &Jason.encode!/1)

    prefix = if contents == "", do: "", else: contents <> "\n"
    File.write!(path, prefix <> truncated <> "\n")
  end

  defp valid_records(path) do
    path
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.map(&Jason.decode!/1)
  end

  defp rewrite_records!(path, records) do
    File.write!(path, Enum.map_join(records, "\n", &Jason.encode!/1) <> "\n")
  end

  defp append_complete_run!(path, run_id, issue_id) do
    issue_identifier = "DUD-SEMANTIC-TAIL"

    assert :ok = append_claim!(path, run_id, issue_id, issue_identifier, 1)

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_started",
               stage: "running",
               run_id: run_id,
               issue_id: issue_id,
               issue_identifier: issue_identifier,
               attempt: 1
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_completed",
               stage: "released",
               run_id: run_id,
               issue_id: issue_id,
               issue_identifier: issue_identifier,
               attempt: 1,
               terminal_reason: "worker_completed"
             })
  end

  defp append_retry_terminal!(path, run_id, issue_id, issue_identifier, attempt, next_attempt) do
    RunLedger.append(path, %{
      transition: "run_failed",
      stage: "released",
      run_id: run_id,
      issue_id: issue_id,
      issue_identifier: issue_identifier,
      attempt: attempt,
      terminal_reason: "worker_exit",
      next_action: "retry",
      next_attempt: next_attempt
    })
  end

  defp append_parked_run!(path, run_id, issue_id) do
    append_parked_predecessors!(path, run_id, issue_id)

    assert :ok = RunLedger.append(path, valid_parked_event(run_id, issue_id))
  end

  defp append_parked_predecessors!(path, run_id, issue_id) do
    assert :ok =
             append_claim!(path, run_id, issue_id, "DUD-SEMANTIC-MIDDLE", 1)

    assert :ok =
             append_started!(path, run_id, issue_id, "DUD-SEMANTIC-MIDDLE", 1)
  end

  defp valid_parked_event(run_id, issue_id) do
    %{
      transition: "run_parked",
      stage: "parked",
      run_id: run_id,
      issue_id: issue_id,
      issue_identifier: "DUD-SEMANTIC-MIDDLE",
      attempt: 1,
      wait_id: "wait-semantic-middle",
      parked_reason: "waiting_owner",
      allowed_actions: ["approve", "reject"]
    }
  end

  defp append_claim!(path, run_id, issue_id, issue_identifier, attempt, opts \\ []) do
    RunLedger.append(path, %{
      transition: "run_claimed",
      stage: "claimed",
      run_id: run_id,
      issue_id: issue_id,
      issue_identifier: issue_identifier,
      attempt: attempt,
      worker_host: Keyword.get(opts, :worker_host),
      workspace_path: Keyword.get(opts, :workspace_path),
      workspace_root: Keyword.get(opts, :workspace_root)
    })
  end

  defp append_started!(path, run_id, issue_id, issue_identifier, attempt) do
    RunLedger.append(path, %{
      transition: "run_started",
      stage: "running",
      run_id: run_id,
      issue_id: issue_id,
      issue_identifier: issue_identifier,
      attempt: attempt
    })
  end
end
