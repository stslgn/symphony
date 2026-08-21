defmodule SymphonyElixir.RunLedgerTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RunLedger

  test "records completed tracker admission before a run starts" do
    path = ledger_path()

    assert :ok = append_claim!(path, "run-admitted", "issue-admitted", "DUD-ADMITTED", 0)

    admission = %{
      stage: "admission",
      run_id: "run-admitted",
      issue_id: "issue-admitted",
      issue_identifier: "DUD-ADMITTED",
      attempt: 0,
      admission_id: "admission-1",
      source_state: "Agent Ready",
      target_state: "Agent Running",
      issue_snapshot_schema: "symphony.issue_snapshot.v1",
      issue_snapshot_bytes: 128,
      issue_snapshot_sha256: String.duplicate("a", 64),
      tracker_authority_digest: String.duplicate("b", 64)
    }

    assert :ok =
             RunLedger.append(
               path,
               Map.put(admission, :transition, "tracker_admission_io_started")
             )

    assert :ok =
             RunLedger.append(
               path,
               Map.put(admission, :transition, "tracker_admission_completed")
             )

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_started",
               stage: "running",
               run_id: "run-admitted",
               issue_id: "issue-admitted",
               issue_identifier: "DUD-ADMITTED",
               attempt: 0
             })

    assert {:ok, [_claim, started, completed, run_started]} = RunLedger.read_events(path)
    assert started["admission_id"] == "admission-1"
    assert started["issue_snapshot_bytes"] == 128
    assert completed["issue_snapshot_sha256"] == String.duplicate("a", 64)
    assert run_started["transition"] == "run_started"
  end

  test "rejects run start while tracker admission is incomplete" do
    path = ledger_path()

    assert :ok = append_claim!(path, "run-incomplete", "issue-incomplete", "DUD-INCOMPLETE", 0)

    assert :ok =
             RunLedger.append(path, %{
               transition: "tracker_admission_io_started",
               stage: "admission",
               run_id: "run-incomplete",
               issue_id: "issue-incomplete",
               issue_identifier: "DUD-INCOMPLETE",
               attempt: 0,
               admission_id: "admission-incomplete",
               source_state: "Agent Ready",
               target_state: "Agent Running",
               issue_snapshot_schema: "symphony.issue_snapshot.v1",
               issue_snapshot_bytes: 128,
               issue_snapshot_sha256: String.duplicate("a", 64),
               tracker_authority_digest: String.duplicate("b", 64)
             })

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_started",
               stage: "running",
               run_id: "run-incomplete",
               issue_id: "issue-incomplete",
               issue_identifier: "DUD-INCOMPLETE",
               attempt: 0
             })

    assert {:error, {:invalid_ledger_record, 3, {:invalid_transition_sequence, "run_started", :illegal_predecessor}}} =
             RunLedger.read_events(path)
  end

  test "startup preserves an in-flight tracker admission for exact reconciliation" do
    path = ledger_path()

    assert :ok = append_claim!(path, "run-reconcile", "issue-reconcile", "DUD-RECONCILE", 2)

    assert :ok =
             RunLedger.append(path, %{
               transition: "tracker_admission_io_started",
               stage: "admission",
               run_id: "run-reconcile",
               issue_id: "issue-reconcile",
               issue_identifier: "DUD-RECONCILE",
               attempt: 2,
               admission_id: "admission-reconcile",
               source_state: "Agent Ready",
               target_state: "Agent Running",
               issue_snapshot_schema: "symphony.issue_snapshot.v1",
               issue_snapshot_bytes: 128,
               issue_snapshot_sha256: String.duplicate("a", 64),
               tracker_authority_digest: String.duplicate("b", 64),
               workspace_path: "/tmp/workspaces/DUD-RECONCILE",
               workspace_root: "/tmp/workspaces"
             })

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-new")
    assert recovery.recovered_attempts == %{}
    assert recovery.recovered_dispatches == %{}

    assert recovery.tracker_admissions["issue-reconcile"] == %{
             admission_id: "admission-reconcile",
             attempt: 2,
             identifier: "DUD-RECONCILE",
             issue_id: "issue-reconcile",
             issue_snapshot_bytes: 128,
             issue_snapshot_schema: "symphony.issue_snapshot.v1",
             issue_snapshot_sha256: String.duplicate("a", 64),
             run_id: "run-reconcile",
             source_state: "Agent Ready",
             status: "io_started",
             target_state: "Agent Running",
             tracker_authority_digest: String.duplicate("b", 64),
             worker_host: nil,
             workspace_path: "/tmp/workspaces/DUD-RECONCILE",
             workspace_root: "/tmp/workspaces"
           }

    assert {:ok, events} = RunLedger.read_events(path)

    refute Enum.any?(events, fn event ->
             event["run_id"] == "run-reconcile" and
               event["transition"] == "run_interrupted"
           end)
  end

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

  test "accepts the typed uncached-input budget terminal reason" do
    path = ledger_path()
    assert :ok = append_claim!(path, "run-uncached", "issue-uncached", "DUD-UNCACHED", 0)
    assert :ok = append_started!(path, "run-uncached", "issue-uncached", "DUD-UNCACHED", 0)

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_parked",
               stage: "parked",
               run_id: "run-uncached",
               issue_id: "issue-uncached",
               issue_identifier: "DUD-UNCACHED",
               attempt: 0,
               wait_id: "wait-uncached",
               parked_reason: "run_budget_exhausted",
               allowed_actions: ["retry", "reject"],
               terminal_reason: "uncached_input_budget_exhausted"
             })

    assert {:ok, [_claim, _started, parked]} = RunLedger.read_events(path)
    assert parked["terminal_reason"] == "uncached_input_budget_exhausted"
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

  test "operator action context restores a pending outcome until the audit event lands" do
    path = ledger_path()
    run_id = "run-pending-operator-outcome"
    issue_id = "issue-pending-operator-outcome"
    comment_at = "2026-08-03T10:00:01.000Z"

    append_parked_run!(path, run_id, issue_id)

    resume_event =
      run_id
      |> valid_parked_event(issue_id)
      |> Map.merge(%{
        transition: "resume_queued",
        stage: "resume_queued",
        attempt: 2,
        comment_id: "comment-pending-outcome",
        comment_created_at: comment_at,
        operator_command: "approve"
      })

    assert :ok = RunLedger.append(path, resume_event)

    assert {:ok, pending_recovery} =
             RunLedger.reconcile_startup(path, "runner-pending-outcome")

    assert pending_recovery.pending_operator_outcomes["comment-pending-outcome"][
             "transition"
           ] == "resume_queued"

    refute MapSet.member?(
             pending_recovery.processed_operator_comment_ids,
             "comment-pending-outcome"
           )

    assert :ok =
             RunLedger.append(path, %{
               transition: "operator_command_applied",
               stage: "operator",
               issue_id: issue_id,
               comment_id: "comment-pending-outcome",
               comment_created_at: comment_at,
               operator_command: "approve",
               runner_generation: "runner-pending-outcome"
             })

    assert {:ok, completed_recovery} =
             RunLedger.reconcile_startup(path, "runner-completed-outcome")

    assert completed_recovery.pending_operator_outcomes == %{}

    assert MapSet.member?(
             completed_recovery.processed_operator_comment_ids,
             "comment-pending-outcome"
           )

    assert completed_recovery.operator_comment_cursors[issue_id] == %{
             created_at: comment_at,
             comment_ids: MapSet.new(["comment-pending-outcome"])
           }

    invalid_path = ledger_path()
    append_parked_predecessors!(invalid_path, "run-partial-context", "issue-partial-context")

    partial_context_event =
      "run-partial-context"
      |> valid_parked_event("issue-partial-context")
      |> Map.put(:comment_id, "comment-without-context")

    assert {:error, {:invalid_field, "operator_command_context"}} =
             RunLedger.append(invalid_path, partial_context_event)
  end

  test "ordered validation rejects duplicate model resolution" do
    path = ledger_path()
    run_id = "run-duplicate-model"
    issue_id = "issue-duplicate-model"

    assert :ok = append_claim!(path, run_id, issue_id, "DUD-DUPLICATE-MODEL", 0)
    assert :ok = append_started!(path, run_id, issue_id, "DUD-DUPLICATE-MODEL", 0)

    model_event = %{
      transition: "model_resolved",
      stage: "running",
      run_id: run_id,
      issue_id: issue_id,
      issue_identifier: "DUD-DUPLICATE-MODEL",
      attempt: 0,
      resolved_model: "gpt-live",
      reasoning_effort: "high",
      model_catalog_source: "live"
    }

    assert :ok = RunLedger.append(path, model_event)
    assert :ok = RunLedger.append(path, model_event)

    assert {:error, {:invalid_ledger_record, 4, sequence_error}} =
             RunLedger.read_events(path)

    assert sequence_error ==
             {:invalid_transition_sequence, "model_resolved", :duplicate_model_resolution}
  end

  test "ordered validation rejects duplicate operator actions and outcomes" do
    action_path = ledger_path()
    action = append_context_operator_action!(action_path, "duplicate-action")
    assert :ok = RunLedger.append(action_path, action)

    assert {:error, {:invalid_ledger_record, 5, sequence_error}} =
             RunLedger.read_events(action_path)

    assert sequence_error ==
             {:invalid_transition_sequence, "wait_rejected", :duplicate_operator_action}

    outcome_path = ledger_path()
    outcome_action = append_context_operator_action!(outcome_path, "duplicate-outcome")
    outcome = operator_outcome_event(outcome_action, "operator_command_applied")
    assert :ok = RunLedger.append(outcome_path, outcome)
    assert :ok = RunLedger.append(outcome_path, outcome)

    assert {:error, {:invalid_ledger_record, 6, sequence_error}} =
             RunLedger.read_events(outcome_path)

    assert sequence_error ==
             {:invalid_transition_sequence, "operator_command_applied", :duplicate_operator_outcome}
  end

  test "ordered validation correlates operator outcome identity and decision" do
    mismatches = [
      {:issue_id, "wrong-issue", "operator_command_applied", :operator_command_identity_mismatch},
      {:comment_created_at, "2026-08-03T10:01:00.000Z", "operator_command_applied", :operator_command_identity_mismatch},
      {:operator_command, "retry", "operator_command_applied", :operator_command_identity_mismatch},
      {:transition, nil, "operator_command_rejected", :operator_command_outcome_mismatch}
    ]

    Enum.with_index(mismatches, 1)
    |> Enum.each(fn {{field, value, transition, expected_reason}, index} ->
      path = ledger_path()
      action = append_context_operator_action!(path, "mismatch-#{index}")

      outcome =
        action
        |> operator_outcome_event(transition)
        |> maybe_override_operator_outcome(field, value)

      assert :ok = RunLedger.append(path, outcome)

      assert {:error, {:invalid_ledger_record, 5, {:invalid_transition_sequence, ^transition, ^expected_reason}}} =
               RunLedger.read_events(path)
    end)
  end

  test "ordered validation keeps one legacy outcome-only record compatible" do
    path = ledger_path()

    legacy_outcome = %{
      transition: "operator_command_rejected",
      stage: "operator",
      issue_id: "issue-legacy-outcome",
      comment_id: "comment-legacy-outcome",
      comment_created_at: "2026-08-03T10:00:00.000Z",
      operator_command: "retry",
      runner_generation: "runner-legacy-outcome"
    }

    assert :ok = RunLedger.append(path, legacy_outcome)
    assert {:ok, [_event]} = RunLedger.read_events(path)
  end

  test "recovery accepts a legacy wait release without weakening new appends" do
    path = ledger_path()
    run_id = "run-legacy-wait-release"
    issue_id = "issue-legacy-wait-release"
    append_parked_run!(path, run_id, issue_id)

    release_event =
      run_id
      |> valid_parked_event(issue_id)
      |> Map.merge(%{transition: "wait_released", release_reason: "tracker_terminal"})

    assert :ok = RunLedger.append(path, release_event)

    strict_path = ledger_path()
    append_parked_run!(strict_path, run_id, issue_id)

    assert {:error, {:invalid_field, "release_reason"}} =
             strict_path
             |> RunLedger.append(Map.delete(release_event, :release_reason))

    assert :ok = RunLedger.append(strict_path, release_event)

    invalid_records = valid_records(strict_path)

    invalid_release =
      invalid_records
      |> List.last()
      |> Map.delete("release_reason")
      |> Map.put("terminal_reason", "tracker_terminal")

    rewrite_records!(strict_path, List.replace_at(invalid_records, -1, invalid_release))

    assert {:error, {:invalid_ledger_record, 4, {:invalid_field, "release_reason"}}} =
             RunLedger.read_events(strict_path)

    records = valid_records(path)

    legacy_release =
      records
      |> List.last()
      |> Map.delete("release_reason")
      |> Map.put("terminal_reason", "tracker_released")

    rewrite_records!(path, List.replace_at(records, -1, legacy_release))

    assert {:ok, events} = RunLedger.read_events(path)
    assert is_nil(events |> List.last() |> Map.get("release_reason"))

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-after-legacy-release")
    assert recovery.parked == %{}
    assert recovery.cleanup_pending == %{}
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

  test "handles empty and unterminated ledgers and surfaces exclusive create errors" do
    empty_path = ledger_path()
    File.mkdir_p!(Path.dirname(empty_path))
    File.write!(empty_path, "")
    assert {:ok, []} = RunLedger.read_events(empty_path)

    unterminated_path = ledger_path()

    assert :ok =
             RunLedger.append(unterminated_path, %{
               transition: "runner_started",
               stage: "startup",
               runner_generation: "runner-1"
             })

    File.write!(unterminated_path, String.trim_trailing(File.read!(unterminated_path), "\n"))
    assert {:ok, [%{"transition" => "runner_started"}]} = RunLedger.read_events(unterminated_path)

    too_long_path =
      Path.join(Path.dirname(ledger_path()), String.duplicate("a", 256))

    assert {:error, :enametoolong} =
             RunLedger.append(too_long_path, %{
               transition: "runner_started",
               stage: "startup",
               runner_generation: "runner-1"
             })
  end

  test "rejects missing required attempts, invalid cursor timestamps, and schema drift" do
    required_attempt_path = ledger_path()
    assert :ok = append_claim!(required_attempt_path, "run-1", "issue-1", "DUD-1", 1)
    [claim] = valid_records(required_attempt_path)
    rewrite_records!(required_attempt_path, [Map.delete(claim, "attempt")])

    assert {:error, {:invalid_ledger_record, 1, {:invalid_field, "attempt"}}} =
             RunLedger.read_events(required_attempt_path)

    required_next_attempt_path = ledger_path()

    assert :ok =
             RunLedger.append(required_next_attempt_path, %{
               transition: "retry_scheduled",
               stage: "retry_queued",
               run_id: "run-1",
               issue_id: "issue-1",
               issue_identifier: "DUD-1",
               attempt: 1,
               next_attempt: 2
             })

    [retry] = valid_records(required_next_attempt_path)
    rewrite_records!(required_next_attempt_path, [Map.delete(retry, "next_attempt")])

    assert {:error, {:invalid_ledger_record, 1, {:invalid_field, "next_attempt"}}} =
             RunLedger.read_events(required_next_attempt_path)

    timestamp_path = ledger_path()

    assert :ok =
             RunLedger.append(timestamp_path, %{
               transition: "operator_cursor_initialized",
               stage: "operator",
               issue_id: "issue-1",
               comment_created_at: "2026-08-05T12:30:00Z",
               runner_generation: "runner-1"
             })

    [cursor] = valid_records(timestamp_path)
    rewrite_records!(timestamp_path, [%{cursor | "comment_created_at" => "not-a-time"}])

    assert {:error, {:invalid_ledger_record, 1, {:invalid_field, "comment_created_at"}}} =
             RunLedger.read_events(timestamp_path)

    schema_path = ledger_path()

    assert :ok =
             RunLedger.append(schema_path, %{
               transition: "runner_started",
               stage: "startup",
               runner_generation: "runner-1"
             })

    [runner] = valid_records(schema_path)
    rewrite_records!(schema_path, [%{runner | "schema_version" => 2}])

    assert {:error, {:invalid_ledger_record, 1, :unsupported_schema_version}} =
             RunLedger.read_events(schema_path)
  end

  test "rejects untyped optional strings during semantic validation" do
    path = ledger_path()

    assert :ok =
             RunLedger.append(path, %{
               transition: "runner_started",
               stage: "startup",
               runner_generation: "runner-1"
             })

    [event] = valid_records(path)
    rewrite_records!(path, [%{event | "runner_generation" => 123}])

    assert {:error, {:invalid_ledger_record, 1, {:invalid_field, "runner_generation"}}} =
             RunLedger.read_events(path)
  end

  test "enforces affinity and terminal retry predecessors" do
    affinity_path = ledger_path()

    assert :ok =
             append_claim!(affinity_path, "run-affinity", "issue-affinity", "DUD-AFFINITY", 1, worker_host: "worker-a")

    assert :ok =
             RunLedger.append(affinity_path, %{
               transition: "run_started",
               stage: "running",
               run_id: "run-affinity",
               issue_id: "issue-affinity",
               issue_identifier: "DUD-AFFINITY",
               attempt: 1,
               worker_host: "worker-b"
             })

    assert_sequence_error(affinity_path, 2, "run_started", :run_affinity_mismatch)

    nonterminal_path = ledger_path()
    assert :ok = append_claim!(nonterminal_path, "run-live", "issue-live", "DUD-LIVE", 1)
    assert :ok = append_started!(nonterminal_path, "run-live", "issue-live", "DUD-LIVE", 1)
    assert :ok = append_retry!(nonterminal_path, "run-live", "issue-live", "DUD-LIVE", 1, 2)
    assert_sequence_error(nonterminal_path, 3, "retry_scheduled", :illegal_predecessor)

    parked_path = ledger_path()
    append_parked_run!(parked_path, "run-parked", "issue-parked")

    assert :ok =
             append_retry!(
               parked_path,
               "run-parked",
               "issue-parked",
               "DUD-SEMANTIC-MIDDLE",
               1,
               2
             )

    assert_sequence_error(parked_path, 4, "retry_scheduled", :parked_run_cannot_retry)
  end

  test "enforces increasing attempts and exact terminal retry intent" do
    nonincreasing_path = ledger_path()
    assert :ok = append_claim!(nonincreasing_path, "run-1", "issue-1", "DUD-1", 1)
    assert :ok = append_started!(nonincreasing_path, "run-1", "issue-1", "DUD-1", 1)
    assert :ok = append_retry_terminal!(nonincreasing_path, "run-1", "issue-1", "DUD-1", 1, 1)
    assert :ok = append_retry!(nonincreasing_path, "run-1", "issue-1", "DUD-1", 1, 1)
    assert_sequence_error(nonincreasing_path, 4, "retry_scheduled", :retry_attempt_not_increasing)

    missing_intent_path = ledger_path()
    append_complete_run!(missing_intent_path, "run-complete", "issue-complete")

    assert :ok =
             append_retry!(
               missing_intent_path,
               "run-complete",
               "issue-complete",
               "DUD-SEMANTIC-TAIL",
               1,
               2,
               "continuation"
             )

    assert_sequence_error(missing_intent_path, 4, "retry_scheduled", :missing_terminal_retry_intent)

    interrupted_path = ledger_path()
    assert :ok = append_claim!(interrupted_path, "run-stale", "issue-stale", "DUD-STALE", 1)
    assert :ok = append_started!(interrupted_path, "run-stale", "issue-stale", "DUD-STALE", 1)

    assert :ok =
             RunLedger.append(interrupted_path, %{
               transition: "run_interrupted",
               stage: "released",
               run_id: "run-stale",
               issue_id: "issue-stale",
               issue_identifier: "DUD-STALE",
               attempt: 1,
               terminal_reason: "runner_restarted"
             })

    assert {:ok, [_claim, _started, _interrupted]} = RunLedger.read_events(interrupted_path)
  end

  test "requires cleanup intent and accepts only idempotent cleanup duplicates" do
    missing_intent_path = ledger_path()
    append_complete_run!(missing_intent_path, "run-complete", "issue-complete")

    assert :ok =
             append_cleanup_requested!(
               missing_intent_path,
               "run-complete",
               "issue-complete",
               "DUD-SEMANTIC-TAIL",
               1
             )

    assert_sequence_error(
      missing_intent_path,
      4,
      "workspace_cleanup_requested",
      :missing_cleanup_intent
    )

    cleanup_path = ledger_path()
    append_cleanup_run!(cleanup_path)
    cleanup_requested = cleanup_requested_event()
    cleanup_io_started = cleanup_lifecycle_event("workspace_cleanup_io_started")
    cleanup_io_completed = cleanup_lifecycle_event("workspace_cleanup_io_completed")
    cleanup_completed = cleanup_completed_event()

    assert :ok = RunLedger.append(cleanup_path, cleanup_requested)
    assert :ok = RunLedger.append(cleanup_path, cleanup_requested)
    assert :ok = RunLedger.append(cleanup_path, cleanup_io_started)
    assert :ok = RunLedger.append(cleanup_path, cleanup_io_completed)
    assert :ok = RunLedger.append(cleanup_path, cleanup_completed)
    assert :ok = RunLedger.append(cleanup_path, cleanup_completed)
    assert {:ok, _events} = RunLedger.read_events(cleanup_path)

    assert {:ok, recovery} = RunLedger.reconcile_startup(cleanup_path, "runner-new")
    assert recovery.cleanup_pending == %{}
  end

  test "cleanup I/O lifecycle recovers fail-closed and requires an explicit retry" do
    path = ledger_path()
    append_cleanup_run!(path)

    assert :ok = RunLedger.append(path, cleanup_requested_event())
    assert :ok = RunLedger.append(path, cleanup_lifecycle_event("workspace_cleanup_io_started"))

    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-after-start")

    assert recovery.cleanup_pending["issue-cleanup"]["cleanup_status"] ==
             "operator_required"

    operator_required =
      cleanup_lifecycle_event("workspace_cleanup_operator_required")
      |> Map.put(:cleanup_error, "workspace_preservation_required")

    assert :ok = RunLedger.append(path, operator_required)
    assert :ok = RunLedger.append(path, cleanup_lifecycle_event("workspace_cleanup_retry_requested"))

    assert {:ok, retry_recovery} = RunLedger.reconcile_startup(path, "runner-after-retry")
    assert retry_recovery.cleanup_pending["issue-cleanup"]["cleanup_status"] == "cleanup_pending"

    assert :ok = RunLedger.append(path, cleanup_lifecycle_event("workspace_cleanup_io_started"))
    assert :ok = RunLedger.append(path, cleanup_lifecycle_event("workspace_cleanup_io_completed"))

    assert {:ok, completed_io_recovery} =
             RunLedger.reconcile_startup(path, "runner-after-io-completed")

    assert completed_io_recovery.cleanup_pending["issue-cleanup"]["cleanup_status"] ==
             "completion_pending"
  end

  test "cleanup lifecycle rejects automatic I/O replay and unbounded error values" do
    replay_path = ledger_path()
    append_cleanup_run!(replay_path)
    assert :ok = RunLedger.append(replay_path, cleanup_requested_event())
    assert :ok = RunLedger.append(replay_path, cleanup_lifecycle_event("workspace_cleanup_io_started"))
    assert :ok = RunLedger.append(replay_path, cleanup_lifecycle_event("workspace_cleanup_io_started"))

    assert_sequence_error(
      replay_path,
      6,
      "workspace_cleanup_io_started",
      :invalid_cleanup_lifecycle
    )

    premature_completion_path = ledger_path()
    append_cleanup_run!(premature_completion_path)
    assert :ok = RunLedger.append(premature_completion_path, cleanup_requested_event())

    assert :ok =
             RunLedger.append(
               premature_completion_path,
               cleanup_lifecycle_event("workspace_cleanup_io_started")
             )

    assert :ok = RunLedger.append(premature_completion_path, cleanup_completed_event())

    assert_sequence_error(
      premature_completion_path,
      6,
      "workspace_cleanup_completed",
      :cleanup_io_not_completed
    )

    failed_io_path = ledger_path()
    append_cleanup_run!(failed_io_path)
    assert :ok = RunLedger.append(failed_io_path, cleanup_requested_event())
    assert :ok = RunLedger.append(failed_io_path, cleanup_lifecycle_event("workspace_cleanup_io_started"))

    assert :ok =
             RunLedger.append(
               failed_io_path,
               cleanup_lifecycle_event("workspace_cleanup_operator_required")
               |> Map.put(:cleanup_error, "workspace_preservation_required")
             )

    assert :ok = RunLedger.append(failed_io_path, cleanup_lifecycle_event("workspace_cleanup_io_completed"))

    assert_sequence_error(
      failed_io_path,
      7,
      "workspace_cleanup_io_completed",
      :invalid_cleanup_lifecycle
    )

    error_path = ledger_path()
    append_cleanup_run!(error_path)
    assert :ok = RunLedger.append(error_path, cleanup_requested_event())
    assert :ok = RunLedger.append(error_path, cleanup_lifecycle_event("workspace_cleanup_io_started"))

    assert {:error, {:invalid_field, "cleanup_error"}} =
             RunLedger.append(
               error_path,
               cleanup_lifecycle_event("workspace_cleanup_operator_required")
               |> Map.put(:cleanup_error, "raw exception with secrets")
             )
  end

  test "accepts a legacy cleanup completion without lifecycle transitions" do
    path = ledger_path()
    append_cleanup_run!(path)

    assert :ok = RunLedger.append(path, cleanup_completed_event())
    assert {:ok, _events} = RunLedger.read_events(path)
    assert {:ok, recovery} = RunLedger.reconcile_startup(path, "runner-after-legacy-cleanup")
    assert recovery.cleanup_pending == %{}
  end

  test "rejects missing waits and claimed dispatch affinity mismatches" do
    missing_wait_path = ledger_path()

    assert :ok =
             RunLedger.append(missing_wait_path, %{
               transition: "wait_rejected",
               stage: "parked",
               run_id: "run-missing",
               issue_id: "issue-missing",
               issue_identifier: "DUD-MISSING",
               attempt: 1,
               wait_id: "wait-missing",
               parked_reason: "waiting_owner",
               allowed_actions: ["approve", "reject"]
             })

    assert_sequence_error(missing_wait_path, 1, "wait_rejected", :missing_parked_wait)

    dispatch_path = ledger_path()

    assert :ok =
             append_claim!(dispatch_path, "run-old", "issue-dispatch", "DUD-DISPATCH", 1, worker_host: "worker-a")

    assert :ok = append_started!(dispatch_path, "run-old", "issue-dispatch", "DUD-DISPATCH", 1)

    assert :ok =
             RunLedger.append(dispatch_path, %{
               transition: "run_failed",
               stage: "released",
               run_id: "run-old",
               issue_id: "issue-dispatch",
               issue_identifier: "DUD-DISPATCH",
               attempt: 1,
               terminal_reason: "worker_exit",
               next_action: "retry",
               next_attempt: 2,
               worker_host: "worker-a"
             })

    assert :ok =
             append_claim!(dispatch_path, "run-new", "issue-dispatch", "DUD-DISPATCH", 2, worker_host: "worker-b")

    assert_sequence_error(dispatch_path, 4, "run_claimed", :dispatch_identity_mismatch)
  end

  test "rejects sequences that could create competing recovery entries" do
    duplicate_resume_path = ledger_path()
    append_parked_run!(duplicate_resume_path, "run-parked", "issue-parked")

    resume_event = %{
      transition: "wait_resumed",
      stage: "resume_queued",
      run_id: "run-parked",
      issue_id: "issue-parked",
      issue_identifier: "DUD-SEMANTIC-MIDDLE",
      attempt: 2,
      wait_id: "wait-semantic-middle",
      parked_reason: "waiting_owner",
      allowed_actions: ["approve", "reject"]
    }

    assert :ok = RunLedger.append(duplicate_resume_path, resume_event)
    assert :ok = RunLedger.append(duplicate_resume_path, %{resume_event | transition: "resume_queued"})
    assert_sequence_error(duplicate_resume_path, 5, "resume_queued", :missing_parked_wait)

    duplicate_terminal_path = ledger_path()

    assert :ok =
             append_claim!(
               duplicate_terminal_path,
               "run-stale",
               "issue-stale",
               "DUD-STALE",
               1
             )

    assert :ok =
             append_started!(
               duplicate_terminal_path,
               "run-stale",
               "issue-stale",
               "DUD-STALE",
               1
             )

    interrupted_event = %{
      transition: "run_interrupted",
      stage: "released",
      run_id: "run-stale",
      issue_id: "issue-stale",
      issue_identifier: "DUD-STALE",
      attempt: 1,
      terminal_reason: "runner_restarted"
    }

    assert :ok = RunLedger.append(duplicate_terminal_path, interrupted_event)
    assert :ok = RunLedger.append(duplicate_terminal_path, interrupted_event)
    assert_sequence_error(duplicate_terminal_path, 4, "run_interrupted", :illegal_predecessor)

    parallel_run_path = ledger_path()
    assert :ok = append_claim!(parallel_run_path, "run-1", "issue-shared", "DUD-SHARED", 1)
    assert :ok = append_started!(parallel_run_path, "run-1", "issue-shared", "DUD-SHARED", 1)
    assert :ok = append_claim!(parallel_run_path, "run-2", "issue-shared", "DUD-SHARED", 2)
    assert_sequence_error(parallel_run_path, 3, "run_claimed", :issue_already_active)
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

  defp append_context_operator_action!(path, tag) do
    run_id = "run-operator-#{tag}"
    issue_id = "issue-operator-#{tag}"
    append_parked_run!(path, run_id, issue_id)

    action =
      run_id
      |> valid_parked_event(issue_id)
      |> Map.merge(%{
        transition: "wait_rejected",
        comment_id: "comment-operator-#{tag}",
        comment_created_at: "2026-08-03T10:00:00.000Z",
        operator_command: "reject"
      })

    assert :ok = RunLedger.append(path, action)
    action
  end

  defp operator_outcome_event(action, transition) do
    %{
      transition: transition,
      stage: "operator",
      issue_id: action.issue_id,
      comment_id: action.comment_id,
      comment_created_at: action.comment_created_at,
      operator_command: action.operator_command,
      runner_generation: "runner-operator-validation"
    }
  end

  defp maybe_override_operator_outcome(event, :transition, _value), do: event
  defp maybe_override_operator_outcome(event, field, value), do: Map.put(event, field, value)

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

  defp append_retry!(path, run_id, issue_id, issue_identifier, attempt, next_attempt, action \\ "retry") do
    RunLedger.append(path, %{
      transition: "retry_scheduled",
      stage: "retry_queued",
      run_id: run_id,
      issue_id: issue_id,
      issue_identifier: issue_identifier,
      attempt: attempt,
      next_action: action,
      next_attempt: next_attempt
    })
  end

  defp append_cleanup_run!(path) do
    assert :ok =
             append_claim!(path, "run-cleanup", "issue-cleanup", "DUD-CLEANUP", 1,
               worker_host: "worker-a",
               workspace_path: "/tmp/DUD-CLEANUP",
               workspace_root: "/tmp"
             )

    assert :ok = append_started!(path, "run-cleanup", "issue-cleanup", "DUD-CLEANUP", 1)

    assert :ok =
             RunLedger.append(path, %{
               transition: "run_stopped",
               stage: "released",
               run_id: "run-cleanup",
               issue_id: "issue-cleanup",
               issue_identifier: "DUD-CLEANUP",
               attempt: 1,
               terminal_reason: "tracker_terminal",
               worker_host: "worker-a",
               workspace_path: "/tmp/DUD-CLEANUP",
               workspace_root: "/tmp"
             })
  end

  defp append_cleanup_requested!(path, run_id, issue_id, issue_identifier, attempt) do
    RunLedger.append(path, %{
      transition: "workspace_cleanup_requested",
      stage: "cleanup",
      run_id: run_id,
      issue_id: issue_id,
      issue_identifier: issue_identifier,
      attempt: attempt,
      terminal_reason: "tracker_terminal"
    })
  end

  defp cleanup_requested_event do
    %{
      transition: "workspace_cleanup_requested",
      stage: "cleanup",
      run_id: "run-cleanup",
      issue_id: "issue-cleanup",
      issue_identifier: "DUD-CLEANUP",
      attempt: 1,
      terminal_reason: "tracker_terminal",
      worker_host: "worker-a",
      workspace_path: "/tmp/DUD-CLEANUP",
      workspace_root: "/tmp"
    }
  end

  defp cleanup_completed_event do
    %{
      transition: "workspace_cleanup_completed",
      stage: "cleanup",
      run_id: "run-cleanup",
      issue_id: "issue-cleanup",
      issue_identifier: "DUD-CLEANUP",
      attempt: 1,
      worker_host: "worker-a",
      workspace_path: "/tmp/DUD-CLEANUP",
      workspace_root: "/tmp"
    }
  end

  defp cleanup_lifecycle_event(transition) do
    %{
      transition: transition,
      stage: "cleanup",
      run_id: "run-cleanup",
      issue_id: "issue-cleanup",
      issue_identifier: "DUD-CLEANUP",
      attempt: 1,
      worker_host: "worker-a",
      workspace_path: "/tmp/DUD-CLEANUP",
      workspace_root: "/tmp"
    }
  end

  defp assert_sequence_error(path, line, transition, reason) do
    assert {:error, {:invalid_ledger_record, ^line, {:invalid_transition_sequence, ^transition, ^reason}}} =
             RunLedger.read_events(path)
  end
end
