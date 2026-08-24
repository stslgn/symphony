defmodule SymphonyElixir.MergeLaneTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.MergeLane

  test "claims and restores one durable session merge lane" do
    path = ledger_path()

    assert {:ok, claim} = MergeLane.claim(path, lane_binding())
    assert claim.state == "claimed"
    assert claim.channel == "session"
    assert claim.fencing_token == 1
    assert claim.version == 1
    assert is_binary(claim.claim_id)

    assert {:ok, restored} = MergeLane.active_claim(path, lane_binding())
    assert restored == claim
  end

  test "session claims may omit a wait while retaining the exact issue and PR binding" do
    path = ledger_path()

    assert {:ok, claim} = MergeLane.claim(path, %{lane_binding() | wait_id: nil})
    assert claim.wait_id == nil
    assert claim.issue_id == "issue-1"
    assert claim.repository == "stslgn/example"
    assert claim.pull_request == 23

    assert {:ok, restored} =
             MergeLane.active_claim(path, %{lane_binding() | wait_id: nil})

    assert restored == claim
  end

  test "simultaneous session and runner claims produce one owner and one durable conflict" do
    path = ledger_path()
    parent = self()

    tasks =
      for channel <- ["session", "runner"] do
        Task.async(fn ->
          send(parent, {:ready, self()})

          receive do
            :go -> MergeLane.claim(path, %{lane_binding() | channel: channel, executor: channel})
          end
        end)
      end

    task_pids =
      for _index <- 1..2 do
        assert_receive {:ready, pid}
        pid
      end

    Enum.each(task_pids, &send(&1, :go))
    results = Enum.map(tasks, &Task.await(&1, 5_000))

    assert [{:ok, owner}] = Enum.filter(results, &match?({:ok, _claim}, &1))

    assert [{:error, {:claim_conflict, claim_id}}] =
             Enum.filter(results, &match?({:error, {:claim_conflict, _claim_id}}, &1))

    assert claim_id == owner.claim_id
    assert {:ok, events} = MergeLane.history(path)
    assert Enum.map(events, & &1["transition"]) == ["claimed", "claim_conflict"]
    assert List.last(events)["claim_id"] == owner.claim_id
  end

  test "only the current fencing token can start merge execution" do
    path = ledger_path()
    assert {:ok, claim} = MergeLane.claim(path, lane_binding())

    assert {:error, :stale_fencing_token} =
             MergeLane.transition(path, claim.claim_id, claim.fencing_token - 1, "executing")

    assert {:ok, executing} =
             MergeLane.transition(path, claim.claim_id, claim.fencing_token, "executing")

    assert executing.state == "executing"
    assert executing.version == 2
    assert {:ok, events} = MergeLane.history(path)
    assert Enum.map(events, & &1["transition"]) == ["claimed", "executing"]
  end

  test "ambiguous provider recovery retains ownership and raises the fence" do
    path = ledger_path()
    evidence_sha256 = String.duplicate("c", 64)
    assert {:ok, claim} = MergeLane.claim(path, lane_binding())

    assert {:ok, executing} =
             MergeLane.transition(path, claim.claim_id, claim.fencing_token, "executing")

    assert {:ok, uncertain} =
             MergeLane.transition(
               path,
               claim.claim_id,
               executing.fencing_token,
               "merge_uncertain",
               %{evidence_sha256: evidence_sha256}
             )

    assert {:ok, retained} =
             MergeLane.recover(
               path,
               claim.claim_id,
               uncertain.fencing_token,
               "reconcile",
               %{result: "ambiguous", evidence_sha256: evidence_sha256}
             )

    assert retained.state == "operator_required"
    assert retained.fencing_token == claim.fencing_token + 1
    assert retained.executor == claim.executor

    assert {:error, :stale_fencing_token} =
             MergeLane.transition(path, claim.claim_id, claim.fencing_token, "executing")

    assert {:ok, observed} =
             MergeLane.recover(
               path,
               claim.claim_id,
               retained.fencing_token,
               "reconcile",
               %{
                 result: "merged",
                 evidence_sha256: evidence_sha256,
                 observed_base_ref: "main",
                 observed_head_sha: String.duplicate("a", 40)
               }
             )

    assert observed.state == "merge_observed"
    assert observed.fencing_token == claim.fencing_token + 2
  end

  test "successful merge acceptance consumes the claim before a new fence is allocated" do
    path = ledger_path()

    evidence = %{
      evidence_sha256: String.duplicate("d", 64),
      observed_base_ref: "main",
      observed_head_sha: String.duplicate("a", 40)
    }

    assert {:ok, claim} = MergeLane.claim(path, lane_binding())
    assert {:ok, executing} = MergeLane.transition(path, claim.claim_id, 1, "executing")

    assert {:ok, observed} =
             MergeLane.transition(path, claim.claim_id, 1, "merge_observed", evidence)

    assert {:ok, acceptance} =
             MergeLane.transition(
               path,
               claim.claim_id,
               1,
               "acceptance_pending",
               %{evidence_sha256: String.duplicate("e", 64)}
             )

    assert {:ok, completed} =
             MergeLane.transition(
               path,
               claim.claim_id,
               1,
               "completed",
               %{evidence_sha256: String.duplicate("f", 64)}
             )

    assert executing.state == "executing"
    assert observed.state == "merge_observed"
    assert acceptance.state == "acceptance_pending"
    assert completed.state == "completed"
    assert {:ok, nil} = MergeLane.active_claim(path, lane_binding())
    assert {:error, :claim_terminal} = MergeLane.transition(path, claim.claim_id, 1, "executing")

    assert {:ok, next_claim} = MergeLane.claim(path, lane_binding())
    assert next_claim.fencing_token == 2
  end

  test "base or head drift cannot be recorded as the approved merged target" do
    path = ledger_path()
    evidence_sha256 = String.duplicate("1", 64)
    assert {:ok, claim} = MergeLane.claim(path, lane_binding())
    assert {:ok, _executing} = MergeLane.transition(path, claim.claim_id, 1, "executing")

    assert {:error, :approved_target_mismatch} =
             MergeLane.transition(path, claim.claim_id, 1, "merge_observed", %{
               evidence_sha256: evidence_sha256,
               observed_base_ref: "release",
               observed_head_sha: String.duplicate("a", 40)
             })

    assert {:ok, invalidated} =
             MergeLane.transition(path, claim.claim_id, 1, "invalidated", %{
               evidence_sha256: evidence_sha256,
               result: "base_ref_mismatch",
               observed_base_ref: "release",
               observed_head_sha: String.duplicate("a", 40)
             })

    assert invalidated.state == "invalidated"
    assert {:ok, nil} = MergeLane.active_claim(path, lane_binding())
    assert {:ok, events} = MergeLane.history(path)
    assert Enum.map(events, & &1["transition"]) == ["claimed", "executing", "invalidated"]
  end

  test "runner approval loses to a session claim and fails closed without a binding" do
    path = ledger_path()

    assert {:error, :runner_merge_claim_required} =
             MergeLane.guard_runner_approval(path, "issue-1", "wait-1", "runner-1")

    assert {:ok, claim} = MergeLane.claim(path, lane_binding())

    assert {:error, {:claim_conflict, claim_id}} =
             MergeLane.guard_runner_approval(path, "issue-1", "wait-1", "runner-1")

    assert claim_id == claim.claim_id
    assert {:ok, events} = MergeLane.history(path)
    assert Enum.map(events, & &1["transition"]) == ["claimed", "claim_conflict"]
    assert List.last(events)["conflicting_channel"] == "runner"
  end

  test "runner approval validates an already claimed exact runner lane" do
    path = ledger_path()
    runner_generation = String.duplicate("c", 64)

    binding = %{
      lane_binding()
      | channel: "runner",
        executor: runner_generation,
        workflow_generation: runner_generation
    }

    assert {:ok, claim} = MergeLane.claim(path, binding)

    assert :ok =
             MergeLane.guard_runner_approval(
               path,
               claim.issue_id,
               claim.wait_id,
               claim.executor
             )

    assert {:ok, [event]} = MergeLane.history(path)
    assert event["transition"] == "claimed"
  end

  test "uncertain merge abort requires durable no-provider-side-effect evidence" do
    path = ledger_path()
    assert {:ok, claim} = MergeLane.claim(path, lane_binding())
    assert {:ok, executing} = MergeLane.transition(path, claim.claim_id, 1, "executing")

    assert {:ok, uncertain} =
             MergeLane.transition(path, claim.claim_id, 1, "merge_uncertain", %{
               evidence_sha256: String.duplicate("2", 64)
             })

    assert {:error, :invalid_recovery} =
             MergeLane.recover(path, claim.claim_id, 1, "abort", %{
               result: "ambiguous",
               evidence_sha256: String.duplicate("3", 64)
             })

    assert {:ok, aborted} =
             MergeLane.recover(path, claim.claim_id, uncertain.fencing_token, "abort", %{
               result: "no_provider_side_effect",
               evidence_sha256: String.duplicate("4", 64)
             })

    assert executing.state == "executing"
    assert aborted.state == "aborted_no_side_effect"
    assert aborted.fencing_token == 2

    assert {:error, :invalid_recovery} =
             MergeLane.recover(path, claim.claim_id, aborted.fencing_token, "abort", %{
               result: "no_provider_side_effect",
               evidence_sha256: String.duplicate("5", 64)
             })
  end

  test "workflow drift stays fenced until an exact same-owner resume" do
    path = ledger_path()
    assert {:ok, claim} = MergeLane.claim(path, lane_binding())

    assert {:ok, retained} =
             MergeLane.transition(path, claim.claim_id, 1, "operator_required", %{
               evidence_sha256: String.duplicate("6", 64),
               reason: "workflow_generation_drift"
             })

    assert retained.state == "operator_required"
    assert retained.operator_source_state == "claimed"

    assert {:error, :invalid_recovery} =
             MergeLane.recover(path, claim.claim_id, 1, "resume", %{
               result: "no_provider_side_effect",
               evidence_sha256: String.duplicate("7", 64),
               observed_base_ref: "release",
               observed_head_sha: String.duplicate("a", 40),
               observed_workflow_generation: String.duplicate("b", 64)
             })

    assert {:ok, resumed} =
             MergeLane.recover(path, claim.claim_id, 1, "resume", %{
               result: "no_provider_side_effect",
               evidence_sha256: String.duplicate("8", 64),
               observed_base_ref: "main",
               observed_head_sha: String.duplicate("a", 40),
               observed_workflow_generation: String.duplicate("b", 64)
             })

    assert resumed.state == "claimed"
    assert resumed.operator_source_state == nil
    assert resumed.fencing_token == 2
    assert resumed.executor == claim.executor
  end

  test "pre-provider abort is terminal only with no-side-effect proof" do
    path = ledger_path()
    assert {:ok, claim} = MergeLane.claim(path, lane_binding())

    assert {:error, :invalid_evidence} =
             MergeLane.transition(path, claim.claim_id, 1, "aborted_no_side_effect", %{})

    assert {:ok, aborted} =
             MergeLane.transition(path, claim.claim_id, 1, "aborted_no_side_effect", %{
               result: "no_provider_side_effect",
               evidence_sha256: String.duplicate("9", 64)
             })

    assert aborted.state == "aborted_no_side_effect"
    assert {:ok, nil} = MergeLane.active_claim(path, lane_binding())
  end

  test "tampered ledger sequence blocks recovery and all later claims" do
    path = ledger_path()
    assert {:ok, claim} = MergeLane.claim(path, lane_binding())
    assert {:ok, [event]} = MergeLane.history(path)

    tampered =
      event
      |> Map.put("event_id", "merge_evt_tampered")
      |> Map.put("transition", "claim_conflict")
      |> Map.put("previous_state", "claimed")
      |> Map.put("previous_version", 99)
      |> Map.put("version", 2)

    File.write!(path, Jason.encode!(tampered) <> "\n", [:append, :sync])

    assert {:error, :invalid_merge_lane_ledger} = MergeLane.history(path)
    assert {:error, :invalid_merge_lane_ledger} = MergeLane.claim(path, lane_binding())
    assert claim.state == "claimed"
  end

  test "tampered recovery transitions outside the closed table are rejected" do
    path = ledger_path()
    assert {:ok, _claim} = MergeLane.claim(path, lane_binding())
    assert {:ok, [event]} = MergeLane.history(path)

    tampered =
      event
      |> Map.put("event_id", "merge_evt_bad_recovery")
      |> Map.put("transition", "executing")
      |> Map.put("state", "executing")
      |> Map.put("previous_state", "claimed")
      |> Map.put("previous_version", 1)
      |> Map.put("version", 2)
      |> Map.put("fencing_token", 2)
      |> Map.put("outcome", "recovery_applied")

    File.write!(path, Jason.encode!(tampered) <> "\n", [:append, :sync])
    assert {:error, :invalid_merge_lane_ledger} = MergeLane.history(path)
  end

  test "claim bindings reject controls, invalid repositories, and oversized identifiers" do
    invalid = [
      %{lane_binding() | executor: "session\nforged"},
      %{lane_binding() | repository: "https://github.com/stslgn/example"},
      %{lane_binding() | base_ref: "refs/heads/main..release"},
      %{lane_binding() | issue_id: String.duplicate("i", 129)}
    ]

    Enum.each(invalid, fn binding ->
      path = ledger_path()
      assert {:error, :invalid_claim_binding} = MergeLane.claim(path, binding)
      refute File.exists?(path)
    end)
  end

  test "public API rejects invalid types, missing claims, and illegal transitions" do
    assert String.ends_with?(MergeLane.default_path(), "merge-lane-ledger.jsonl")
    assert {:error, :invalid_claim} = MergeLane.claim(nil, %{})
    assert {:error, :invalid_claim} = MergeLane.active_claim(nil, %{})
    assert {:error, :invalid_runner_approval} = MergeLane.guard_runner_approval(nil, nil, nil, nil)
    assert {:error, :invalid_transition} = MergeLane.transition(nil, nil, nil, nil)
    assert {:error, :invalid_transition} = MergeLane.transition(nil, nil, nil, nil, nil)
    assert {:error, :invalid_recovery} = MergeLane.recover(nil, nil, nil, nil, nil)
    assert {:error, :invalid_claim} = MergeLane.history(nil)

    path = ledger_path()
    assert {:ok, claim} = MergeLane.claim(path, lane_binding())
    assert {:error, :claim_not_found} = MergeLane.transition(path, "missing", 1, "executing")
    assert {:error, :invalid_transition} = MergeLane.transition(path, claim.claim_id, 1, "completed")
  end

  test "the repository and pull request identity conflict even for another issue" do
    path = ledger_path()
    assert {:ok, owner} = MergeLane.claim(path, lane_binding())

    contender = %{
      lane_binding()
      | issue_id: "issue-2",
        issue_identifier: "DUD-2",
        wait_id: "wait-2",
        executor: "codex-session-2"
    }

    assert {:error, {:claim_conflict, claim_id}} = MergeLane.claim(path, contender)
    assert claim_id == owner.claim_id
  end

  test "remaining terminal and retained lifecycle branches are closed" do
    path = ledger_path()
    assert {:ok, claim} = MergeLane.claim(path, lane_binding())
    assert {:ok, _executing} = MergeLane.transition(path, claim.claim_id, 1, "executing")

    assert {:ok, retained} =
             MergeLane.transition(path, claim.claim_id, 1, "operator_required", %{
               evidence_sha256: String.duplicate("a", 64),
               reason: "provider_ambiguity"
             })

    assert retained.operator_source_state == "executing"

    assert {:ok, resumed} =
             MergeLane.recover(path, claim.claim_id, 1, "resume", %{
               result: "no_provider_side_effect",
               evidence_sha256: String.duplicate("b", 64),
               observed_base_ref: "main",
               observed_head_sha: String.duplicate("a", 40),
               observed_workflow_generation: String.duplicate("b", 64)
             })

    assert resumed.state == "executing"

    assert {:ok, aborted} =
             MergeLane.transition(path, claim.claim_id, 2, "aborted_no_side_effect", %{
               result: "no_provider_side_effect",
               evidence_sha256: String.duplicate("c", 64)
             })

    assert aborted.state == "aborted_no_side_effect"

    acceptance_path = ledger_path()
    assert {:ok, acceptance_claim} = MergeLane.claim(acceptance_path, lane_binding())
    assert {:ok, _} = MergeLane.transition(acceptance_path, acceptance_claim.claim_id, 1, "executing")

    assert {:ok, _} =
             MergeLane.transition(acceptance_path, acceptance_claim.claim_id, 1, "merge_observed", %{
               evidence_sha256: String.duplicate("d", 64),
               observed_base_ref: "main",
               observed_head_sha: String.duplicate("a", 40)
             })

    assert {:ok, _} =
             MergeLane.transition(acceptance_path, acceptance_claim.claim_id, 1, "acceptance_pending", %{
               evidence_sha256: String.duplicate("e", 64)
             })

    assert {:ok, failed} =
             MergeLane.transition(acceptance_path, acceptance_claim.claim_id, 1, "acceptance_failed", %{
               evidence_sha256: String.duplicate("f", 64)
             })

    assert failed.state == "acceptance_failed"

    interrupted_path = ledger_path()
    assert {:ok, interrupted_claim} = MergeLane.claim(interrupted_path, lane_binding())
    assert {:ok, _} = MergeLane.transition(interrupted_path, interrupted_claim.claim_id, 1, "executing")

    assert {:error, :approved_target_mismatch} =
             MergeLane.transition(interrupted_path, interrupted_claim.claim_id, 1, "merge_observed", %{
               evidence_sha256: String.duplicate("1", 64),
               observed_base_ref: nil,
               observed_head_sha: String.duplicate("a", 40)
             })

    assert {:error, :invalid_evidence} =
             MergeLane.transition(interrupted_path, interrupted_claim.claim_id, 1, "merge_observed", %{
               evidence_sha256: "invalid",
               observed_base_ref: "main",
               observed_head_sha: String.duplicate("a", 40)
             })

    assert {:ok, _} =
             MergeLane.transition(interrupted_path, interrupted_claim.claim_id, 1, "merge_observed", %{
               evidence_sha256: String.duplicate("2", 64),
               observed_base_ref: "main",
               observed_head_sha: String.duplicate("a", 40)
             })

    assert {:ok, _} =
             MergeLane.transition(interrupted_path, interrupted_claim.claim_id, 1, "acceptance_pending", %{
               evidence_sha256: String.duplicate("3", 64)
             })

    assert {:ok, interrupted} =
             MergeLane.transition(interrupted_path, interrupted_claim.claim_id, 1, "operator_required", %{
               evidence_sha256: String.duplicate("4", 64),
               reason: "acceptance_interrupted"
             })

    assert interrupted.operator_source_state == "acceptance_pending"
  end

  test "empty, malformed, unreadable, locked, and unwritable ledgers fail closed" do
    empty_path = ledger_path()
    File.write!(empty_path, "")
    assert {:ok, []} = MergeLane.history(empty_path)

    malformed_path = ledger_path()
    File.write!(malformed_path, "not-json\n")
    assert {:error, :invalid_merge_lane_ledger} = MergeLane.history(malformed_path)

    directory_path = ledger_path()
    File.mkdir_p!(directory_path)
    assert {:error, {:ledger_read_failed, _reason}} = MergeLane.history(directory_path)
    assert {:error, {:ledger_read_failed, _reason}} = MergeLane.claim(directory_path, lane_binding())

    locked_path = ledger_path()
    File.mkdir_p!(locked_path <> ".lock")
    assert {:error, :operation_locked} = MergeLane.claim(locked_path, lane_binding())

    read_only_root = ledger_path() <> "-read-only"
    File.mkdir_p!(read_only_root)
    File.chmod!(read_only_root, 0o500)

    on_exit(fn ->
      File.chmod!(read_only_root, 0o700)
      File.rm_rf!(read_only_root)
    end)

    read_only_path = Path.join(read_only_root, "ledger.jsonl")
    assert {:error, {:operation_lock_failed, _reason}} = MergeLane.claim(read_only_path, lane_binding())
  end

  test "typed binding and timestamp errors fail ledger validation" do
    invalid_bindings = [
      %{lane_binding() | pull_request: "23"},
      %{lane_binding() | channel: "other"}
    ]

    Enum.each(invalid_bindings, fn binding ->
      assert {:error, :invalid_claim_binding} = MergeLane.claim(ledger_path(), binding)
    end)

    path = ledger_path()
    assert {:ok, _claim} = MergeLane.claim(path, lane_binding())
    assert {:ok, [event]} = MergeLane.history(path)
    File.write!(path, Jason.encode!(%{event | "occurred_at" => nil}) <> "\n")
    assert {:error, :invalid_merge_lane_ledger} = MergeLane.history(path)
  end

  defp ledger_path do
    suffix = :crypto.strong_rand_bytes(8) |> Base.url_encode64(padding: false)

    Path.join(
      System.tmp_dir!(),
      "symphony-merge-lane-#{suffix}.jsonl"
    )
  end

  defp lane_binding do
    %{
      issue_id: "issue-1",
      issue_identifier: "DUD-1",
      wait_id: "wait-1",
      repository: "stslgn/example",
      pull_request: 23,
      base_ref: "main",
      head_sha: String.duplicate("a", 40),
      channel: "session",
      executor: "codex-session-1",
      workflow_generation: String.duplicate("b", 64)
    }
  end
end
