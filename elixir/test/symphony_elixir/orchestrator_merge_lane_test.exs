defmodule SymphonyElixir.OrchestratorMergeLaneTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{MergeLane, OperatorWait, Orchestrator}

  test "native Human Review approval cannot resume while the session lane is claimed" do
    root = temp_root()
    run_ledger_path = Path.join(root, "run-ledger.jsonl")
    merge_lane_path = Path.join(root, "merge-lane-ledger.jsonl")

    assert {:ok, wait} =
             OperatorWait.new("waiting_owner", %{
               wait_id: "wait-1",
               issue_id: "issue-1",
               identifier: "DUD-1",
               run_id: "run-1",
               tracker_state: "Human Review"
             })

    assert {:ok, claim} = MergeLane.claim(merge_lane_path, lane_binding())

    state = %Orchestrator.State{
      run_ledger_path: run_ledger_path,
      runner_generation: "runner-1",
      operator_commands: %Orchestrator.OperatorCommandState{
        workflow_generation: String.duplicate("c", 64)
      },
      parked: %{wait.issue_id => wait},
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    assert {:reply, {:error, {:claim_conflict, claim_id}}, unchanged} =
             Orchestrator.handle_call(
               {:resolve_wait, wait.issue_id, wait.wait_id, "approve"},
               {self(), make_ref()},
               state
             )

    assert claim_id == claim.claim_id
    assert unchanged.parked[wait.issue_id].wait_id == wait.wait_id
    assert unchanged.queued_resumes == %{}
  end

  test "native Human Review approval resumes only with its preclaimed runner lane" do
    root = temp_root()
    run_ledger_path = Path.join(root, "run-ledger.jsonl")
    merge_lane_path = Path.join(root, "merge-lane-ledger.jsonl")
    runner_generation = SymphonyElixir.RunLedger.new_id("runner")
    workflow_generation = String.duplicate("c", 64)

    assert {:ok, wait} =
             OperatorWait.new("waiting_owner", %{
               wait_id: "wait-1",
               issue_id: "issue-1",
               identifier: "DUD-1",
               run_id: "run-1",
               tracker_state: "Human Review"
             })

    runner_binding = %{
      lane_binding()
      | channel: "runner",
        executor: runner_generation,
        workflow_generation: workflow_generation
    }

    assert {:ok, _claim} = MergeLane.claim(merge_lane_path, runner_binding)

    state = %Orchestrator.State{
      run_ledger_path: run_ledger_path,
      runner_generation: runner_generation,
      operator_commands: %Orchestrator.OperatorCommandState{
        workflow_generation: workflow_generation
      },
      parked: %{wait.issue_id => wait},
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    assert {:reply, {:ok, %{action: "approve", resumed: true}}, resumed} =
             Orchestrator.handle_call(
               {:resolve_wait, wait.issue_id, wait.wait_id, "approve"},
               {self(), make_ref()},
               state
             )

    refute Map.has_key?(resumed.parked, wait.issue_id)
    assert resumed.queued_resumes[wait.issue_id].wait_id == wait.wait_id
  end

  test "native Human Review approval fails closed when runtime identities are unavailable" do
    root = temp_root()

    assert {:ok, wait} =
             OperatorWait.new("waiting_owner", %{
               wait_id: "wait-1",
               issue_id: "issue-1",
               identifier: "DUD-1",
               run_id: "run-1",
               tracker_state: "Human Review"
             })

    state = %Orchestrator.State{
      run_ledger_path: Path.join(root, "run-ledger.jsonl"),
      runner_generation: "runner-generation",
      operator_commands: %Orchestrator.OperatorCommandState{workflow_generation: nil},
      parked: %{wait.issue_id => wait},
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0}
    }

    assert {:reply, {:error, :runner_merge_identity_unavailable}, unchanged} =
             Orchestrator.handle_call(
               {:resolve_wait, wait.issue_id, wait.wait_id, "approve"},
               {self(), make_ref()},
               state
             )

    assert unchanged.parked[wait.issue_id].wait_id == wait.wait_id
    assert unchanged.queued_resumes == %{}
  end

  defp temp_root do
    suffix = :crypto.strong_rand_bytes(8) |> Base.url_encode64(padding: false)
    Path.join(System.tmp_dir!(), "symphony-orchestrator-merge-lane-#{suffix}")
  end

  defp lane_binding do
    %{
      issue_id: "issue-1",
      issue_identifier: "DUD-1",
      wait_id: "wait-1",
      repository: "stslgn/example",
      repository_id: 12_345,
      pull_request: 23,
      base_ref: "main",
      head_sha: String.duplicate("a", 40),
      channel: "session",
      executor: "codex-session-1",
      workflow_generation: String.duplicate("b", 64)
    }
  end
end
