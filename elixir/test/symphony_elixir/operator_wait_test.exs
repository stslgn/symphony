defmodule SymphonyElixir.OperatorWaitTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.OperatorWait

  test "defines the complete typed reason set and actions" do
    assert OperatorWait.reasons() == [
             "auth_reconnect_required",
             "operator_stopped",
             "review_cap_reached",
             "run_budget_exhausted",
             "waiting_infrastructure",
             "waiting_live_approval",
             "waiting_owner",
             "waiting_secret"
           ]

    assert OperatorWait.valid_reason?("waiting_owner")
    refute OperatorWait.valid_reason?(:waiting_owner)
    refute OperatorWait.valid_reason?("unknown")
    assert OperatorWait.allowed_actions("waiting_secret") == ["retry", "reject"]
    assert OperatorWait.allowed_actions("run_budget_exhausted") == ["retry", "reject"]
    assert OperatorWait.allowed_actions("operator_stopped") == ["retry", "reject"]
    assert OperatorWait.allowed_actions("unknown") == []
  end

  test "maps approval tracker states to typed waits" do
    assert OperatorWait.reason_for_tracker_state("Human Review") == "waiting_owner"
    assert OperatorWait.reason_for_tracker_state(" human clarification ") == "waiting_owner"
    assert OperatorWait.reason_for_tracker_state("Deploy Ready") == "waiting_live_approval"
    assert OperatorWait.reason_for_tracker_state("Paused") == nil
    assert OperatorWait.reason_for_tracker_state(nil) == nil
  end

  test "builds bounded waits and validates actions" do
    assert {:ok, wait} =
             OperatorWait.new("waiting_infrastructure", %{
               wait_id: "wait-fixed",
               issue_id: "issue-1",
               identifier: "DUD-1",
               run_id: "run-1",
               attempt: 2,
               tracker_state: "Human Clarification",
               terminal_reason: "time_budget_exhausted"
             })

    assert wait.wait_id == "wait-fixed"
    assert wait.stage == "parked"
    assert wait.terminal_reason == "time_budget_exhausted"
    assert OperatorWait.action_allowed?(wait, "retry")
    refute OperatorWait.action_allowed?(wait, "approve")
    refute OperatorWait.action_allowed?(wait, :retry)
    refute OperatorWait.action_allowed?(nil, "retry")
    assert {:error, :invalid_wait_reason} = OperatorWait.new("unknown", %{})
    assert {:error, :invalid_wait_reason} = OperatorWait.new(nil, %{})
  end

  test "restores only complete ledger waits with valid timestamps" do
    event = %{
      "parked_reason" => "waiting_owner",
      "wait_id" => "wait-1",
      "issue_id" => "issue-1",
      "issue_identifier" => "DUD-1",
      "run_id" => "run-1",
      "attempt" => 1,
      "stage" => "human_review",
      "tracker_state" => "Human Review",
      "terminal_reason" => "turn_budget_exhausted",
      "allowed_actions" => ["approve", "reject"],
      "occurred_at" => "2026-07-29T10:00:00.000Z"
    }

    assert {:ok, wait} = OperatorWait.from_ledger_event(event)
    assert wait.parked_at == ~U[2026-07-29 10:00:00.000Z]
    assert wait.terminal_reason == "turn_budget_exhausted"

    assert {:error, {:invalid_wait_field, "occurred_at"}} =
             OperatorWait.from_ledger_event(%{event | "occurred_at" => "bad"})

    assert {:error, {:invalid_wait_field, "run_id"}} =
             OperatorWait.from_ledger_event(Map.delete(event, "run_id"))

    assert {:error, {:invalid_wait_field, "allowed_actions"}} =
             OperatorWait.from_ledger_event(%{event | "allowed_actions" => ["retry"]})
  end
end
