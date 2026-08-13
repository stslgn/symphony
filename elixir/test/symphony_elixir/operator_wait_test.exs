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
    assert OperatorWait.reason_for_tracker_state("Blocked") == "waiting_infrastructure"
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
               terminal_reason: "time_budget_exhausted",
               worker_host: "worker-a",
               workspace_path: "/srv/symphony/DUD-1"
             })

    assert wait.wait_id == "wait-fixed"
    assert wait.stage == "parked"
    assert wait.terminal_reason == "time_budget_exhausted"
    assert wait.worker_host == "worker-a"
    assert wait.workspace_path == "/srv/symphony/DUD-1"
    assert OperatorWait.action_allowed?(wait, "retry")
    refute OperatorWait.action_allowed?(wait, "approve")
    refute OperatorWait.action_allowed?(wait, :retry)
    refute OperatorWait.action_allowed?(nil, "retry")
    assert {:error, :invalid_wait_reason} = OperatorWait.new("unknown", %{})
    assert {:error, :invalid_wait_reason} = OperatorWait.new(nil, %{})
  end

  test "rejects oversized, control-bearing, invalid UTF-8, and untyped wait fields" do
    assert OperatorWait.persisted_field_limits() == %{
             "issue_id" => 128,
             "issue_identifier" => 96,
             "run_id" => 128,
             "tracker_state" => 128,
             "wait_id" => 128,
             "worker_host" => 255,
             "workspace_path" => 4_096,
             "workspace_root" => 4_096
           }

    valid = %{
      wait_id: "wait-bounded",
      issue_id: "issue-bounded",
      identifier: "DUD-BOUNDED",
      run_id: "run-bounded",
      attempt: 1,
      tracker_state: "Human Review",
      worker_host: "worker-a",
      workspace_path: "/srv/symphony/DUD-BOUNDED",
      workspace_root: "/srv/symphony"
    }

    exact_path = "/" <> String.duplicate("p", 4_095)
    assert {:ok, wait} = OperatorWait.new("waiting_owner", %{valid | workspace_path: exact_path})
    assert wait.workspace_path == exact_path

    invalid_fields = [
      {:wait_id, String.duplicate("w", 129), "wait_id"},
      {:identifier, "DUD-\nCONTROL", "issue_identifier"},
      {:tracker_state, <<0xFF>>, "tracker_state"},
      {:worker_host, String.duplicate("h", 256), "worker_host"},
      {:workspace_path, "/tmp/unsafe\e]0;title", "workspace_path"},
      {:workspace_root, String.duplicate("r", 4_097), "workspace_root"}
    ]

    Enum.each(invalid_fields, fn {field, value, persisted_field} ->
      assert {:error, {:invalid_wait_field, ^persisted_field}} =
               OperatorWait.new("waiting_owner", Map.put(valid, field, value))
    end)

    assert {:error, {:invalid_wait_field, "stage"}} =
             OperatorWait.new("waiting_owner", Map.put(valid, :stage, "retrying"))

    assert {:error, {:invalid_wait_field, "terminal_reason"}} =
             OperatorWait.new("waiting_owner", Map.put(valid, :terminal_reason, "free_form"))
  end

  test "restores only complete ledger waits with valid timestamps" do
    event = %{
      "parked_reason" => "waiting_owner",
      "wait_id" => "wait-1",
      "issue_id" => "issue-1",
      "issue_identifier" => "DUD-1",
      "run_id" => "run-1",
      "attempt" => 1,
      "stage" => "parked",
      "tracker_state" => "Human Review",
      "terminal_reason" => "turn_budget_exhausted",
      "worker_host" => "worker-a",
      "workspace_path" => "/srv/symphony/DUD-1",
      "allowed_actions" => ["approve", "reject"],
      "occurred_at" => "2026-07-29T10:00:00.000Z"
    }

    assert {:ok, wait} = OperatorWait.from_ledger_event(event)
    assert wait.parked_at == ~U[2026-07-29 10:00:00.000Z]
    assert wait.terminal_reason == "turn_budget_exhausted"
    assert wait.worker_host == "worker-a"
    assert wait.workspace_path == "/srv/symphony/DUD-1"

    assert {:error, {:invalid_wait_field, "occurred_at"}} =
             OperatorWait.from_ledger_event(%{event | "occurred_at" => "bad"})

    assert {:error, {:invalid_wait_field, "run_id"}} =
             OperatorWait.from_ledger_event(Map.delete(event, "run_id"))

    assert {:error, {:invalid_wait_field, "allowed_actions"}} =
             OperatorWait.from_ledger_event(%{event | "allowed_actions" => ["retry"]})
  end

  test "rejects invalid persisted containers and field value types" do
    assert {:error, {:invalid_wait_field, "event"}} = OperatorWait.from_ledger_event(:invalid)
    assert {:error, {:invalid_wait_field, "event"}} = OperatorWait.validate_persisted_fields(:invalid)

    assert {:error, {:invalid_wait_field, "issue_id"}} =
             OperatorWait.validate_persisted_fields(%{
               wait_id: "wait-1",
               issue_id: 123,
               identifier: "DUD-1",
               run_id: "run-1"
             })
  end

  test "rejects invalid ledger attempts and reasons" do
    event = valid_ledger_event()

    assert {:error, {:invalid_wait_field, "attempt"}} =
             OperatorWait.from_ledger_event(%{event | "attempt" => -1})

    assert {:error, :invalid_wait_reason} =
             OperatorWait.from_ledger_event(%{
               event
               | "parked_reason" => "unknown",
                 "allowed_actions" => []
             })
  end

  test "rejects non-binary ledger timestamps" do
    assert {:error, {:invalid_wait_field, "occurred_at"}} =
             OperatorWait.from_ledger_event(%{valid_ledger_event() | "occurred_at" => nil})
  end

  defp valid_ledger_event do
    %{
      "parked_reason" => "waiting_owner",
      "wait_id" => "wait-1",
      "issue_id" => "issue-1",
      "issue_identifier" => "DUD-1",
      "run_id" => "run-1",
      "attempt" => 1,
      "stage" => "parked",
      "allowed_actions" => ["approve", "reject"],
      "occurred_at" => "2026-08-05T12:30:00Z"
    }
  end
end
