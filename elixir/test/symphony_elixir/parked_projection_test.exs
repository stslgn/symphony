defmodule SymphonyElixir.ParkedProjectionTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ParkedProjection

  test "publishes exact projection limits and defaults invalid collections to empty" do
    assert ParkedProjection.limits() == %{
             fields: %{
               issue_id: 128,
               issue_identifier: 96,
               run_id: 128,
               tracker_state: 128,
               wait_id: 128,
               worker_host: 128,
               workspace_path: 512
             },
             row_limit: 100,
             byte_limit: 65_536
           }

    assert ParkedProjection.collection(:invalid) == ParkedProjection.collection([])
  end

  test "defaults invalid rows and non-text fields without raising" do
    assert ParkedProjection.row(:invalid) == ParkedProjection.row(%{})

    row =
      ParkedProjection.row(%{
        issue_id: 123,
        identifier: :invalid,
        reason: :waiting_owner,
        attempt: "1",
        stage: :parked,
        terminal_reason: :operator_stop,
        parked_at: "yesterday"
      })

    assert row.issue_id == nil
    assert row.issue_identifier == nil
    assert row.reason == nil
    assert row.allowed_actions == []
    assert row.attempt == nil
    assert row.stage == nil
    assert row.terminal_reason == nil
    assert row.parked_at == nil
  end

  test "escapes carriage returns and non-printing controls" do
    row =
      ParkedProjection.row(%{
        issue_id: "issue-1",
        identifier: "DUD\r1",
        wait_id: "wait\u0085id",
        reason: "waiting_owner",
        stage: "parked",
        parked_at: ~U[2026-08-05 12:30:00Z]
      })

    assert row.issue_identifier == "DUD\\r1"
    assert row.wait_id == "wait\\u{85}id"
    assert row.parked_at == "2026-08-05T12:30:00Z"
  end

  test "keeps equal sort keys stable" do
    waits = [
      %{issue_id: "same", identifier: "DUD-1", wait_id: "wait-1", tracker_state: "first"},
      %{issue_id: "same", identifier: "DUD-1", wait_id: "wait-1", tracker_state: "second"}
    ]

    assert %{rows: [%{tracker_state: "first"}, %{tracker_state: "second"}]} =
             ParkedProjection.collection(waits)
  end
end
