defmodule SymphonyElixir.TrackerAdmissionTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Linear.Issue
  alias SymphonyElixir.Tracker.PollContext
  alias SymphonyElixir.TrackerAdmission

  test "builds deterministic state-independent issue snapshot evidence" do
    ready_issue = issue(state: "Agent Ready", labels: ["zeta", "alpha", "alpha"])
    running_issue = %{ready_issue | state: "Agent Running", labels: ["alpha", "zeta"]}

    assert {:ok, ready_snapshot} = TrackerAdmission.snapshot(ready_issue)
    assert {:ok, running_snapshot} = TrackerAdmission.snapshot(running_issue)

    assert ready_snapshot.schema == "symphony.issue_snapshot.v1"
    assert ready_snapshot.sha256 == running_snapshot.sha256
    assert ready_snapshot.bytes == byte_size(ready_snapshot.canonical_json)
    assert ready_snapshot.canonical_json =~ ~s("labels":["alpha","zeta"])
    refute ready_snapshot.canonical_json =~ "Agent Ready"
  end

  test "read-back requires the exact target state and unchanged snapshot" do
    assert {:ok, expected} = TrackerAdmission.snapshot(issue())

    assert {:ok, verified} =
             TrackerAdmission.verify_readback([issue(state: "Agent Running")], "issue-1", expected, target_state: "Agent Running")

    assert verified.state == "Agent Running"

    assert {:error, :target_state_not_observed} =
             TrackerAdmission.verify_readback([issue()], "issue-1", expected, target_state: "Agent Running")

    changed = issue(state: "Agent Running", title: "Changed concurrently")

    assert {:error, :issue_snapshot_conflict} =
             TrackerAdmission.verify_readback([changed], "issue-1", expected, target_state: "Agent Running")

    assert {:error, :issue_not_found} =
             TrackerAdmission.verify_readback([], "issue-1", expected, target_state: "Agent Running")
  end

  test "rejects malformed issue snapshot inputs" do
    assert {:error, {:invalid_snapshot_field, :labels}} =
             TrackerAdmission.snapshot(issue(labels: ["valid", nil]))

    assert {:error, {:invalid_snapshot_field, :id}} =
             TrackerAdmission.snapshot(issue(id: nil))

    assert {:error, {:invalid_snapshot_field, :description}} =
             TrackerAdmission.snapshot(issue(description: 42))

    assert {:error, {:invalid_snapshot_field, :labels}} =
             TrackerAdmission.snapshot(issue(labels: :invalid))
  end

  test "rejects malformed packets and unavailable tracker authority" do
    context = %PollContext{authority_generation: {:workflow_store, 7}}

    assert {:error, :invalid_admission_packet} =
             TrackerAdmission.packet(issue(state: nil), context, "admission-1", "Agent Running")

    assert {:error, :tracker_authority_unavailable} =
             TrackerAdmission.packet(
               issue(),
               %PollContext{authority_generation: nil},
               "admission-1",
               "Agent Running"
             )
  end

  test "read-back propagates malformed snapshot evidence" do
    assert {:ok, expected} = TrackerAdmission.snapshot(issue())

    assert {:error, {:invalid_snapshot_field, :title}} =
             TrackerAdmission.verify_readback(
               [issue(state: "Agent Running", title: nil)],
               "issue-1",
               expected,
               target_state: "Agent Running"
             )
  end

  test "recovery evidence pins snapshot bytes and tracker authority" do
    context = context(authority_generation: {self(), 7})

    assert {:ok, packet, _snapshot} =
             TrackerAdmission.packet(
               issue(),
               context,
               "admission-recovery",
               "Agent Running"
             )

    assert :ok = TrackerAdmission.verify_recovery_evidence(issue(), context, packet)

    restarted_context = %{context | authority_generation: {spawn(fn -> :ok end), 0}}

    assert :ok =
             TrackerAdmission.verify_recovery_evidence(
               issue(),
               restarted_context,
               packet
             )

    changed_context = %{context | endpoint: "https://other.example/graphql"}

    assert {:error, :tracker_authority_conflict} =
             TrackerAdmission.verify_recovery_evidence(issue(), changed_context, packet)

    assert {:error, :issue_snapshot_conflict} =
             TrackerAdmission.verify_recovery_evidence(
               issue(title: "Changed"),
               context,
               packet
             )
  end

  defp issue(overrides \\ []) do
    struct!(
      Issue,
      Keyword.merge(
        [
          id: "issue-1",
          identifier: "DUD-1",
          title: "Admission contract",
          description: "Do not start the model before read-back.",
          state: "Agent Ready",
          url: "https://linear.example/DUD-1",
          labels: ["alpha", "zeta"]
        ],
        overrides
      )
    )
  end

  defp context(overrides) do
    struct!(
      PollContext,
      Keyword.merge(
        [
          kind: "linear",
          endpoint: "https://api.linear.app/graphql",
          api_key: "synthetic-secret",
          api_key_env_var: "LINEAR_API_KEY",
          webhook_secret_env_var: "LINEAR_WEBHOOK_SECRET",
          project_slug: "project",
          authority_generation: {self(), 0}
        ],
        overrides
      )
    )
  end
end
