defmodule SymphonyElixir.MergeLaneCLITest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.MergeLaneCLI

  test "claims through the bounded JSON CLI contract and project logs root" do
    logs_root = temp_root()
    request = Jason.encode!(lane_binding())

    assert {:ok, response} =
             MergeLaneCLI.run(["claim", "--logs-root", logs_root], request)

    assert response.state == "claimed"
    assert response.repository == "stslgn/example"
    assert File.regular?(Path.join(logs_root, "merge-lane-ledger.jsonl"))
  end

  test "advances and inspects a claim through the same CLI ledger" do
    logs_root = temp_root()
    assert {:ok, claim} = MergeLaneCLI.run(["claim", "--logs-root", logs_root], Jason.encode!(lane_binding()))

    transition = %{
      claim_id: claim.claim_id,
      fencing_token: claim.fencing_token,
      target_state: "executing",
      evidence: %{}
    }

    assert {:ok, executing} =
             MergeLaneCLI.run(
               ["transition", "--logs-root", logs_root],
               Jason.encode!(transition)
             )

    assert executing.state == "executing"

    assert {:ok, %{"events" => events}} =
             MergeLaneCLI.run(["history", "--logs-root", logs_root], "{}")

    assert Enum.map(events, & &1["transition"]) == ["claimed", "executing"]
  end

  test "recovers an uncertain claim and rejects malformed commands" do
    logs_root = temp_root()
    assert {:ok, claim} = MergeLaneCLI.run(["claim", "--logs-root", logs_root], Jason.encode!(lane_binding()))

    assert {:ok, _executing} =
             MergeLaneCLI.run(
               ["transition", "--logs-root", logs_root],
               Jason.encode!(%{
                 claim_id: claim.claim_id,
                 fencing_token: 1,
                 target_state: "executing"
               })
             )

    assert {:ok, _uncertain} =
             MergeLaneCLI.run(
               ["transition", "--logs-root", logs_root],
               Jason.encode!(%{
                 claim_id: claim.claim_id,
                 fencing_token: 1,
                 target_state: "merge_uncertain",
                 evidence: %{evidence_sha256: String.duplicate("c", 64)}
               })
             )

    assert {:ok, retained} =
             MergeLaneCLI.run(
               ["recover", "--logs-root", logs_root],
               Jason.encode!(%{
                 claim_id: claim.claim_id,
                 fencing_token: 1,
                 action: "reconcile",
                 evidence: %{
                   result: "ambiguous",
                   evidence_sha256: String.duplicate("d", 64)
                 }
               })
             )

    assert retained.state == "operator_required"

    assert {:error, :invalid_merge_lane_command} = MergeLaneCLI.run(:invalid, "{}")
    assert {:error, :invalid_merge_lane_command} = MergeLaneCLI.run(["unknown"], "{}")
    assert {:error, :invalid_logs_root} = MergeLaneCLI.run(["claim"], "{}")
    assert {:error, :invalid_logs_root} = MergeLaneCLI.run(["claim", "--logs-root", " "], "{}")

    assert {:error, :invalid_merge_lane_request} =
             MergeLaneCLI.run(["claim", "--logs-root", logs_root], "[]")

    assert {:error, :merge_lane_request_too_large} =
             MergeLaneCLI.run(
               ["claim", "--logs-root", logs_root],
               String.duplicate("x", 16_385)
             )
  end

  test "history propagates ledger corruption" do
    logs_root = temp_root()
    File.mkdir_p!(logs_root)
    File.write!(Path.join(logs_root, "merge-lane-ledger.jsonl"), "not-json\n")

    assert {:error, :invalid_merge_lane_ledger} =
             MergeLaneCLI.run(["history", "--logs-root", logs_root], "{}")
  end

  defp temp_root do
    suffix = :crypto.strong_rand_bytes(8) |> Base.url_encode64(padding: false)
    Path.join(System.tmp_dir!(), "symphony-merge-lane-cli-#{suffix}")
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
