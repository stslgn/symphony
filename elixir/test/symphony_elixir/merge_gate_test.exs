defmodule SymphonyElixir.MergeGateTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{MergeGate, MergeLane}

  test "head-only provider capability fails before the merge boundary is called" do
    {path, claim} = executing_claim()
    parent = self()

    provider = %{
      capabilities: fn ->
        {:ok,
         %{
           atomic_base_ref: false,
           atomic_head_sha: true,
           base_retarget_impossible: false,
           evidence_sha256: String.duplicate("c", 64)
         }}
      end,
      merge: fn _request ->
        send(parent, :provider_merge_called)
        {:ok, %{}}
      end
    }

    assert {:error, :atomic_base_head_unavailable} =
             MergeGate.merge(
               path,
               claim.claim_id,
               claim.fencing_token,
               claim.owner_capability,
               provider_snapshot(),
               provider
             )

    refute_received :provider_merge_called
  end

  test "current ledger claim, exact target, and atomic capability reach the provider once" do
    {path, claim} = executing_claim()
    parent = self()

    provider = %{
      capabilities: fn -> {:ok, atomic_capabilities()} end,
      merge: fn request ->
        send(parent, {:provider_merge_called, request})
        {:ok, %{merged: true}}
      end
    }

    assert {:ok, %{merged: true}} =
             MergeGate.merge(
               path,
               claim.claim_id,
               claim.fencing_token,
               claim.owner_capability,
               provider_snapshot(),
               provider
             )

    assert_receive {:provider_merge_called, request}
    assert request.expected_repository_id == 12_345
    assert request.expected_base_ref == "main"
    assert request.expected_head_sha == String.duplicate("a", 40)
    assert request.fencing_token == claim.fencing_token
  end

  test "fabricated, stale, terminal, and mismatched claims never reach the provider" do
    {path, claim} = executing_claim()
    parent = self()

    provider = %{
      capabilities: fn -> {:ok, atomic_capabilities()} end,
      merge: fn _request ->
        send(parent, :provider_merge_called)
        {:ok, %{merged: true}}
      end
    }

    assert {:error, :claim_not_found} =
             MergeGate.merge(
               path,
               "merge_claim_fabricated",
               claim.fencing_token,
               claim.owner_capability,
               provider_snapshot(),
               provider
             )

    assert {:error, :stale_fencing_token} =
             MergeGate.merge(
               path,
               claim.claim_id,
               claim.fencing_token + 1,
               claim.owner_capability,
               provider_snapshot(),
               provider
             )

    assert {:error, :claim_owner_mismatch} =
             MergeGate.merge(
               path,
               claim.claim_id,
               claim.fencing_token,
               "not-the-owner-capability",
               provider_snapshot(),
               provider
             )

    for {field, value} <- [
          repository_id: nil,
          repository: nil,
          pull_request: nil,
          base_ref: nil,
          head_sha: nil,
          state: "CLOSED"
        ] do
      assert {:error, :approved_target_mismatch} =
               MergeGate.merge(
                 path,
                 claim.claim_id,
                 claim.fencing_token,
                 claim.owner_capability,
                 Map.put(provider_snapshot(), field, value),
                 provider
               )
    end

    assert {:ok, _terminal} =
             MergeLane.transition(
               path,
               claim.claim_id,
               claim.fencing_token,
               "aborted_no_side_effect",
               claim.owner_capability,
               evidence(%{result: "no_provider_side_effect"})
             )

    assert {:error, :claim_state_mismatch} =
             MergeGate.merge(
               path,
               claim.claim_id,
               claim.fencing_token,
               claim.owner_capability,
               provider_snapshot(),
               provider
             )

    refute_received :provider_merge_called
  end

  test "invalid request and invalid capabilities fail closed" do
    {path, claim} = executing_claim()

    assert {:error, :invalid_merge_gate_request} =
             MergeGate.merge(nil, nil, nil, nil, %{}, %{})

    nonmap_capabilities = %{
      capabilities: fn -> {:ok, :invalid} end,
      merge: fn _request -> {:ok, %{merged: true}} end
    }

    assert {:error, :atomic_base_head_unavailable} =
             MergeGate.merge(
               path,
               claim.claim_id,
               claim.fencing_token,
               claim.owner_capability,
               provider_snapshot(),
               nonmap_capabilities
             )
  end

  defp executing_claim do
    path = ledger_path()
    assert {:ok, claim} = MergeLane.claim(path, lane_binding())

    assert {:ok, executing} =
             MergeLane.transition(
               path,
               claim.claim_id,
               claim.fencing_token,
               "executing",
               claim.owner_capability
             )

    {path, executing}
  end

  defp atomic_capabilities do
    %{
      atomic_base_ref: true,
      atomic_head_sha: true,
      base_retarget_impossible: false,
      evidence_sha256: String.duplicate("d", 64)
    }
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

  defp provider_snapshot do
    %{
      repository_id: 12_345,
      repository: "stslgn/example",
      pull_request: 23,
      state: "OPEN",
      base_ref: "main",
      head_sha: String.duplicate("a", 40)
    }
  end

  defp evidence(fields) do
    Map.put(fields, :evidence_sha256, MergeLane.evidence_sha256(fields))
  end

  defp ledger_path do
    suffix = :crypto.strong_rand_bytes(8) |> Base.url_encode64(padding: false)
    Path.join(System.tmp_dir!(), "symphony-merge-gate-#{suffix}.jsonl")
  end
end
