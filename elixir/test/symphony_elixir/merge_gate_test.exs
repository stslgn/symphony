defmodule SymphonyElixir.MergeGateTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.MergeGate

  test "head-only provider capability fails before the merge boundary is called" do
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
             MergeGate.merge(executing_claim(), provider_snapshot(), provider)

    refute_received :provider_merge_called
  end

  test "exact target and atomic base-head capability reach the provider once" do
    parent = self()

    provider = %{
      capabilities: fn ->
        {:ok,
         %{
           atomic_base_ref: true,
           atomic_head_sha: true,
           base_retarget_impossible: false,
           evidence_sha256: String.duplicate("d", 64)
         }}
      end,
      merge: fn request ->
        send(parent, {:provider_merge_called, request})
        {:ok, %{merged: true}}
      end
    }

    assert {:ok, %{merged: true}} =
             MergeGate.merge(executing_claim(), provider_snapshot(), provider)

    assert_receive {:provider_merge_called, request}
    assert request.expected_base_ref == "main"
    assert request.expected_head_sha == String.duplicate("a", 40)
    assert request.fencing_token == 7
  end

  test "invalid requests, inactive claims, and invalid capabilities fail closed" do
    valid_provider = %{
      capabilities: fn -> {:ok, atomic_capabilities()} end,
      merge: fn _request -> {:ok, %{merged: true}} end
    }

    assert {:error, :invalid_merge_gate_request} = MergeGate.merge(nil, %{}, %{})

    assert {:error, :claim_not_executing} =
             MergeGate.merge(%{executing_claim() | state: "claimed"}, provider_snapshot(), valid_provider)

    nonmap_capabilities = %{valid_provider | capabilities: fn -> {:ok, :invalid} end}

    assert {:error, :atomic_base_head_unavailable} =
             MergeGate.merge(executing_claim(), provider_snapshot(), nonmap_capabilities)
  end

  defp atomic_capabilities do
    %{
      atomic_base_ref: true,
      atomic_head_sha: true,
      base_retarget_impossible: false,
      evidence_sha256: String.duplicate("d", 64)
    }
  end

  defp executing_claim do
    %{
      claim_id: "merge-claim-1",
      state: "executing",
      fencing_token: 7,
      repository: "stslgn/example",
      pull_request: 23,
      base_ref: "main",
      head_sha: String.duplicate("a", 40)
    }
  end

  defp provider_snapshot do
    %{
      repository: "stslgn/example",
      pull_request: 23,
      state: "OPEN",
      base_ref: "main",
      head_sha: String.duplicate("a", 40)
    }
  end
end
