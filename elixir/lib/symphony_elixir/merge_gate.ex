defmodule SymphonyElixir.MergeGate do
  @moduledoc """
  Fail-closed boundary for a provider merge mutation.

  A provider is callable only when the current pull-request snapshot still
  matches the fenced claim and verified capabilities bind both the base-ref
  identity and approved head SHA for the mutation.
  """

  alias SymphonyElixir.MergeLane

  @type provider :: %{
          required(:capabilities) => (-> {:ok, map()} | {:error, term()}),
          required(:merge) => (map() -> {:ok, map()} | {:error, term()})
        }

  @spec merge(Path.t(), String.t(), pos_integer(), String.t(), map(), provider()) ::
          {:ok, map()} | {:error, term()}
  def merge(
        ledger_path,
        claim_id,
        fencing_token,
        owner_capability,
        snapshot,
        %{capabilities: capabilities_fn, merge: merge_fn}
      )
      when is_binary(ledger_path) and is_binary(claim_id) and is_integer(fencing_token) and
             fencing_token > 0 and is_binary(owner_capability) and is_map(snapshot) and
             is_function(capabilities_fn, 0) and is_function(merge_fn, 1) do
    MergeLane.with_current_claim(
      ledger_path,
      claim_id,
      fencing_token,
      owner_capability,
      "executing",
      fn claim ->
        with :ok <- validate_snapshot(claim, snapshot),
             {:ok, capabilities} <- capabilities_fn.(),
             :ok <- validate_capabilities(capabilities) do
          merge_fn.(merge_request(claim))
        end
      end
    )
  end

  def merge(
        _ledger_path,
        _claim_id,
        _fencing_token,
        _owner_capability,
        _snapshot,
        _provider
      ),
      do: {:error, :invalid_merge_gate_request}

  defp validate_snapshot(claim, snapshot) do
    if snapshot[:state] == "OPEN" and
         is_integer(snapshot[:repository_id]) and
         snapshot[:repository_id] == claim[:repository_id] and
         snapshot[:repository] == claim[:repository] and
         snapshot[:pull_request] == claim[:pull_request] and
         snapshot[:base_ref] == claim[:base_ref] and
         snapshot[:head_sha] == claim[:head_sha] do
      :ok
    else
      {:error, :approved_target_mismatch}
    end
  end

  defp validate_capabilities(capabilities) when is_map(capabilities) do
    with true <- capabilities[:atomic_head_sha] == true,
         true <-
           capabilities[:atomic_base_ref] == true or
             capabilities[:base_retarget_impossible] == true,
         true <- valid_sha256?(capabilities[:evidence_sha256]) do
      :ok
    else
      _other -> {:error, :atomic_base_head_unavailable}
    end
  end

  defp validate_capabilities(_capabilities), do: {:error, :atomic_base_head_unavailable}

  defp merge_request(claim) do
    %{
      claim_id: claim.claim_id,
      fencing_token: claim.fencing_token,
      repository: claim.repository,
      expected_repository_id: claim.repository_id,
      pull_request: claim.pull_request,
      expected_base_ref: claim.base_ref,
      expected_head_sha: claim.head_sha
    }
  end

  defp valid_sha256?(value),
    do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
end
