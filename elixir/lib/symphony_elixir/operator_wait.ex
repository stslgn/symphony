defmodule SymphonyElixir.OperatorWait do
  @moduledoc """
  Typed, bounded operator-wait records used to park Symphony issues safely.
  """

  @reason_actions %{
    "waiting_owner" => ["approve", "reject"],
    "waiting_secret" => ["retry", "reject"],
    "waiting_live_approval" => ["approve", "reject"],
    "waiting_infrastructure" => ["retry", "reject"],
    "review_cap_reached" => ["approve", "reject"],
    "auth_reconnect_required" => ["retry", "reject"],
    "run_budget_exhausted" => ["retry", "reject"],
    "operator_stopped" => ["retry", "reject"]
  }

  @spec reasons() :: [String.t()]
  def reasons, do: Map.keys(@reason_actions) |> Enum.sort()

  @spec valid_reason?(term()) :: boolean()
  def valid_reason?(reason) when is_binary(reason), do: Map.has_key?(@reason_actions, reason)
  def valid_reason?(_reason), do: false

  @spec allowed_actions(String.t()) :: [String.t()]
  def allowed_actions(reason), do: Map.get(@reason_actions, reason, [])

  @spec action_allowed?(map(), term()) :: boolean()
  def action_allowed?(wait, action) when is_map(wait) and is_binary(action) do
    action in Map.get(wait, :allowed_actions, [])
  end

  def action_allowed?(_wait, _action), do: false

  @spec reason_for_tracker_state(term()) :: String.t() | nil
  def reason_for_tracker_state(state) when is_binary(state) do
    case state |> String.trim() |> String.downcase() do
      "human review" -> "waiting_owner"
      "human clarification" -> "waiting_owner"
      "deploy ready" -> "waiting_live_approval"
      _other -> nil
    end
  end

  def reason_for_tracker_state(_state), do: nil

  @spec new(String.t(), map()) :: {:ok, map()} | {:error, :invalid_wait_reason}
  def new(reason, attrs) when is_binary(reason) and is_map(attrs) do
    if valid_reason?(reason) do
      {:ok,
       %{
         wait_id: Map.get(attrs, :wait_id) || SymphonyElixir.RunLedger.new_id("wait"),
         reason: reason,
         allowed_actions: allowed_actions(reason),
         issue_id: Map.get(attrs, :issue_id),
         identifier: Map.get(attrs, :identifier),
         run_id: Map.get(attrs, :run_id),
         attempt: Map.get(attrs, :attempt, 0),
         stage: Map.get(attrs, :stage, "parked"),
         tracker_state: Map.get(attrs, :tracker_state),
         terminal_reason: Map.get(attrs, :terminal_reason),
         parked_at: Map.get(attrs, :parked_at) || DateTime.utc_now()
       }}
    else
      {:error, :invalid_wait_reason}
    end
  end

  def new(_reason, _attrs), do: {:error, :invalid_wait_reason}

  @spec from_ledger_event(map()) :: {:ok, map()} | {:error, :invalid_wait_reason}
  def from_ledger_event(event) when is_map(event) do
    new(event["parked_reason"], %{
      wait_id: event["wait_id"],
      issue_id: event["issue_id"],
      identifier: event["issue_identifier"],
      run_id: event["run_id"],
      attempt: event["attempt"],
      stage: event["stage"],
      tracker_state: event["tracker_state"],
      terminal_reason: event["terminal_reason"],
      parked_at: parse_timestamp(event["occurred_at"])
    })
  end

  defp parse_timestamp(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, timestamp, _offset} -> timestamp
      {:error, _reason} -> DateTime.utc_now()
    end
  end

  defp parse_timestamp(_value), do: DateTime.utc_now()
end
