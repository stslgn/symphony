defmodule SymphonyElixir.OperatorWait do
  @moduledoc """
  Typed, bounded operator-wait records used to park Symphony issues safely.

  Persisted text is valid UTF-8, contains no Unicode control codepoints, and is
  limited in bytes as follows: wait, issue, and run ids are 128 bytes; issue
  identifiers are 96 bytes; tracker states are 128 bytes; worker hosts are 255
  bytes; and exact workspace paths and roots are 4,096 bytes. Workspace
  affinity is validated and retained byte-for-byte inside this boundary; it is
  never truncated before resume or cleanup.
  """

  @reason_actions %{
    "waiting_owner" => ["approve", "reject"],
    "waiting_secret" => ["retry", "reject"],
    "waiting_live_approval" => ["approve", "reject"],
    "waiting_infrastructure" => ["retry", "reject"],
    "review_cap_reached" => ["approve", "reject"],
    "auth_reconnect_required" => ["retry", "reject"],
    "run_budget_exhausted" => ["retry", "reject"],
    "tracker_admission_failed" => ["retry", "reject"],
    "tracker_admission_conflict" => ["retry", "reject"],
    "operator_stopped" => ["retry", "reject"]
  }
  @field_limits %{
    "wait_id" => 128,
    "issue_id" => 128,
    "issue_identifier" => 96,
    "run_id" => 128,
    "tracker_state" => 128,
    "worker_host" => 255,
    "workspace_path" => 4_096,
    "workspace_root" => 4_096
  }
  @field_sources [
    {"wait_id", :wait_id},
    {"issue_id", :issue_id},
    {"issue_identifier", :identifier},
    {"run_id", :run_id},
    {"tracker_state", :tracker_state},
    {"worker_host", :worker_host},
    {"workspace_path", :workspace_path},
    {"workspace_root", :workspace_root}
  ]
  @required_fields ~w(wait_id issue_id issue_identifier run_id)
  @stages ["parked"]
  @terminal_reasons [
    "operator_stop",
    "time_budget_exhausted",
    "token_budget_exhausted",
    "token_telemetry_integrity_failed",
    "uncached_input_budget_exhausted",
    "turn_budget_exhausted"
  ]

  @spec persisted_field_limits() :: %{String.t() => pos_integer()}
  def persisted_field_limits, do: @field_limits

  @spec reasons() :: [String.t()]
  def reasons, do: Map.keys(@reason_actions) |> Enum.sort()

  @spec valid_reason?(term()) :: boolean()
  def valid_reason?(reason) when is_binary(reason), do: Map.has_key?(@reason_actions, reason)
  def valid_reason?(_reason), do: false

  @spec allowed_actions(String.t()) :: [String.t()]
  def allowed_actions(reason), do: Map.get(@reason_actions, reason, [])

  @spec valid_stage?(term()) :: boolean()
  def valid_stage?(stage), do: stage in @stages

  @spec valid_terminal_reason?(term()) :: boolean()
  def valid_terminal_reason?(nil), do: true
  def valid_terminal_reason?(reason), do: reason in @terminal_reasons

  @spec action_allowed?(map(), term()) :: boolean()
  def action_allowed?(wait, action) when is_map(wait) and is_binary(action) do
    action in Map.get(wait, :allowed_actions, [])
  end

  def action_allowed?(_wait, _action), do: false

  @spec human_review_state?(term()) :: boolean()
  def human_review_state?(state) when is_binary(state) do
    state |> String.trim() |> String.downcase() == "human review"
  end

  def human_review_state?(_state), do: false

  @spec reason_for_tracker_state(term()) :: String.t() | nil
  def reason_for_tracker_state(state) when is_binary(state) do
    case state |> String.trim() |> String.downcase() do
      "human review" -> "waiting_owner"
      "human clarification" -> "waiting_owner"
      "deploy ready" -> "waiting_live_approval"
      "blocked" -> "waiting_infrastructure"
      _other -> nil
    end
  end

  def reason_for_tracker_state(_state), do: nil

  @spec new(String.t(), map()) ::
          {:ok, map()}
          | {:error, :invalid_wait_reason | {:invalid_wait_field, String.t()}}
  def new(reason, attrs) when is_binary(reason) and is_map(attrs) do
    if valid_reason?(reason) do
      wait = %{
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
        worker_host: Map.get(attrs, :worker_host),
        workspace_path: Map.get(attrs, :workspace_path),
        workspace_root: Map.get(attrs, :workspace_root),
        parked_at: Map.get(attrs, :parked_at) || DateTime.utc_now()
      }

      with :ok <- validate_persisted_fields(wait),
           :ok <- validate_typed_fields(wait) do
        {:ok, wait}
      end
    else
      {:error, :invalid_wait_reason}
    end
  end

  def new(_reason, _attrs), do: {:error, :invalid_wait_reason}

  @spec from_ledger_event(map()) ::
          {:ok, map()} | {:error, :invalid_wait_reason | {:invalid_wait_field, String.t()}}
  def from_ledger_event(event) when is_map(event) do
    with :ok <- validate_ledger_identity(event),
         :ok <- validate_ledger_actions(event),
         {:ok, parked_at} <- parse_timestamp(event["occurred_at"]) do
      new(event["parked_reason"], %{
        wait_id: event["wait_id"],
        issue_id: event["issue_id"],
        identifier: event["issue_identifier"],
        run_id: event["run_id"],
        attempt: event["attempt"],
        stage: event["stage"],
        tracker_state: event["tracker_state"],
        terminal_reason: event["terminal_reason"],
        worker_host: event["worker_host"],
        workspace_path: event["workspace_path"],
        workspace_root: event["workspace_root"],
        parked_at: parked_at
      })
    end
  end

  def from_ledger_event(_event), do: {:error, {:invalid_wait_field, "event"}}

  @spec validate_persisted_fields(map()) ::
          :ok | {:error, {:invalid_wait_field, String.t()}}
  def validate_persisted_fields(fields) when is_map(fields) do
    Enum.reduce_while(@field_sources, :ok, fn {field, atom_key}, :ok ->
      value = field_value(fields, field, atom_key)

      case validate_text_field(field, value) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  def validate_persisted_fields(_fields), do: {:error, {:invalid_wait_field, "event"}}

  defp validate_ledger_identity(event) do
    with :ok <- validate_required_string(event, "wait_id"),
         :ok <- validate_required_string(event, "issue_id"),
         :ok <- validate_required_string(event, "issue_identifier"),
         :ok <- validate_required_string(event, "run_id") do
      validate_required_attempt(event)
    end
  end

  defp validate_required_string(event, field) do
    case Map.fetch(event, field) do
      {:ok, value} when is_binary(value) and value != "" -> :ok
      _other -> {:error, {:invalid_wait_field, field}}
    end
  end

  defp validate_required_attempt(%{"attempt" => attempt})
       when is_integer(attempt) and attempt >= 0,
       do: :ok

  defp validate_required_attempt(_event), do: {:error, {:invalid_wait_field, "attempt"}}

  defp validate_ledger_actions(event) do
    reason = event["parked_reason"]

    cond do
      not valid_reason?(reason) ->
        {:error, :invalid_wait_reason}

      event["allowed_actions"] != allowed_actions(reason) ->
        {:error, {:invalid_wait_field, "allowed_actions"}}

      true ->
        :ok
    end
  end

  defp validate_typed_fields(wait) do
    cond do
      not valid_stage?(Map.get(wait, :stage)) ->
        {:error, {:invalid_wait_field, "stage"}}

      not valid_terminal_reason?(Map.get(wait, :terminal_reason)) ->
        {:error, {:invalid_wait_field, "terminal_reason"}}

      not (is_integer(Map.get(wait, :attempt)) and Map.get(wait, :attempt) >= 0) ->
        {:error, {:invalid_wait_field, "attempt"}}

      not match?(%DateTime{}, Map.get(wait, :parked_at)) ->
        {:error, {:invalid_wait_field, "parked_at"}}

      true ->
        :ok
    end
  end

  defp field_value(fields, field, atom_key) do
    case Map.fetch(fields, atom_key) do
      {:ok, value} -> value
      :error -> Map.get(fields, field)
    end
  end

  defp validate_text_field(field, nil) do
    if field in @required_fields,
      do: {:error, {:invalid_wait_field, field}},
      else: :ok
  end

  defp validate_text_field(field, value) when is_binary(value) do
    if value != "" and String.valid?(value) and byte_size(value) <= Map.fetch!(@field_limits, field) and
         not Regex.match?(~r/\p{C}/u, value) do
      :ok
    else
      {:error, {:invalid_wait_field, field}}
    end
  end

  defp validate_text_field(field, _value), do: {:error, {:invalid_wait_field, field}}

  defp parse_timestamp(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, timestamp, _offset} -> {:ok, timestamp}
      {:error, _reason} -> {:error, {:invalid_wait_field, "occurred_at"}}
    end
  end

  defp parse_timestamp(_value), do: {:error, {:invalid_wait_field, "occurred_at"}}
end
