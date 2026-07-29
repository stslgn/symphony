defmodule SymphonyElixir.RunBudget do
  @moduledoc """
  Attempt-scoped worker limits and bounded status snapshots.
  """

  @terminal_reasons [
    "turn_budget_exhausted",
    "token_budget_exhausted",
    "time_budget_exhausted"
  ]

  @type limits :: %{
          max_turns: pos_integer(),
          max_tokens: pos_integer() | nil,
          max_seconds: pos_integer() | nil
        }

  @spec from_agent_config(map()) :: limits()
  def from_agent_config(agent) when is_map(agent) do
    %{
      max_turns: Map.fetch!(agent, :max_turns),
      max_tokens: Map.get(agent, :max_run_tokens),
      max_seconds: Map.get(agent, :max_run_seconds)
    }
  end

  @spec terminal_reasons() :: [String.t()]
  def terminal_reasons, do: @terminal_reasons

  @spec valid_terminal_reason?(term()) :: boolean()
  def valid_terminal_reason?(reason), do: reason in @terminal_reasons

  @spec exhausted_reason(limits(), map()) :: String.t() | nil
  def exhausted_reason(limits, metrics) when is_map(limits) and is_map(metrics) do
    cond do
      token_exhausted?(limits, metrics) -> "token_budget_exhausted"
      time_exhausted?(limits, metrics) -> "time_budget_exhausted"
      true -> nil
    end
  end

  @spec snapshot(limits(), map()) :: map()
  def snapshot(limits, metrics) when is_map(limits) and is_map(metrics) do
    turns_used = non_negative_integer(Map.get(metrics, :turns))
    seconds_used = non_negative_integer(Map.get(metrics, :seconds))
    telemetry_observed? = Map.get(metrics, :token_telemetry_observed, false) == true
    tokens_used = non_negative_integer(Map.get(metrics, :tokens))
    token_limit = Map.get(limits, :max_tokens)

    %{
      turns: allowance(Map.fetch!(limits, :max_turns), turns_used),
      tokens: %{
        limit: token_limit,
        used: if(telemetry_observed?, do: tokens_used),
        remaining: if(telemetry_observed?, do: remaining(token_limit, tokens_used)),
        telemetry_observed: telemetry_observed?
      },
      time: allowance(Map.get(limits, :max_seconds), seconds_used)
    }
  end

  defp token_exhausted?(limits, metrics) do
    limit = Map.get(limits, :max_tokens)
    observed? = Map.get(metrics, :token_telemetry_observed, false) == true
    used = non_negative_integer(Map.get(metrics, :tokens))
    is_integer(limit) and observed? and used >= limit
  end

  defp time_exhausted?(limits, metrics) do
    limit = Map.get(limits, :max_seconds)
    used = non_negative_integer(Map.get(metrics, :seconds))
    is_integer(limit) and used >= limit
  end

  defp allowance(limit, used) do
    %{limit: limit, used: used, remaining: remaining(limit, used)}
  end

  defp remaining(nil, _used), do: nil
  defp remaining(limit, used), do: max(limit - used, 0)

  defp non_negative_integer(value) when is_integer(value), do: max(value, 0)
  defp non_negative_integer(_value), do: 0
end
