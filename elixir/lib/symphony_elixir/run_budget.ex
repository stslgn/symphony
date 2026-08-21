defmodule SymphonyElixir.RunBudget do
  @moduledoc """
  Attempt-scoped worker limits and bounded status snapshots.
  """

  @terminal_reasons [
    "turn_budget_exhausted",
    "token_budget_exhausted",
    "token_telemetry_integrity_failed",
    "uncached_input_budget_exhausted",
    "time_budget_exhausted"
  ]

  @type limits :: %{
          max_turns: pos_integer(),
          max_tokens: pos_integer() | nil,
          max_uncached_input_tokens: pos_integer() | nil,
          max_seconds: pos_integer() | nil
        }

  @spec from_agent_config(map()) :: limits()
  def from_agent_config(agent) when is_map(agent) do
    %{
      max_turns: Map.fetch!(agent, :max_turns),
      max_tokens: Map.get(agent, :max_run_tokens),
      max_uncached_input_tokens: Map.get(agent, :max_run_uncached_input_tokens),
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
      token_integrity_failed?(limits, metrics) -> "token_telemetry_integrity_failed"
      token_exhausted?(limits, metrics) -> "token_budget_exhausted"
      uncached_input_exhausted?(limits, metrics) -> "uncached_input_budget_exhausted"
      time_exhausted?(limits, metrics) -> "time_budget_exhausted"
      true -> nil
    end
  end

  @spec snapshot(limits(), map()) :: map()
  def snapshot(limits, metrics) when is_map(limits) and is_map(metrics) do
    turns_used = non_negative_integer(Map.get(metrics, :turns))
    seconds_used = non_negative_integer(Map.get(metrics, :seconds))
    reported_observed? = Map.get(metrics, :token_telemetry_observed, false) == true
    tokens_used = non_negative_integer(Map.get(metrics, :tokens))
    token_limit = Map.get(limits, :max_tokens)
    telemetry_integrity = telemetry_integrity(metrics, reported_observed?)
    integrity_failed? = telemetry_integrity == :failed
    telemetry_observed? = reported_observed? and telemetry_integrity == :valid

    uncached_reported_observed? =
      Map.get(metrics, :uncached_input_telemetry_observed, false) == true

    uncached_tokens_used = non_negative_integer(Map.get(metrics, :uncached_input_tokens))
    uncached_token_limit = Map.get(limits, :max_uncached_input_tokens)

    uncached_telemetry_integrity =
      uncached_telemetry_integrity(metrics, uncached_reported_observed?)

    uncached_integrity_failed? = uncached_telemetry_integrity == :failed

    uncached_telemetry_observed? =
      uncached_reported_observed? and uncached_telemetry_integrity == :valid

    %{
      turns: allowance(Map.fetch!(limits, :max_turns), turns_used),
      tokens: %{
        limit: token_limit,
        used: if(telemetry_observed? or integrity_failed?, do: tokens_used),
        remaining: if(telemetry_observed?, do: remaining(token_limit, tokens_used)),
        telemetry_observed: telemetry_observed?,
        telemetry_integrity: Atom.to_string(telemetry_integrity),
        integrity_error: integrity_error(metrics, integrity_failed?)
      },
      uncached_input_tokens: %{
        limit: uncached_token_limit,
        used:
          if(uncached_telemetry_observed? or uncached_integrity_failed?,
            do: uncached_tokens_used
          ),
        remaining:
          if(uncached_telemetry_observed?,
            do: remaining(uncached_token_limit, uncached_tokens_used)
          ),
        telemetry_observed: uncached_telemetry_observed?,
        telemetry_integrity: Atom.to_string(uncached_telemetry_integrity),
        integrity_error: uncached_integrity_error(metrics, uncached_integrity_failed?)
      },
      time: allowance(Map.get(limits, :max_seconds), seconds_used)
    }
  end

  defp token_integrity_failed?(limits, metrics) do
    total_failed? =
      is_integer(Map.get(limits, :max_tokens)) and
        telemetry_integrity(metrics, Map.get(metrics, :token_telemetry_observed, false) == true) ==
          :failed

    uncached_failed? =
      is_integer(Map.get(limits, :max_uncached_input_tokens)) and
        uncached_telemetry_integrity(
          metrics,
          Map.get(metrics, :uncached_input_telemetry_observed, false) == true
        ) == :failed

    total_failed? or uncached_failed?
  end

  defp token_exhausted?(limits, metrics) do
    limit = Map.get(limits, :max_tokens)
    observed? = Map.get(metrics, :token_telemetry_observed, false) == true
    used = non_negative_integer(Map.get(metrics, :tokens))
    is_integer(limit) and observed? and used >= limit
  end

  defp uncached_input_exhausted?(limits, metrics) do
    limit = Map.get(limits, :max_uncached_input_tokens)
    observed? = Map.get(metrics, :uncached_input_telemetry_observed, false) == true
    used = non_negative_integer(Map.get(metrics, :uncached_input_tokens))
    is_integer(limit) and observed? and used >= limit
  end

  defp time_exhausted?(limits, metrics) do
    limit = Map.get(limits, :max_seconds)
    used = non_negative_integer(Map.get(metrics, :seconds))
    is_integer(limit) and used >= limit
  end

  defp telemetry_integrity(metrics, observed?) do
    case Map.get(metrics, :token_telemetry_integrity) do
      integrity when integrity in [:unobserved, :valid, :failed] -> integrity
      _other -> if(observed?, do: :valid, else: :unobserved)
    end
  end

  defp uncached_telemetry_integrity(metrics, observed?) do
    case Map.get(metrics, :uncached_input_telemetry_integrity) do
      integrity when integrity in [:unobserved, :valid, :failed] -> integrity
      _other -> if(observed?, do: :valid, else: :unobserved)
    end
  end

  defp integrity_error(metrics, true) do
    case Map.get(metrics, :token_telemetry_failure) do
      failure when is_atom(failure) -> Atom.to_string(failure)
      failure when is_binary(failure) -> failure
      _other -> "unknown_integrity_failure"
    end
  end

  defp integrity_error(_metrics, false), do: nil

  defp uncached_integrity_error(metrics, true) do
    case Map.get(metrics, :uncached_input_telemetry_failure) do
      failure when is_atom(failure) -> Atom.to_string(failure)
      failure when is_binary(failure) -> failure
      _other -> "unknown_integrity_failure"
    end
  end

  defp uncached_integrity_error(_metrics, false), do: nil

  defp allowance(limit, used) do
    %{limit: limit, used: used, remaining: remaining(limit, used)}
  end

  defp remaining(nil, _used), do: nil
  defp remaining(limit, used), do: max(limit - used, 0)

  defp non_negative_integer(value) when is_integer(value), do: max(value, 0)
  defp non_negative_integer(_value), do: 0
end
