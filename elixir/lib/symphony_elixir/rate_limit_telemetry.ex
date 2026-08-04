defmodule SymphonyElixir.RateLimitTelemetry do
  @moduledoc """
  Normalizes provider rate-limit telemetry before it reaches observability state.

  The returned map is newly constructed from documented scalar fields. Unknown
  keys and values outside the accepted types and bounds are discarded.
  """

  @identifier_pattern ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]{0,63}\z/
  @max_count 1_000_000_000_000
  @max_reset_seconds 31_536_000
  @max_reset_epoch 9_999_999_999
  @max_window_duration_mins 525_600
  @max_balance 1_000_000_000_000

  @type bucket :: %{
          optional(:remaining) => non_neg_integer(),
          optional(:limit) => non_neg_integer(),
          optional(:used_percent) => number(),
          optional(:window_duration_mins) => non_neg_integer(),
          optional(:reset_in_seconds) => non_neg_integer(),
          optional(:reset_at) => non_neg_integer() | String.t()
        }

  @type credits :: %{
          optional(:has_credits) => boolean(),
          optional(:unlimited) => boolean(),
          optional(:balance) => number()
        }

  @type t :: %{
          :limit_id => String.t(),
          optional(:primary) => bucket(),
          optional(:secondary) => bucket(),
          optional(:credits) => credits()
        }

  @doc """
  Converts a provider-controlled rate-limit map into the internal safe shape.
  """
  @spec normalize(term()) :: t() | nil
  def normalize(rate_limits) when is_map(rate_limits) do
    with {:ok, limit_id} <- normalize_identifier(rate_limits) do
      normalized =
        %{limit_id: limit_id}
        |> put_normalized_section(:primary, normalize_bucket(fetch_value(rate_limits, [:primary, "primary"])))
        |> put_normalized_section(
          :secondary,
          normalize_bucket(fetch_value(rate_limits, [:secondary, "secondary"]))
        )
        |> put_normalized_section(:credits, normalize_credits(fetch_value(rate_limits, [:credits, "credits"])))

      if map_size(normalized) > 1, do: normalized
    else
      _reason -> nil
    end
  end

  def normalize(_rate_limits), do: nil

  @doc """
  Rebuilds an allowlisted projection for an observability consumer.

  Keeping this separate from ingestion ensures an accidentally malformed or
  manually supplied snapshot still fails closed at the presentation boundary.
  """
  @spec project(term()) :: t() | nil
  def project(rate_limits), do: normalize(rate_limits)

  defp normalize_identifier(rate_limits) do
    case fetch_value(rate_limits, [
           :limit_id,
           "limit_id",
           :limitId,
           "limitId",
           :limit_name,
           "limit_name",
           :limitName,
           "limitName"
         ]) do
      {:ok, value} when is_binary(value) ->
        if Regex.match?(@identifier_pattern, value), do: {:ok, value}, else: :error

      _other ->
        :error
    end
  end

  defp normalize_bucket({:ok, bucket}) when is_map(bucket) do
    %{}
    |> put_validated(:remaining, bounded_integer(bucket, [:remaining, "remaining"], @max_count))
    |> put_validated(:limit, bounded_integer(bucket, [:limit, "limit"], @max_count))
    |> put_validated(
      :used_percent,
      bounded_number(bucket, [:used_percent, "used_percent", :usedPercent, "usedPercent"], 100)
    )
    |> put_validated(
      :window_duration_mins,
      bounded_integer(
        bucket,
        [:window_duration_mins, "window_duration_mins", :windowDurationMins, "windowDurationMins"],
        @max_window_duration_mins
      )
    )
    |> put_validated(
      :reset_in_seconds,
      bounded_integer(
        bucket,
        [:reset_in_seconds, "reset_in_seconds", :resetInSeconds, "resetInSeconds"],
        @max_reset_seconds
      )
    )
    |> put_validated(
      :reset_at,
      normalize_reset_at(
        fetch_value(bucket, [
          :reset_at,
          "reset_at",
          :resetAt,
          "resetAt",
          :resets_at,
          "resets_at",
          :resetsAt,
          "resetsAt"
        ])
      )
    )
    |> non_empty_map()
  end

  defp normalize_bucket(_bucket), do: nil

  defp normalize_credits({:ok, credits}) when is_map(credits) do
    %{}
    |> put_validated(
      :has_credits,
      boolean_value(credits, [:has_credits, "has_credits", :hasCredits, "hasCredits"])
    )
    |> put_validated(:unlimited, boolean_value(credits, [:unlimited, "unlimited"]))
    |> put_validated(:balance, bounded_number(credits, [:balance, "balance"], @max_balance))
    |> non_empty_map()
  end

  defp normalize_credits(_credits), do: nil

  defp bounded_integer(map, keys, maximum) do
    case fetch_value(map, keys) do
      {:ok, value} when is_integer(value) and value >= 0 and value <= maximum -> {:ok, value}
      _other -> :error
    end
  end

  defp bounded_number(map, keys, maximum) do
    case fetch_value(map, keys) do
      {:ok, value} when is_integer(value) and value >= 0 and value <= maximum ->
        {:ok, value}

      {:ok, value} when is_float(value) and value >= 0.0 and value <= maximum ->
        {:ok, value}

      _other ->
        :error
    end
  end

  defp boolean_value(map, keys) do
    case fetch_value(map, keys) do
      {:ok, value} when is_boolean(value) -> {:ok, value}
      _other -> :error
    end
  end

  defp normalize_reset_at({:ok, value})
       when is_integer(value) and value >= 0 and value <= @max_reset_epoch,
       do: {:ok, value}

  defp normalize_reset_at({:ok, value}) when is_binary(value) and byte_size(value) <= 40 do
    case DateTime.from_iso8601(value) do
      {:ok, _datetime, _offset} -> {:ok, value}
      _other -> :error
    end
  end

  defp normalize_reset_at(_value), do: :error

  defp fetch_value(map, keys) do
    Enum.reduce_while(keys, :error, fn key, _acc ->
      if Map.has_key?(map, key), do: {:halt, {:ok, Map.get(map, key)}}, else: {:cont, :error}
    end)
  end

  defp put_validated(map, key, {:ok, value}), do: Map.put(map, key, value)
  defp put_validated(map, _key, :error), do: map

  defp put_normalized_section(map, _key, nil), do: map
  defp put_normalized_section(map, key, value), do: Map.put(map, key, value)

  defp non_empty_map(map) when map_size(map) == 0, do: nil
  defp non_empty_map(map), do: map
end
