defmodule SymphonyElixir.Linear.Webhook do
  @moduledoc """
  Verifies Linear webhook authentication without treating payload data as state.
  """

  @max_age_ms 60_000

  @type verification_result ::
          {:ok, %{delivery_id: String.t(), event: String.t()}}
          | {:ignore, :unsupported_event}
          | {:error, :invalid_signature | :invalid_delivery_id | :stale_timestamp | :invalid_event}

  @spec verify(
          binary(),
          String.t() | nil,
          String.t() | nil,
          String.t() | nil,
          map(),
          String.t(),
          integer()
        ) :: verification_result()
  def verify(raw_body, signature, delivery_id, event, params, secret, now_ms)
      when is_binary(raw_body) and is_map(params) and is_binary(secret) and is_integer(now_ms) do
    with :ok <- verify_signature(raw_body, signature, secret),
         :ok <- verify_delivery_id(delivery_id),
         :ok <- verify_timestamp(params["webhookTimestamp"], now_ms),
         :ok <- verify_event_identity(event, params["type"]) do
      if event == "Issue" or (event == "Comment" and params["action"] == "create") do
        {:ok, %{delivery_id: delivery_id, event: event}}
      else
        {:ignore, :unsupported_event}
      end
    end
  end

  defp verify_signature(raw_body, signature, secret)
       when is_binary(signature) and byte_size(signature) == 64 and byte_size(secret) > 0 do
    expected = :crypto.mac(:hmac, :sha256, secret, raw_body)

    case Base.decode16(signature, case: :mixed) do
      {:ok, provided} when byte_size(provided) == byte_size(expected) ->
        if Plug.Crypto.secure_compare(expected, provided),
          do: :ok,
          else: {:error, :invalid_signature}

      _other ->
        {:error, :invalid_signature}
    end
  end

  defp verify_signature(_raw_body, _signature, _secret), do: {:error, :invalid_signature}

  defp verify_delivery_id(delivery_id) when is_binary(delivery_id) do
    case Ecto.UUID.cast(delivery_id) do
      {:ok, _uuid} -> :ok
      :error -> {:error, :invalid_delivery_id}
    end
  end

  defp verify_delivery_id(_delivery_id), do: {:error, :invalid_delivery_id}

  defp verify_timestamp(timestamp, now_ms) when is_integer(timestamp) do
    if abs(now_ms - timestamp) <= @max_age_ms,
      do: :ok,
      else: {:error, :stale_timestamp}
  end

  defp verify_timestamp(_timestamp, _now_ms), do: {:error, :stale_timestamp}

  defp verify_event_identity(event, event) when is_binary(event), do: :ok
  defp verify_event_identity(_header_event, _body_event), do: {:error, :invalid_event}
end
