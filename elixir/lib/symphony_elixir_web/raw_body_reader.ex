defmodule SymphonyElixirWeb.RawBodyReader do
  @moduledoc """
  Caches only Linear webhook request bodies for signature verification.
  """

  alias Plug.Conn

  @raw_body_key :symphony_linear_webhook_raw_body
  @webhook_path "/api/v1/webhooks/linear"

  @spec read_body(Conn.t(), keyword()) ::
          {:ok, binary(), Conn.t()} | {:more, binary(), Conn.t()} | {:error, term()}
  def read_body(conn, opts) do
    case Conn.read_body(conn, opts) do
      {:ok, body, conn} -> {:ok, body, cache_body(conn, body)}
      {:more, body, conn} -> {:more, body, cache_body(conn, body)}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec fetch_raw_body(Conn.t()) :: {:ok, binary()} | {:error, :raw_body_unavailable}
  def fetch_raw_body(%Conn{} = conn) do
    case conn.private[@raw_body_key] do
      chunks when is_list(chunks) and chunks != [] ->
        {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary()}

      _other ->
        {:error, :raw_body_unavailable}
    end
  end

  defp cache_body(%Conn{request_path: @webhook_path} = conn, body) when is_binary(body) do
    Conn.put_private(conn, @raw_body_key, [body | Map.get(conn.private, @raw_body_key, [])])
  end

  defp cache_body(conn, _body), do: conn
end
