defmodule SymphonyElixir.RawBodyReaderTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias SymphonyElixirWeb.RawBodyReader

  defmodule ErrorBodyAdapter do
    def read_req_body(_state, _opts), do: {:error, :closed}
  end

  test "collects a chunked Linear webhook body in exact wire order" do
    conn = conn(:post, "/api/v1/webhooks/linear", "abcdef")

    assert {:more, "abc", conn} = RawBodyReader.read_body(conn, length: 3)
    assert {:ok, "def", conn} = RawBodyReader.read_body(conn, length: 3)
    assert {:ok, "abcdef"} = RawBodyReader.fetch_raw_body(conn)
  end

  test "does not cache bodies outside the Linear webhook route" do
    conn = conn(:post, "/api/v1/refresh", "{}")

    assert {:ok, "{}", conn} = RawBodyReader.read_body(conn, [])
    assert {:error, :raw_body_unavailable} = RawBodyReader.fetch_raw_body(conn)
  end

  test "propagates request body adapter errors" do
    conn = %Plug.Conn{adapter: {ErrorBodyAdapter, :state}}

    assert {:error, :closed} = RawBodyReader.read_body(conn, [])
  end
end
