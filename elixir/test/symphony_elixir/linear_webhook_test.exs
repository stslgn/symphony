defmodule SymphonyElixir.LinearWebhookTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Linear.Webhook

  @secret "synthetic-webhook-secret"
  @delivery_id "234d1a4e-b617-4388-90fe-adc3633d6b72"
  @now_ms 1_700_000_000_000

  test "verifies the exact raw body and accepts a fresh Issue event" do
    body = body("Issue", @now_ms)

    assert {:ok, %{delivery_id: @delivery_id, event: "Issue"}} =
             Webhook.verify(
               body,
               signature(body),
               @delivery_id,
               "Issue",
               Jason.decode!(body),
               @secret,
               @now_ms
             )
  end

  test "rejects a signature made from a re-encoded body" do
    body = body("Issue", @now_ms)
    reencoded = body |> Jason.decode!() |> Jason.encode_to_iodata!() |> IO.iodata_to_binary()
    signature = signature(reencoded <> " ")

    assert {:error, :invalid_signature} =
             Webhook.verify(
               body,
               signature,
               @delivery_id,
               "Issue",
               Jason.decode!(body),
               @secret,
               @now_ms
             )
  end

  test "rejects missing or malformed signature inputs" do
    body = body("Issue", @now_ms)
    params = Jason.decode!(body)

    for signature <- [nil, "short", String.duplicate("z", 64)] do
      assert {:error, :invalid_signature} =
               Webhook.verify(
                 body,
                 signature,
                 @delivery_id,
                 "Issue",
                 params,
                 @secret,
                 @now_ms
               )
    end
  end

  test "rejects stale, future, and malformed timestamps" do
    for timestamp <- [@now_ms - 60_001, @now_ms + 60_001, "not-a-timestamp", nil] do
      body = body("Issue", timestamp)

      assert {:error, :stale_timestamp} =
               Webhook.verify(
                 body,
                 signature(body),
                 @delivery_id,
                 "Issue",
                 Jason.decode!(body),
                 @secret,
                 @now_ms
               )
    end
  end

  test "rejects malformed delivery identity" do
    body = body("Issue", @now_ms)

    for delivery_id <- ["not-a-uuid", nil] do
      assert {:error, :invalid_delivery_id} =
               Webhook.verify(
                 body,
                 signature(body),
                 delivery_id,
                 "Issue",
                 Jason.decode!(body),
                 @secret,
                 @now_ms
               )
    end
  end

  test "accepts a fresh Comment create event as an operator-command wake-up" do
    body = body("Comment", @now_ms)

    assert {:ok, %{delivery_id: @delivery_id, event: "Comment"}} =
             Webhook.verify(
               body,
               signature(body),
               @delivery_id,
               "Comment",
               Jason.decode!(body),
               @secret,
               @now_ms
             )
  end

  test "acknowledges unsupported verified events without requesting a wake-up" do
    body = body("Project", @now_ms)

    assert {:ignore, :unsupported_event} =
             Webhook.verify(
               body,
               signature(body),
               @delivery_id,
               "Project",
               Jason.decode!(body),
               @secret,
               @now_ms
             )
  end

  test "rejects a mismatch between signed body type and event header" do
    body = body("Comment", @now_ms)

    assert {:error, :invalid_event} =
             Webhook.verify(
               body,
               signature(body),
               @delivery_id,
               "Issue",
               Jason.decode!(body),
               @secret,
               @now_ms
             )
  end

  defp body(type, timestamp) do
    Jason.encode!(%{
      "action" => if(type == "Comment", do: "create", else: "update"),
      "data" => %{"id" => "issue-webhook"},
      "type" => type,
      "webhookTimestamp" => timestamp
    })
  end

  defp signature(body) do
    :crypto.mac(:hmac, :sha256, @secret, body)
    |> Base.encode16(case: :lower)
  end
end
