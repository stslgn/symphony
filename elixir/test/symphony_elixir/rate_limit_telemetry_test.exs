defmodule SymphonyElixir.RateLimitTelemetryTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RateLimitTelemetry

  test "accepts bounded integer numbers and ISO 8601 reset timestamps" do
    reset_at = "2026-08-05T12:30:00Z"

    assert RateLimitTelemetry.normalize(%{
             "limit_id" => "codex",
             "primary" => %{"usedPercent" => 42, "resetAt" => reset_at},
             "credits" => %{"balance" => 7}
           }) == %{
             limit_id: "codex",
             primary: %{used_percent: 42, reset_at: reset_at},
             credits: %{balance: 7}
           }
  end

  test "drops malformed reset timestamps and empty provider sections" do
    assert RateLimitTelemetry.normalize(%{
             limit_id: "codex",
             primary: %{reset_at: "not-a-timestamp"},
             secondary: %{},
             credits: %{balance: -1}
           }) == nil

    assert RateLimitTelemetry.normalize(%{
             limit_id: "codex",
             primary: %{reset_at: String.duplicate("1", 41)}
           }) == nil
  end

  test "projects malformed snapshots fail closed" do
    assert RateLimitTelemetry.project(%{limit_id: "codex", primary: :invalid}) == nil
    assert RateLimitTelemetry.project(%{limit_id: "bad identifier", primary: %{remaining: 1}}) == nil
    assert RateLimitTelemetry.project(:invalid) == nil
  end
end
