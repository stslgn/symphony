defmodule SymphonyElixir.RunBudgetTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RunBudget

  test "builds limits from agent config" do
    assert RunBudget.from_agent_config(%{
             max_turns: 20,
             max_run_tokens: 250_000,
             max_run_uncached_input_tokens: 100_000,
             max_run_seconds: 7_200
           }) == %{
             max_turns: 20,
             max_tokens: 250_000,
             max_uncached_input_tokens: 100_000,
             max_seconds: 7_200
           }
  end

  test "enforces uncached input independently from total tokens" do
    limits = %{
      max_turns: 20,
      max_tokens: 350_000,
      max_uncached_input_tokens: 100_000,
      max_seconds: nil
    }

    metrics = %{
      tokens: 200_000,
      token_telemetry_observed: true,
      uncached_input_tokens: 100_000,
      uncached_input_telemetry_observed: true,
      uncached_input_telemetry_integrity: :valid
    }

    assert RunBudget.exhausted_reason(limits, metrics) ==
             "uncached_input_budget_exhausted"

    assert RunBudget.snapshot(limits, metrics).uncached_input_tokens == %{
             limit: 100_000,
             used: 100_000,
             remaining: 0,
             telemetry_observed: true,
             telemetry_integrity: "valid",
             integrity_error: nil
           }
  end

  test "fails closed on cached-input telemetry loss only when uncached guard is enabled" do
    metrics = %{
      uncached_input_tokens: 0,
      uncached_input_telemetry_observed: false,
      uncached_input_telemetry_integrity: :failed,
      uncached_input_telemetry_failure: :missing_cached_input_counter
    }

    enabled = %{
      max_turns: 20,
      max_tokens: nil,
      max_uncached_input_tokens: 100_000,
      max_seconds: nil
    }

    disabled = %{enabled | max_uncached_input_tokens: nil}

    assert RunBudget.exhausted_reason(enabled, metrics) ==
             "token_telemetry_integrity_failed"

    assert RunBudget.exhausted_reason(disabled, metrics) == nil

    assert RunBudget.snapshot(enabled, metrics).uncached_input_tokens == %{
             limit: 100_000,
             used: 0,
             remaining: nil,
             telemetry_observed: false,
             telemetry_integrity: "failed",
             integrity_error: "missing_cached_input_counter"
           }
  end

  test "enforces observed token and elapsed time limits" do
    limits = %{max_turns: 20, max_tokens: 100, max_seconds: 60}

    assert RunBudget.exhausted_reason(limits, %{
             tokens: 100,
             token_telemetry_observed: true,
             seconds: 1
           }) == "token_budget_exhausted"

    assert RunBudget.exhausted_reason(limits, %{
             tokens: 0,
             token_telemetry_observed: false,
             seconds: 60
           }) == "time_budget_exhausted"

    assert RunBudget.exhausted_reason(limits, %{
             tokens: 1_000,
             token_telemetry_observed: false,
             seconds: 1
           }) == nil
  end

  test "reports missing token telemetry without claiming verified zero usage" do
    snapshot =
      RunBudget.snapshot(
        %{max_turns: 20, max_tokens: 250_000, max_seconds: 7_200},
        %{turns: 3, tokens: 0, token_telemetry_observed: false, seconds: 120}
      )

    assert snapshot.turns == %{limit: 20, used: 3, remaining: 17}

    assert snapshot.tokens == %{
             limit: 250_000,
             used: nil,
             remaining: nil,
             telemetry_observed: false,
             telemetry_integrity: "unobserved",
             integrity_error: nil
           }

    assert snapshot.time == %{limit: 7_200, used: 120, remaining: 7_080}
  end

  test "normalizes absent metrics and supports disabled optional limits" do
    snapshot =
      RunBudget.snapshot(
        %{max_turns: 20, max_tokens: nil, max_seconds: nil},
        %{}
      )

    assert snapshot.turns == %{limit: 20, used: 0, remaining: 20}

    assert snapshot.tokens == %{
             limit: nil,
             used: nil,
             remaining: nil,
             telemetry_observed: false,
             telemetry_integrity: "unobserved",
             integrity_error: nil
           }

    assert snapshot.time == %{limit: nil, used: 0, remaining: nil}
  end

  test "lists only bounded terminal reasons" do
    assert RunBudget.terminal_reasons() == [
             "turn_budget_exhausted",
             "token_budget_exhausted",
             "token_telemetry_integrity_failed",
             "uncached_input_budget_exhausted",
             "time_budget_exhausted"
           ]

    assert RunBudget.valid_terminal_reason?("token_budget_exhausted")
    refute RunBudget.valid_terminal_reason?("worker_exit")
  end

  test "fails a configured token budget closed when telemetry integrity is lost" do
    limits = %{max_turns: 20, max_tokens: 250, max_seconds: nil}

    metrics = %{
      turns: 1,
      tokens: 200,
      token_telemetry_observed: false,
      token_telemetry_integrity: :failed,
      token_telemetry_failure: :ambiguous_counter_decrease,
      seconds: 2
    }

    assert RunBudget.exhausted_reason(limits, metrics) ==
             "token_telemetry_integrity_failed"

    assert RunBudget.snapshot(limits, metrics).tokens == %{
             limit: 250,
             used: 200,
             remaining: nil,
             telemetry_observed: false,
             telemetry_integrity: "failed",
             integrity_error: "ambiguous_counter_decrease"
           }

    assert RunBudget.exhausted_reason(%{limits | max_tokens: nil}, metrics) == nil
  end

  test "preserves bounded string integrity failures" do
    snapshot =
      RunBudget.snapshot(
        %{max_turns: 20, max_tokens: 250, max_seconds: nil},
        %{
          token_telemetry_integrity: :failed,
          token_telemetry_failure: "missing_usage_event"
        }
      )

    assert snapshot.tokens.integrity_error == "missing_usage_event"
  end

  test "uses a generic integrity failure for untyped details" do
    snapshot =
      RunBudget.snapshot(
        %{max_turns: 20, max_tokens: 250, max_seconds: nil},
        %{token_telemetry_integrity: :failed, token_telemetry_failure: %{raw: "hidden"}}
      )

    assert snapshot.tokens.integrity_error == "unknown_integrity_failure"
  end
end
