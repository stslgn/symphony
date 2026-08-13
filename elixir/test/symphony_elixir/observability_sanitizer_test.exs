defmodule SymphonyElixir.ObservabilitySanitizerTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ObservabilitySanitizer

  test "uses the public default for missing and opaque errors" do
    assert ObservabilitySanitizer.error_code(nil) == "runtime_error"
    assert ObservabilitySanitizer.error_code(123) == "runtime_error"
    assert ObservabilitySanitizer.error_code({"opaque", :details}) == "runtime_error"
  end

  test "extracts safe codes from protocol tuples" do
    assert ObservabilitySanitizer.error_code({:turn_failed, "model_overloaded", %{secret: true}}) ==
             "model_overloaded"

    assert ObservabilitySanitizer.error_code({:response_error, "request_failed"}) == "request_failed"
    assert ObservabilitySanitizer.error_code({:custom_failure, :details}) == "custom_failure"
    assert ObservabilitySanitizer.error_code({:custom_failure, :code, :details}) == "custom_failure"
  end

  test "normalizes nested map and atom candidates without leaking unsafe codes" do
    assert ObservabilitySanitizer.error_code(%{"error_code" => "remote_failure"}) ==
             "remote_failure"

    assert ObservabilitySanitizer.error_code(%{"reason" => %{code: :workspace_failure}}) ==
             "workspace_failure"

    assert ObservabilitySanitizer.error_code(%{error_code: "UPPER CASE"}, "safe_fallback") ==
             "safe_fallback"

    assert ObservabilitySanitizer.error_code(:worker_failure) == "worker_failure"
    assert ObservabilitySanitizer.error_code(%{}, "safe_fallback") == "safe_fallback"
  end

  test "maps non-binary retry errors to a bounded fallback" do
    assert ObservabilitySanitizer.retry_error_code(nil) == nil

    assert ObservabilitySanitizer.retry_error_code(:workspace_cleanup_failed) ==
             "workspace_cleanup_failed"

    assert ObservabilitySanitizer.retry_error_code(:workspace_preservation_required) ==
             "workspace_preservation_required"

    assert ObservabilitySanitizer.retry_error_code({"opaque", :details}) == "worker_failure"
  end
end
