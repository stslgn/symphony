defmodule SymphonyElixir.ObservabilitySanitizer do
  @moduledoc false

  @safe_identifier ~r/\A[A-Za-z0-9][A-Za-z0-9._:\/-]{0,95}\z/
  @safe_error_code ~r/\A[a-z][a-z0-9_-]{0,63}\z/
  @safe_protocol_method ~r/\A(?:account|codex\/event|item|thread|tool|turn)\/[A-Za-z0-9_\/-]{1,80}\z/
  @retry_error_codes ~w(agent_exit worker_stalled spawn_failed retry_poll_failed capacity_unavailable worker_failure workspace_cleanup_pending workspace_cleanup_failed workspace_affinity_missing)

  @spec protocol_method(term()) :: String.t() | nil
  def protocol_method(method) when is_binary(method) do
    if Regex.match?(@safe_protocol_method, method), do: method
  end

  def protocol_method(_method), do: nil

  @spec identifier(term()) :: String.t() | nil
  def identifier(value) when is_binary(value) do
    if Regex.match?(@safe_identifier, value), do: value
  end

  def identifier(_value), do: nil

  @spec error_code(term(), String.t()) :: String.t()
  def error_code(reason, fallback) do
    reason
    |> error_code_candidate()
    |> normalize_error_code(fallback)
  end

  @spec error_code(term()) :: String.t()
  def error_code(reason), do: error_code(reason, "runtime_error")

  @spec retry_error_code(term()) :: String.t() | nil
  def retry_error_code(nil), do: nil

  def retry_error_code(error) when is_binary(error) do
    cond do
      error in @retry_error_codes -> error
      String.starts_with?(error, "agent exited:") -> "agent_exit"
      String.starts_with?(error, "stalled for ") -> "worker_stalled"
      String.starts_with?(error, "failed to spawn agent:") -> "spawn_failed"
      String.starts_with?(error, "retry poll failed:") -> "retry_poll_failed"
      error == "no available orchestrator slots" -> "capacity_unavailable"
      true -> "worker_failure"
    end
  end

  def retry_error_code(error), do: error_code(error, "worker_failure")

  defp error_code_candidate({kind, code, _details})
       when kind in [:terminal_protocol_error, :app_server_error, :turn_failed, :response_error],
       do: code

  defp error_code_candidate({kind, code})
       when kind in [:terminal_protocol_error, :app_server_error, :turn_failed, :response_error],
       do: code

  defp error_code_candidate({kind, _details}) when is_atom(kind), do: kind
  defp error_code_candidate({kind, _code, _details}) when is_atom(kind), do: kind

  defp error_code_candidate(error) when is_tuple(error) and tuple_size(error) > 0 do
    case elem(error, 0) do
      kind when is_atom(kind) -> kind
      _kind -> nil
    end
  end

  defp error_code_candidate(%{} = error) do
    Map.get(error, "error_code") ||
      Map.get(error, :error_code) ||
      Map.get(error, "code") ||
      Map.get(error, :code) ||
      error_code_candidate(Map.get(error, "reason") || Map.get(error, :reason))
  end

  defp error_code_candidate(nil), do: nil
  defp error_code_candidate(error) when is_atom(error), do: error
  defp error_code_candidate(_error), do: nil

  defp normalize_error_code(nil, fallback), do: fallback

  defp normalize_error_code(code, fallback) when is_atom(code),
    do: normalize_error_code(Atom.to_string(code), fallback)

  defp normalize_error_code(code, fallback) when is_binary(code) do
    if Regex.match?(@safe_error_code, code), do: code, else: fallback
  end

  defp normalize_error_code(_code, fallback), do: fallback
end
