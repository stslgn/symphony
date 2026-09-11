defmodule SymphonyElixir.MergeLaneCLI do
  @moduledoc """
  Bounded JSON command interface for session-coordinated merge-lane claims.
  """

  alias SymphonyElixir.MergeLane

  @max_request_bytes 16_384
  @switches [logs_root: :string]

  @spec run([String.t()], String.t()) :: {:ok, map()} | {:error, term()}
  def run(args, request_json) when is_list(args) and is_binary(request_json) do
    with :ok <- validate_request_size(request_json),
         {:ok, action, logs_root} <- parse_args(args),
         {:ok, request} <- decode_request(request_json) do
      dispatch(action, ledger_path(logs_root), request)
    end
  end

  def run(_args, _request_json), do: {:error, :invalid_merge_lane_command}

  defp parse_args(args) do
    case OptionParser.parse(args, strict: @switches) do
      {opts, [action], []} when action in ["claim", "transition", "recover", "history"] ->
        case opts |> Keyword.get(:logs_root) |> normalize_logs_root() do
          {:ok, logs_root} -> {:ok, action, logs_root}
          {:error, reason} -> {:error, reason}
        end

      _other ->
        {:error, :invalid_merge_lane_command}
    end
  end

  defp normalize_logs_root(logs_root) when is_binary(logs_root) do
    case String.trim(logs_root) do
      "" -> {:error, :invalid_logs_root}
      root -> {:ok, Path.expand(root)}
    end
  end

  defp normalize_logs_root(_logs_root), do: {:error, :invalid_logs_root}

  defp decode_request(request_json) do
    case Jason.decode(request_json) do
      {:ok, request} when is_map(request) -> {:ok, request}
      _other -> {:error, :invalid_merge_lane_request}
    end
  end

  defp dispatch("claim", path, request), do: MergeLane.claim(path, request)

  defp dispatch("transition", path, request) do
    MergeLane.transition(
      path,
      request["claim_id"],
      request["fencing_token"],
      request["target_state"],
      request["owner_capability"],
      request["evidence"] || %{}
    )
  end

  defp dispatch("recover", path, request) do
    MergeLane.recover(
      path,
      request["claim_id"],
      request["fencing_token"],
      request["action"],
      request["owner_capability"],
      request["evidence"] || %{}
    )
  end

  defp dispatch("history", path, request) do
    case MergeLane.history(path, request["limit"] || 1_000) do
      {:ok, events} -> {:ok, %{"events" => events}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ledger_path(logs_root) do
    logs_root
    |> Path.join("run-ledger.jsonl")
    |> MergeLane.default_path()
  end

  defp validate_request_size(request_json) do
    if byte_size(request_json) <= @max_request_bytes,
      do: :ok,
      else: {:error, :merge_lane_request_too_large}
  end
end
