defmodule SymphonyElixir.Codex.ModelCatalog do
  @moduledoc """
  Discovers the authenticated Codex model catalog through app-server `model/list`.

  The catalog is intentionally narrow. It keeps only model identifiers, default
  selection metadata, and supported reasoning efforts. It never calls
  `config/read`, whose response may contain credentials from effective MCP
  configuration.
  """

  @first_request_id 10_000
  @max_pages 20
  @max_models 128
  @max_model_length 160
  @max_efforts_per_model 16
  @max_effort_length 64

  @type model_entry :: %{
          model: String.t(),
          default?: boolean(),
          default_reasoning_effort: String.t() | nil,
          supported_reasoning_efforts: [String.t()],
          upgrade: String.t() | nil
        }

  @type t :: %{
          source: :live | :unavailable,
          models: [model_entry()],
          default_model: String.t() | nil,
          fetched_at: DateTime.t() | nil,
          error: atom() | nil
        }

  @type request_fun ::
          (non_neg_integer(), String.t(), map() -> {:ok, map()} | {:error, term()})

  @spec discover(request_fun()) :: {:ok, t()} | {:error, term()}
  def discover(request_fun) when is_function(request_fun, 3) do
    with {:ok, models} <- fetch_pages(request_fun, nil, @first_request_id, [], MapSet.new(), 0),
         false <- models == [] do
      {:ok,
       %{
         source: :live,
         models: models,
         default_model: default_model(models),
         fetched_at: DateTime.utc_now() |> DateTime.truncate(:second),
         error: nil
       }}
    else
      true -> {:error, :empty_model_catalog}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec unavailable(term()) :: t()
  def unavailable(reason) do
    %{
      source: :unavailable,
      models: [],
      default_model: nil,
      fetched_at: nil,
      error: error_code(reason)
    }
  end

  @spec validate_resolution(t(), String.t() | nil, String.t() | nil) ::
          :ok | {:error, term()}
  def validate_resolution(%{source: :live, models: models}, model, reasoning_effort)
      when is_binary(model) do
    case Enum.find(models, &(&1.model == model)) do
      nil ->
        {:error, {:model_not_available, model}}

      entry ->
        validate_reasoning_effort(entry, reasoning_effort)
    end
  end

  def validate_resolution(%{source: :live}, _model, _reasoning_effort),
    do: {:error, :resolved_model_missing}

  def validate_resolution(_catalog, _model, _reasoning_effort), do: :ok

  @spec payload(t()) :: map()
  def payload(catalog) do
    %{
      source: Atom.to_string(catalog.source),
      default_model: catalog.default_model,
      fetched_at: iso8601(catalog.fetched_at),
      error: catalog.error && Atom.to_string(catalog.error),
      models:
        Enum.map(catalog.models, fn entry ->
          %{
            model: entry.model,
            default: entry.default?,
            default_reasoning_effort: entry.default_reasoning_effort,
            supported_reasoning_efforts: entry.supported_reasoning_efforts,
            upgrade: entry.upgrade
          }
        end)
    }
  end

  @spec error_code(term()) :: atom()
  def error_code(:response_timeout), do: :response_timeout
  def error_code({:port_exit, _status}), do: :app_server_exit
  def error_code({:response_error, _error}), do: :request_failed
  def error_code(:empty_model_catalog), do: :empty_model_catalog
  def error_code(:catalog_page_limit), do: :catalog_page_limit
  def error_code(:catalog_model_limit), do: :catalog_model_limit
  def error_code(_reason), do: :discovery_failed

  defp fetch_pages(_request_fun, _cursor, _request_id, _models, _seen, page)
       when page >= @max_pages do
    {:error, :catalog_page_limit}
  end

  defp fetch_pages(request_fun, cursor, request_id, models, seen, page) do
    params = %{"cursor" => cursor, "includeHidden" => false}

    with {:ok, %{"data" => data} = result} <- request_fun.(request_id, "model/list", params),
         true <- is_list(data),
         {:ok, next_models, next_seen} <- append_models(data, models, seen) do
      case Map.get(result, "nextCursor") do
        next_cursor when is_binary(next_cursor) and next_cursor != "" ->
          fetch_pages(
            request_fun,
            next_cursor,
            request_id + 1,
            next_models,
            next_seen,
            page + 1
          )

        _next_cursor ->
          {:ok, next_models}
      end
    else
      false -> {:error, :invalid_model_catalog}
      {:ok, _result} -> {:error, :invalid_model_catalog}
      {:error, reason} -> {:error, reason}
    end
  end

  defp append_models(data, models, seen) do
    Enum.reduce_while(data, {:ok, models, seen}, &append_model/2)
  end

  defp append_model(raw_entry, {:ok, entries, seen_models}) do
    case parse_model(raw_entry) do
      {:ok, entry} -> append_parsed_model(entry, entries, seen_models)
      :ignore -> {:cont, {:ok, entries, seen_models}}
    end
  end

  defp append_parsed_model(%{model: model} = entry, entries, seen_models) do
    cond do
      MapSet.member?(seen_models, model) ->
        {:cont, {:ok, entries, seen_models}}

      length(entries) >= @max_models ->
        {:halt, {:error, :catalog_model_limit}}

      true ->
        {:cont, {:ok, entries ++ [entry], MapSet.put(seen_models, model)}}
    end
  end

  defp parse_model(%{"model" => raw_model} = raw_entry) do
    with {:ok, model} <- bounded_identifier(raw_model, @max_model_length),
         {:ok, reasoning_efforts} <-
           parse_reasoning_efforts(Map.get(raw_entry, "supportedReasoningEfforts")) do
      {:ok,
       %{
         model: model,
         default?: Map.get(raw_entry, "isDefault") == true,
         default_reasoning_effort: optional_identifier(Map.get(raw_entry, "defaultReasoningEffort"), @max_effort_length),
         supported_reasoning_efforts: reasoning_efforts,
         upgrade: optional_identifier(Map.get(raw_entry, "upgrade"), @max_model_length)
       }}
    else
      :error -> :ignore
    end
  end

  defp parse_model(_raw_entry), do: :ignore

  defp parse_reasoning_efforts(efforts) when is_list(efforts) do
    parsed =
      efforts
      |> Enum.flat_map(fn
        %{"reasoningEffort" => effort} ->
          case bounded_identifier(effort, @max_effort_length) do
            {:ok, value} -> [value]
            :error -> []
          end

        %{"effort" => effort} ->
          case bounded_identifier(effort, @max_effort_length) do
            {:ok, value} -> [value]
            :error -> []
          end

        _entry ->
          []
      end)
      |> Enum.uniq()

    if length(parsed) <= @max_efforts_per_model, do: {:ok, parsed}, else: :error
  end

  defp parse_reasoning_efforts(_efforts), do: {:ok, []}

  defp bounded_identifier(value, max_length) when is_binary(value) do
    trimmed = String.trim(value)

    if trimmed != "" and String.length(trimmed) <= max_length and
         not String.contains?(trimmed, ["\n", "\r", <<0>>]) do
      {:ok, trimmed}
    else
      :error
    end
  end

  defp bounded_identifier(_value, _max_length), do: :error

  defp optional_identifier(nil, _max_length), do: nil

  defp optional_identifier(value, max_length) do
    case bounded_identifier(value, max_length) do
      {:ok, identifier} -> identifier
      :error -> nil
    end
  end

  defp default_model(models) do
    case Enum.find(models, & &1.default?) do
      %{model: model} -> model
      nil -> nil
    end
  end

  defp validate_reasoning_effort(_entry, nil), do: :ok

  defp validate_reasoning_effort(entry, reasoning_effort) when is_binary(reasoning_effort) do
    if reasoning_effort in entry.supported_reasoning_efforts do
      :ok
    else
      {:error, {:reasoning_effort_not_supported, entry.model, reasoning_effort, entry.supported_reasoning_efforts}}
    end
  end

  defp iso8601(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp iso8601(_value), do: nil
end
