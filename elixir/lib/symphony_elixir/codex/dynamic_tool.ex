defmodule SymphonyElixir.Codex.DynamicTool do
  @moduledoc """
  Executes client-side tool calls requested by Codex app-server turns.
  """

  alias SymphonyElixir.Linear.Client
  alias SymphonyElixir.Tracker

  @linear_graphql_tool "linear_graphql"
  @linear_graphql_description """
  Execute a raw GraphQL query or mutation against Linear using Symphony's configured auth.

  Prefer the issue context already provided in the prompt. Use this tool only
  for exact Linear reads or writes that are still required. Linear projects use
  `slugId`, not `slug`; repeated `__type` introspection fields must be aliased.
  Do not retry an unchanged failing query shape in a loop.
  """
  @linear_graphql_input_schema %{
    "type" => "object",
    "additionalProperties" => false,
    "required" => ["query"],
    "properties" => %{
      "query" => %{
        "type" => "string",
        "description" => "GraphQL query or mutation document to execute against Linear."
      },
      "variables" => %{
        "type" => ["object", "null"],
        "description" => "Optional GraphQL variables object.",
        "additionalProperties" => true
      }
    }
  }

  @spec execute(String.t() | nil, term(), keyword()) :: map()
  def execute(tool, arguments, opts \\ []) do
    case tool do
      @linear_graphql_tool ->
        execute_linear_graphql(arguments, opts)

      other ->
        failure_response(%{
          "error" => %{
            "message" => "Unsupported dynamic tool: #{inspect(other)}.",
            "supportedTools" => supported_tool_names()
          }
        })
    end
  end

  @spec denied_response(term(), [String.t()]) :: map()
  def denied_response(tool, allowed_tools) when is_list(allowed_tools) do
    failure_response(%{
      "error" => %{
        "message" => "Dynamic tool denied by Symphony capability policy: #{inspect(tool)}.",
        "allowedTools" => allowed_tools
      },
      "symphonyBoundary" => "dynamic_tool_allowlist"
    })
    |> Map.put("symphonyBoundary", "dynamic_tool_allowlist")
  end

  @spec supported_tool_names() :: [String.t()]
  def supported_tool_names, do: [@linear_graphql_tool]

  @spec tool_specs([String.t()]) :: [map()]
  def tool_specs(allowed_tools \\ supported_tool_names()) when is_list(allowed_tools) do
    specs = [
      %{
        "name" => @linear_graphql_tool,
        "description" => @linear_graphql_description,
        "inputSchema" => @linear_graphql_input_schema
      }
    ]

    Enum.filter(specs, &(Map.fetch!(&1, "name") in allowed_tools))
  end

  defp execute_linear_graphql(arguments, opts) do
    linear_client = Keyword.get(opts, :linear_client, &Client.graphql/3)

    with {:ok, tracker_context} <- fetch_tracker_context(opts),
         {:ok, query, variables} <- normalize_linear_graphql_arguments(arguments),
         :ok <- validate_linear_graphql_query(query),
         :ok <- validate_tracker_authority(tracker_context),
         {:ok, response} <-
           linear_client.(query, variables, tracker_context: tracker_context) do
      graphql_response(response)
    else
      {:error, reason} ->
        failure_response(tool_error_payload(reason))
    end
  end

  defp fetch_tracker_context(opts) do
    case Keyword.fetch(opts, :tracker_context) do
      {:ok, %Tracker.PollContext{} = context} -> {:ok, context}
      _ -> {:error, :tracker_context_required}
    end
  end

  defp validate_tracker_authority(tracker_context) do
    if Tracker.authority_valid?(tracker_context),
      do: :ok,
      else: {:error, :tracker_authority_invalidated}
  end

  defp normalize_linear_graphql_arguments(arguments) when is_binary(arguments) do
    case String.trim(arguments) do
      "" -> {:error, :missing_query}
      query -> {:ok, query, %{}}
    end
  end

  defp normalize_linear_graphql_arguments(arguments) when is_map(arguments) do
    case normalize_query(arguments) do
      {:ok, query} ->
        case normalize_variables(arguments) do
          {:ok, variables} ->
            {:ok, query, variables}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_linear_graphql_arguments(_arguments), do: {:error, :invalid_arguments}

  defp validate_linear_graphql_query(query) when is_binary(query) do
    cond do
      invalid_project_slug_query?(query) ->
        {:error, :invalid_project_slug_query}

      repeated_unaliased_type_introspection?(query) ->
        {:error, :unaliased_type_introspection_conflict}

      true ->
        :ok
    end
  end

  defp normalize_query(arguments) do
    case Map.get(arguments, "query") || Map.get(arguments, :query) do
      query when is_binary(query) ->
        case String.trim(query) do
          "" -> {:error, :missing_query}
          trimmed -> {:ok, trimmed}
        end

      _ ->
        {:error, :missing_query}
    end
  end

  defp normalize_variables(arguments) do
    case Map.get(arguments, "variables") || Map.get(arguments, :variables) || %{} do
      variables when is_map(variables) -> {:ok, variables}
      _ -> {:error, :invalid_variables}
    end
  end

  defp invalid_project_slug_query?(query) when is_binary(query) do
    Regex.match?(~r/project\s*(?:\([^)]*\))?\s*\{[^{}]*\bslug\b/s, query) or
      Regex.match?(~r/project\s*:\s*\{[^{}]*\bslug\s*:/s, query)
  end

  defp repeated_unaliased_type_introspection?(query) when is_binary(query) do
    query
    |> String.replace(~r/[A-Za-z_][A-Za-z0-9_]*\s*:\s*__type\s*\(/, "")
    |> then(&Regex.scan(~r/__type\s*\(/, &1))
    |> length()
    |> Kernel.>(1)
  end

  defp graphql_response(response) do
    success =
      case response do
        %{"errors" => errors} when is_list(errors) and errors != [] -> false
        %{errors: errors} when is_list(errors) and errors != [] -> false
        _ -> true
      end

    dynamic_tool_response(success, encode_payload(response))
  end

  defp failure_response(payload) do
    dynamic_tool_response(false, encode_payload(payload))
  end

  defp dynamic_tool_response(success, output) when is_boolean(success) and is_binary(output) do
    %{
      "success" => success,
      "output" => output,
      "contentItems" => [
        %{
          "type" => "inputText",
          "text" => output
        }
      ]
    }
  end

  defp encode_payload(payload) when is_map(payload) or is_list(payload) do
    Jason.encode!(payload, pretty: true)
  end

  defp encode_payload(payload), do: inspect(payload)

  defp tool_error_payload(:missing_query) do
    %{
      "error" => %{
        "message" => "`linear_graphql` requires a non-empty `query` string."
      }
    }
  end

  defp tool_error_payload(:invalid_arguments) do
    %{
      "error" => %{
        "message" => "`linear_graphql` expects either a GraphQL query string or an object with `query` and optional `variables`."
      }
    }
  end

  defp tool_error_payload(:invalid_variables) do
    %{
      "error" => %{
        "message" => "`linear_graphql.variables` must be a JSON object when provided."
      }
    }
  end

  defp tool_error_payload(:invalid_project_slug_query) do
    %{
      "error" => %{
        "message" => "Linear Project does not expose `slug`. Use `slugId` for project reads and filters."
      }
    }
  end

  defp tool_error_payload(:unaliased_type_introspection_conflict) do
    %{
      "error" => %{
        "message" => "Repeated `__type` introspection fields must use GraphQL aliases."
      }
    }
  end

  defp tool_error_payload(:missing_linear_api_token) do
    %{
      "error" => %{
        "message" => "Symphony is missing Linear auth. Set `linear.api_key` in `WORKFLOW.md` or export `LINEAR_API_KEY`."
      }
    }
  end

  defp tool_error_payload(:tracker_authority_invalidated) do
    %{
      "error" => %{
        "message" => "Linear access is disabled because tracker authority changed after runner startup. Restart the managed runner before retrying."
      },
      "symphonyBoundary" => "tracker_authority_generation"
    }
  end

  defp tool_error_payload(:tracker_context_required) do
    %{
      "error" => %{
        "message" => "Linear access requires the tracker context admitted when the worker session started."
      },
      "symphonyBoundary" => "tracker_context"
    }
  end

  defp tool_error_payload({:linear_api_status, status}) do
    %{
      "error" => %{
        "message" => "Linear GraphQL request failed with HTTP #{status}.",
        "status" => status
      }
    }
  end

  defp tool_error_payload({:linear_api_request, reason}) do
    %{
      "error" => %{
        "message" => "Linear GraphQL request failed before receiving a successful response.",
        "reason" => inspect(reason)
      }
    }
  end

  defp tool_error_payload(reason) do
    %{
      "error" => %{
        "message" => "Linear GraphQL tool execution failed.",
        "reason" => inspect(reason)
      }
    }
  end
end
