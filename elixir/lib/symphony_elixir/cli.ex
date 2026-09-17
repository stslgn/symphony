defmodule SymphonyElixir.CLI do
  @moduledoc """
  Escript entrypoint for running Symphony with an explicit WORKFLOW.md path.
  """

  alias SymphonyElixir.{LogFile, MergeLaneCLI, RuntimeIdentity, Workflow}

  @acknowledgement_switch :i_understand_that_this_will_be_running_without_the_usual_guardrails
  @startup_protocol "3"
  @managed_project_env "SYMPHONY_MANAGED_PROJECT"
  @expected_runtime_identity_env "SYMPHONY_EXPECTED_EXECUTION_SHA256"
  @verified_runtime_identity_env "SYMPHONY_VERIFIED_EXECUTION_SHA256"
  @switches [{@acknowledgement_switch, :boolean}, logs_root: :string, port: :integer]

  @type ensure_started_result :: {:ok, [atom()]} | {:error, term()}
  @type deps :: %{
          file_regular?: (String.t() -> boolean()),
          set_workflow_file_path: (String.t() -> :ok | {:error, term()}),
          set_logs_root: (String.t() -> :ok | {:error, term()}),
          set_server_port_override: (non_neg_integer() | nil -> :ok | {:error, term()}),
          ensure_all_started: (-> ensure_started_result())
        }

  @spec main([String.t()]) :: :ok | no_return()
  def main(["--startup-protocol"]) do
    IO.puts(startup_protocol())
  end

  def main(["--runtime-identity"]) do
    case RuntimeIdentity.evidence() do
      {:ok, evidence} ->
        IO.puts("image_sha256=#{evidence.image_sha256}")
        IO.puts("execution_sha256=#{evidence.execution_sha256}")

      {:error, reason} ->
        halt_runtime_identity(reason)
    end
  end

  def main(["--managed-workflow-identity", workflow_path, expected_project_slug]) do
    case managed_workflow_identity(workflow_path, expected_project_slug) do
      {:ok, operator_user_ids} -> Enum.each(operator_user_ids, &IO.puts/1)
      {:error, reason} -> halt_managed_workflow_identity(reason)
    end
  end

  def main(["merge-lane" | args]) do
    case admit_runtime_identity() do
      :ok -> run_merge_lane(args)
      {:error, reason} -> halt_runtime_identity(reason)
    end
  end

  def main(args) do
    case admit_runtime_identity() do
      :ok -> start_runtime(args)
      {:error, reason} -> halt_runtime_identity(reason)
    end
  end

  @spec startup_protocol() :: String.t()
  def startup_protocol, do: @startup_protocol

  @doc false
  @spec managed_workflow_identity(Path.t(), String.t()) ::
          {:ok, [String.t()]} | {:error, term()}
  def managed_workflow_identity(workflow_path, expected_project_slug)
      when is_binary(workflow_path) and is_binary(expected_project_slug) do
    with {:ok, %{config: config}} <- Workflow.load(workflow_path),
         tracker when is_map(tracker) <- Map.get(config, "tracker"),
         :ok <- validate_managed_tracker(tracker, expected_project_slug),
         operator_user_ids when is_list(operator_user_ids) <-
           Map.get(tracker, "operator_user_ids", []),
         true <- Enum.all?(operator_user_ids, &is_binary/1) do
      {:ok, operator_user_ids}
    else
      {:error, reason} ->
        {:error, reason}

      nil ->
        {:error, :tracker_must_be_a_map}

      tracker when not is_map(tracker) ->
        {:error, :tracker_must_be_a_map}

      operator_user_ids when not is_list(operator_user_ids) ->
        {:error, :operator_user_ids_must_be_a_list}

      false ->
        {:error, :operator_user_ids_must_contain_only_strings}
    end
  end

  @spec start_runtime([String.t()]) :: no_return()
  defp start_runtime(args) do
    case evaluate(args) do
      :ok ->
        wait_for_shutdown()

      {:error, message} ->
        IO.puts(:stderr, message)
        System.halt(1)
    end
  end

  defp admit_runtime_identity do
    if System.get_env(@managed_project_env) in [nil, ""] do
      System.delete_env(@verified_runtime_identity_env)
      :ok
    else
      expected = System.get_env(@expected_runtime_identity_env)

      with {:ok, actual} <- RuntimeIdentity.verify(expected) do
        System.put_env(@verified_runtime_identity_env, actual)
      end
    end
  end

  defp validate_managed_tracker(tracker, expected_project_slug) do
    endpoint = Map.get(tracker, "endpoint", "https://api.linear.app/graphql")
    webhook_secret = Map.get(tracker, "webhook_secret")
    assignee = Map.get(tracker, "assignee")

    cond do
      Map.get(tracker, "kind") != "linear" ->
        {:error, :tracker_kind_must_be_linear}

      Map.get(tracker, "api_key") != "$LINEAR_API_KEY" ->
        {:error, :tracker_api_key_must_use_linear_api_key_env}

      webhook_secret not in [nil, "$LINEAR_WEBHOOK_SECRET"] ->
        {:error, :tracker_webhook_secret_must_use_linear_webhook_secret_env}

      is_binary(assignee) and String.starts_with?(assignee, "$") ->
        {:error, :managed_tracker_assignee_must_not_use_ambient_env}

      endpoint != "https://api.linear.app/graphql" ->
        {:error, :tracker_endpoint_must_be_linear_graphql}

      Map.get(tracker, "project_slug") != expected_project_slug ->
        {:error, :tracker_project_slug_mismatch}

      true ->
        :ok
    end
  end

  defp run_merge_lane(args) do
    request_json = IO.read(:stdio, :eof)

    case MergeLaneCLI.run(args, request_json) do
      {:ok, response} ->
        IO.puts(Jason.encode!(response))
        :ok

      {:error, reason} ->
        IO.puts(:stderr, Jason.encode!(%{error: inspect(reason)}))
        System.halt(1)
    end
  end

  @spec halt_runtime_identity(term()) :: no_return()
  defp halt_runtime_identity(reason) do
    IO.puts(:stderr, "runtime_identity_unavailable: #{inspect(reason)}")
    System.halt(78)
  end

  defp halt_managed_workflow_identity(reason) do
    IO.puts(:stderr, "managed_workflow_identity_unavailable: #{inspect(reason)}")
    System.halt(65)
  end

  @spec evaluate([String.t()], deps()) :: :ok | {:error, String.t()}
  def evaluate(args, deps \\ runtime_deps()) do
    case OptionParser.parse(args, strict: @switches) do
      {opts, [], []} ->
        with :ok <- require_guardrails_acknowledgement(opts),
             :ok <- maybe_set_logs_root(opts, deps),
             :ok <- maybe_set_server_port(opts, deps) do
          run(Path.expand("WORKFLOW.md"), deps)
        end

      {opts, [workflow_path], []} ->
        with :ok <- require_guardrails_acknowledgement(opts),
             :ok <- maybe_set_logs_root(opts, deps),
             :ok <- maybe_set_server_port(opts, deps) do
          run(workflow_path, deps)
        end

      _ ->
        {:error, usage_message()}
    end
  end

  @spec run(String.t(), deps()) :: :ok | {:error, String.t()}
  def run(workflow_path, deps) do
    expanded_path = Path.expand(workflow_path)

    if deps.file_regular?.(expanded_path) do
      :ok = deps.set_workflow_file_path.(expanded_path)

      case deps.ensure_all_started.() do
        {:ok, _started_apps} ->
          :ok

        {:error, reason} ->
          {:error, "Failed to start Symphony with workflow #{expanded_path}: #{inspect(reason)}"}
      end
    else
      {:error, "Workflow file not found: #{expanded_path}"}
    end
  end

  @spec usage_message() :: String.t()
  defp usage_message do
    "Usage: symphony [--logs-root <path>] [--port <port>] [path-to-WORKFLOW.md]"
  end

  @spec runtime_deps() :: deps()
  defp runtime_deps do
    %{
      file_regular?: &File.regular?/1,
      set_workflow_file_path: &SymphonyElixir.Workflow.set_workflow_file_path/1,
      set_logs_root: &set_logs_root/1,
      set_server_port_override: &set_server_port_override/1,
      ensure_all_started: fn -> Application.ensure_all_started(:symphony_elixir) end
    }
  end

  defp maybe_set_logs_root(opts, deps) do
    case Keyword.get_values(opts, :logs_root) do
      [] ->
        :ok

      values ->
        logs_root = values |> List.last() |> String.trim()

        if logs_root == "" do
          {:error, usage_message()}
        else
          :ok = deps.set_logs_root.(Path.expand(logs_root))
        end
    end
  end

  defp require_guardrails_acknowledgement(opts) do
    if Keyword.get(opts, @acknowledgement_switch, false) do
      :ok
    else
      {:error, acknowledgement_banner()}
    end
  end

  @spec acknowledgement_banner() :: String.t()
  defp acknowledgement_banner do
    lines = [
      "This Symphony implementation is a low key engineering preview.",
      "Codex will run without any guardrails.",
      "SymphonyElixir is not a supported product and is presented as-is.",
      "To proceed, start with `--i-understand-that-this-will-be-running-without-the-usual-guardrails` CLI argument"
    ]

    width = Enum.max(Enum.map(lines, &String.length/1))
    border = String.duplicate("─", width + 2)
    top = "╭" <> border <> "╮"
    bottom = "╰" <> border <> "╯"
    spacer = "│ " <> String.duplicate(" ", width) <> " │"

    content =
      [
        top,
        spacer
        | Enum.map(lines, fn line ->
            "│ " <> String.pad_trailing(line, width) <> " │"
          end)
      ] ++ [spacer, bottom]

    [
      IO.ANSI.red(),
      IO.ANSI.bright(),
      Enum.join(content, "\n"),
      IO.ANSI.reset()
    ]
    |> IO.iodata_to_binary()
  end

  defp set_logs_root(logs_root) do
    Application.put_env(:symphony_elixir, :log_file, LogFile.default_log_file(logs_root))
    :ok
  end

  defp maybe_set_server_port(opts, deps) do
    case Keyword.get_values(opts, :port) do
      [] ->
        :ok

      values ->
        port = List.last(values)

        if is_integer(port) and port >= 0 do
          :ok = deps.set_server_port_override.(port)
        else
          {:error, usage_message()}
        end
    end
  end

  defp set_server_port_override(port) when is_integer(port) and port >= 0 do
    Application.put_env(:symphony_elixir, :server_port_override, port)
    :ok
  end

  @spec wait_for_shutdown() :: no_return()
  defp wait_for_shutdown do
    case Process.whereis(SymphonyElixir.Supervisor) do
      nil ->
        IO.puts(:stderr, "Symphony supervisor is not running")
        System.halt(1)

      pid ->
        ref = Process.monitor(pid)

        receive do
          {:DOWN, ^ref, :process, ^pid, reason} ->
            case reason do
              :normal -> System.halt(0)
              _ -> System.halt(1)
            end
        end
    end
  end
end
