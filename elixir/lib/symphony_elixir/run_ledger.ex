defmodule SymphonyElixir.RunLedger do
  @moduledoc """
  Append-only durable event ledger for Symphony run attempts.

  Events are deliberately bounded and schema-filtered. The ledger never accepts
  arbitrary prompts, agent output, credentials, or external comments.
  """

  require Logger

  @schema_version 1
  @terminal_transitions MapSet.new([
                          "run_completed",
                          "run_failed",
                          "run_interrupted",
                          "run_parked",
                          "run_stopped"
                        ])
  @allowed_fields MapSet.new([
                    :allowed_actions,
                    :attempt,
                    :issue_id,
                    :issue_identifier,
                    :parked_reason,
                    :run_id,
                    :runner_generation,
                    :stage,
                    :terminal_reason,
                    :tracker_state,
                    :transition,
                    :wait_id,
                    :worker_host,
                    :workspace_path
                  ])

  @spec default_path() :: Path.t()
  def default_path do
    Application.get_env(:symphony_elixir, :run_ledger_path, default_path_from_log_file())
  end

  @spec new_id(String.t()) :: String.t()
  def new_id(prefix) when is_binary(prefix) do
    random = :crypto.strong_rand_bytes(12) |> Base.url_encode64(padding: false)
    "#{prefix}_#{random}"
  end

  @spec append(Path.t(), map()) :: :ok | {:error, term()}
  def append(path, event) when is_binary(path) and is_map(event) do
    payload =
      event
      |> Map.take(MapSet.to_list(@allowed_fields))
      |> Map.put(:schema_version, @schema_version)
      |> Map.put(:event_id, new_id("evt"))
      |> Map.put(:occurred_at, DateTime.utc_now() |> DateTime.truncate(:millisecond))

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- ensure_private_file(path),
         {:ok, encoded} <- Jason.encode(payload) do
      append_synced(path, encoded <> "\n")
    end
  end

  @spec reconcile_startup(Path.t(), String.t()) ::
          {:ok, %{recovered_attempts: map(), parked: map()}} | {:error, term()}
  def reconcile_startup(path, runner_generation) do
    reconcile_startup(path, runner_generation, [])
  end

  @spec reconcile_startup(Path.t(), String.t(), keyword()) ::
          {:ok, %{recovered_attempts: map(), parked: map()}} | {:error, term()}
  def reconcile_startup(path, runner_generation, opts)
      when is_binary(path) and is_binary(runner_generation) and is_list(opts) do
    append_fn = Keyword.get(opts, :append_fn, &append/2)

    with {:ok, %{stale_runs: stale_runs, parked: parked}} <- recovery_state(path),
         :ok <- append_interrupted_runs(path, stale_runs, runner_generation, append_fn),
         :ok <-
           append_fn.(path, %{
             transition: "runner_started",
             stage: "startup",
             runner_generation: runner_generation
           }) do
      recovered_attempts =
        Enum.reduce(stale_runs, %{}, fn {_run_id, event}, acc ->
          issue_id = event["issue_id"]
          next_attempt = max(integer_value(event["attempt"], 0) + 1, 1)
          Map.update(acc, issue_id, next_attempt, &max(&1, next_attempt))
        end)

      {:ok, %{recovered_attempts: recovered_attempts, parked: parked}}
    end
  end

  @spec read_events(Path.t()) :: {:ok, [map()]} | {:error, term()}
  def read_events(path) when is_binary(path) do
    case File.read(path) do
      {:ok, contents} ->
        events =
          contents
          |> String.split("\n", trim: true)
          |> Enum.flat_map(&decode_line/1)

        {:ok, events}

      {:error, :enoent} ->
        {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp default_path_from_log_file do
    log_file =
      Application.get_env(
        :symphony_elixir,
        :log_file,
        SymphonyElixir.LogFile.default_log_file()
      )

    Path.join(Path.dirname(Path.expand(log_file)), "run-ledger.jsonl")
  end

  defp append_synced(path, contents), do: File.write(path, contents, [:append, :sync])

  defp ensure_private_file(path) do
    case File.open(path, [:write, :exclusive, :binary]) do
      {:ok, device} ->
        :ok = File.close(device)
        File.chmod(path, 0o600)

      {:error, :eexist} ->
        File.chmod(path, 0o600)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp recovery_state(path) do
    with {:ok, events} <- read_events(path) do
      states =
        Enum.reduce(events, %{}, fn event, acc ->
          run_id = event["run_id"]
          update_run_state(acc, run_id, event)
        end)

      unfinished =
        states
        |> Enum.filter(&unfinished_run?/1)
        |> Map.new(fn {run_id, state} -> {run_id, state.event} end)

      parked = Enum.reduce(events, %{}, &update_parked_state/2)

      {:ok, %{stale_runs: unfinished, parked: parked}}
    end
  end

  defp update_run_state(acc, run_id, event) when is_binary(run_id) do
    current = Map.get(acc, run_id, %{started: false, terminal: false, event: event})
    transition = event["transition"]

    Map.put(acc, run_id, %{
      started: current.started or transition in ["run_claimed", "run_started"],
      terminal: current.terminal or MapSet.member?(@terminal_transitions, transition),
      event: event
    })
  end

  defp update_run_state(acc, _run_id, _event), do: acc

  defp unfinished_run?({_run_id, state}), do: state.started and not state.terminal

  defp update_parked_state(%{"transition" => "run_parked", "issue_id" => issue_id} = event, acc)
       when is_binary(issue_id) do
    Map.put(acc, issue_id, event)
  end

  defp update_parked_state(
         %{"transition" => transition, "issue_id" => issue_id},
         acc
       )
       when transition in ["wait_resumed", "wait_released", "run_started"] and
              is_binary(issue_id) do
    Map.delete(acc, issue_id)
  end

  defp update_parked_state(_event, acc), do: acc

  defp append_interrupted_runs(path, stale_runs, runner_generation, append_fn) do
    Enum.reduce_while(stale_runs, :ok, fn {run_id, event}, :ok ->
      recovery_event = %{
        transition: "run_interrupted",
        stage: "released",
        terminal_reason: "runner_restarted",
        runner_generation: runner_generation,
        run_id: run_id,
        issue_id: event["issue_id"],
        issue_identifier: event["issue_identifier"],
        attempt: integer_value(event["attempt"], 0),
        worker_host: event["worker_host"],
        workspace_path: event["workspace_path"]
      }

      case append_fn.(path, recovery_event) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp decode_line(line) do
    case Jason.decode(line) do
      {:ok, event} when is_map(event) ->
        [event]

      {:ok, _other} ->
        Logger.warning("Ignoring non-object Symphony run ledger event")
        []

      {:error, reason} ->
        Logger.warning("Ignoring malformed Symphony run ledger event: #{Exception.message(reason)}")
        []
    end
  end

  defp integer_value(value, _default) when is_integer(value), do: value
  defp integer_value(_value, default), do: default
end
