defmodule SymphonyElixir.ScenarioHarness do
  @moduledoc """
  Deterministic, offline rig for cross-feature Symphony scenarios.

  The harness drives the real orchestrator and ledger with an in-memory tracker
  and bounded fake Codex protocol. It never needs network access or credentials.
  """

  import ExUnit.Assertions

  alias SymphonyElixir.Linear.Issue
  alias SymphonyElixir.{OperatorWait, Orchestrator, RunLedger}

  defstruct [:name, :pid, :ledger_path]

  @type t :: %__MODULE__{
          name: GenServer.name(),
          pid: pid(),
          ledger_path: Path.t()
        }

  @spec start!(GenServer.name(), Path.t()) :: t()
  def start!(name, ledger_path) do
    {:ok, pid} = Orchestrator.start_link(name: name, run_ledger_path: ledger_path)
    %__MODULE__{name: name, pid: pid, ledger_path: ledger_path}
  end

  @spec stop(t()) :: :ok
  def stop(%__MODULE__{pid: pid}) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
    :ok
  end

  @spec await_snapshot(t(), (map() -> boolean()), non_neg_integer()) :: map()
  def await_snapshot(harness, predicate, timeout_ms \\ 2_000)
      when is_function(predicate, 1) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_snapshot(harness, predicate, deadline)
  end

  @spec await_poll_idle(t(), non_neg_integer()) :: map()
  def await_poll_idle(harness, timeout_ms \\ 2_000) do
    await_snapshot(
      harness,
      fn snapshot ->
        get_in(snapshot, [:polling, :checking?]) == false and
          is_integer(get_in(snapshot, [:polling, :next_poll_in_ms])) and
          get_in(snapshot, [:polling, :next_poll_in_ms]) > 0
      end,
      timeout_ms
    )
  end

  @spec events(t()) :: [map()]
  def events(%__MODULE__{ledger_path: path}) do
    {:ok, events} = RunLedger.read_events(path)
    events
  end

  @spec seed_running!(t(), Issue.t(), keyword()) :: map()
  def seed_running!(harness, %Issue{} = issue, opts \\ []) do
    run_id = Keyword.get(opts, :run_id, "run-scenario")
    max_tokens = Keyword.get(opts, :max_tokens)

    agent_pid =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    started_at = DateTime.utc_now()

    entry = %{
      run_id: run_id,
      pid: agent_pid,
      ref: nil,
      identifier: issue.identifier,
      issue: issue,
      worker_host: nil,
      workspace_path: Keyword.get(opts, :workspace_path),
      session_id: nil,
      session_title: nil,
      resolved_model: nil,
      reasoning_effort: nil,
      model_catalog_source: nil,
      model_catalog: nil,
      last_codex_message: nil,
      last_codex_timestamp: nil,
      last_codex_event: nil,
      codex_app_server_pid: nil,
      codex_input_tokens: 0,
      codex_output_tokens: 0,
      codex_total_tokens: 0,
      codex_last_reported_input_tokens: 0,
      codex_last_reported_output_tokens: 0,
      codex_last_reported_total_tokens: 0,
      codex_token_telemetry_observed: false,
      turn_count: 1,
      run_budget: %{max_turns: 20, max_tokens: max_tokens, max_seconds: nil},
      run_budget_timer_ref: nil,
      retry_attempt: 0,
      started_at: started_at
    }

    :ok =
      RunLedger.append(harness.ledger_path, %{
        transition: "run_claimed",
        stage: "claimed",
        run_id: run_id,
        issue_id: issue.id,
        issue_identifier: issue.identifier,
        attempt: 0
      })

    :ok =
      RunLedger.append(harness.ledger_path, %{
        transition: "run_started",
        stage: "running",
        run_id: run_id,
        issue_id: issue.id,
        issue_identifier: issue.identifier,
        attempt: 0
      })

    :sys.replace_state(harness.pid, fn state ->
      state
      |> Map.put(:running, Map.put(state.running, issue.id, entry))
      |> Map.put(:claimed, MapSet.put(state.claimed, issue.id))
    end)

    entry
  end

  @spec report_tokens(t(), Issue.t(), String.t(), non_neg_integer(), non_neg_integer()) :: :ok
  def report_tokens(harness, %Issue{} = issue, run_id, total_tokens, output_tokens \\ 0) do
    input_tokens = max(total_tokens - output_tokens, 0)

    send(
      harness.pid,
      {:codex_worker_update, issue.id,
       %{
         run_id: run_id,
         event: :notification,
         timestamp: DateTime.utc_now(),
         payload: %{
           "params" => %{
             "msg" => %{
               "payload" => %{
                 "info" => %{
                   "total_token_usage" => %{
                     "input_tokens" => input_tokens,
                     "output_tokens" => output_tokens,
                     "total_tokens" => total_tokens
                   }
                 }
               }
             }
           }
         }
       }}
    )

    :ok
  end

  @spec assert_consistent!(t()) :: :ok
  def assert_consistent!(harness) do
    snapshot = Orchestrator.snapshot(harness.name, 1_000)
    assert is_map(snapshot)

    assert_unique_issue_ids!(snapshot.running, "running")
    assert_unique_issue_ids!(snapshot.parked, "parked")
    assert_unique_issue_ids!(snapshot.retrying, "retrying")

    running_ids = ids(snapshot.running)
    parked_ids = ids(snapshot.parked)
    retrying_ids = ids(snapshot.retrying)

    assert MapSet.disjoint?(running_ids, parked_ids)
    assert MapSet.disjoint?(running_ids, retrying_ids)
    assert MapSet.disjoint?(parked_ids, retrying_ids)

    Enum.each(snapshot.parked, fn wait ->
      assert OperatorWait.valid_reason?(wait.reason)
      assert wait.allowed_actions == OperatorWait.allowed_actions(wait.reason)
    end)

    events = events(harness)

    events
    |> Enum.reject(&(is_nil(&1["run_id"]) or &1["run_id"] == ""))
    |> Enum.group_by(& &1["run_id"])
    |> Enum.each(fn {run_id, run_events} ->
      assert count_transition(run_events, "model_resolved") <= 1,
             "duplicate model_resolved for #{run_id}"

      if count_transition(run_events, "run_parked") > 0 do
        assert count_transition(run_events, "retry_scheduled") == 0,
               "parked run scheduled an implicit retry for #{run_id}"
      end

      terminal_count =
        Enum.count(run_events, fn event ->
          event["transition"] in [
            "run_completed",
            "run_failed",
            "run_interrupted",
            "run_parked",
            "run_stopped"
          ]
        end)

      assert terminal_count <= 1, "duplicate terminal transitions for #{run_id}"
    end)

    :ok
  end

  @spec write_fake_codex!(Path.t(), atom()) :: %{binary: Path.t(), trace: Path.t()}
  def write_fake_codex!(root, mode) when mode in [:live_ok, :live_mismatch, :unavailable] do
    File.mkdir_p!(root)
    binary = Path.join(root, "fake-codex-#{mode}")
    trace = Path.join(root, "fake-codex-#{mode}.trace")

    File.write!(binary, fake_codex_script(mode, trace))
    File.chmod!(binary, 0o755)

    %{binary: binary, trace: trace}
  end

  defp do_await_snapshot(harness, predicate, deadline) do
    snapshot = Orchestrator.snapshot(harness.name, 1_000)

    cond do
      is_map(snapshot) and predicate.(snapshot) ->
        snapshot

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("scenario timed out; last snapshot=#{inspect(snapshot, limit: 20)}")

      true ->
        Process.sleep(10)
        do_await_snapshot(harness, predicate, deadline)
    end
  end

  defp ids(entries) do
    entries
    |> Enum.map(& &1.issue_id)
    |> MapSet.new()
  end

  defp assert_unique_issue_ids!(entries, location) do
    issue_ids = Enum.map(entries, & &1.issue_id)

    duplicates =
      issue_ids
      |> Enum.frequencies()
      |> Enum.filter(fn {_issue_id, count} -> count > 1 end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort()

    assert duplicates == [],
           "duplicate issue ids in #{location}: #{Enum.join(duplicates, ", ")}"
  end

  defp count_transition(events, transition) do
    Enum.count(events, &(&1["transition"] == transition))
  end

  defp fake_codex_script(mode, trace) do
    model = if mode == :live_mismatch, do: "gpt-missing", else: "gpt-live"

    model_response =
      case mode do
        :unavailable ->
          ~s({"id":10000,"error":{"code":-32601,"message":"method not found"}})

        _ ->
          ~s({"id":10000,"result":{"data":[{"model":"gpt-live","isDefault":true,"defaultReasoningEffort":"low","supportedReasoningEfforts":[{"reasoningEffort":"low"}]}],"nextCursor":null}})
      end

    """
    #!/bin/sh
    trace_file=#{shell_quote(trace)}
    while IFS= read -r line; do
      printf 'JSON:%s\\n' "$line" >> "$trace_file"
      case "$line" in
        *'"method":"initialize"'*)
          printf '%s\\n' '{"id":1,"result":{}}'
          ;;
        *'"method":"initialized"'*)
          ;;
        *'"method":"model/list"'*)
          printf '%s\\n' '#{model_response}'
          ;;
        *'"method":"thread/start"'*)
          printf '%s\\n' '{"id":2,"result":{"thread":{"id":"thread-scenario"},"model":"#{model}","reasoningEffort":"low"}}'
          ;;
        *'"method":"turn/start"'*)
          printf '%s\\n' '{"id":3,"result":{"turn":{"id":"turn-scenario"}}}'
          printf '%s\\n' '{"method":"turn/completed","params":{"turn":{"id":"turn-scenario"}}}'
          ;;
      esac
    done
    """
  end

  defp shell_quote(value), do: "'" <> String.replace(value, "'", "'\"'\"'") <> "'"
end
