defmodule SymphonyElixir.RunLedger do
  @moduledoc """
  Append-only durable event ledger for Symphony run attempts.

  Events are deliberately bounded and schema-filtered. The ledger never accepts
  arbitrary prompts, agent output, credentials, or external comments.
  """

  @schema_version 1
  @terminal_transitions MapSet.new([
                          "run_completed",
                          "run_failed",
                          "run_interrupted",
                          "run_parked",
                          "run_stopped"
                        ])
  @transition_stages %{
    "runner_started" => ["startup"],
    "run_claimed" => ["claimed"],
    "run_started" => ["running"],
    "run_runtime_ready" => ["running"],
    "model_resolved" => ["running"],
    "run_completed" => ["released"],
    "run_failed" => ["released"],
    "run_interrupted" => ["released"],
    "run_parked" => ["parked"],
    "run_stopped" => ["released"],
    "retry_scheduled" => ["retry_queued"],
    "wait_resumed" => ["parked", "resume_queued"],
    "resume_queued" => ["resume_queued"],
    "wait_rejected" => ["parked"],
    "wait_released" => ["parked"],
    "dispatch_paused" => ["operator"],
    "dispatch_resumed" => ["operator"],
    "operator_cursor_initialized" => ["operator"],
    "operator_cursor_advanced" => ["operator"],
    "operator_command_applied" => ["operator"],
    "operator_command_rejected" => ["operator"]
  }
  @terminal_reasons %{
    "run_completed" => MapSet.new(["worker_completed"]),
    "run_failed" => MapSet.new(["spawn_failed", "worker_exit"]),
    "run_interrupted" => MapSet.new(["runner_restarted"]),
    "run_stopped" =>
      MapSet.new([
        "stall_timeout",
        "tracker_non_active",
        "tracker_not_visible",
        "tracker_terminal",
        "worker_route_removed"
      ]),
    "wait_released" => MapSet.new(["tracker_released", "tracker_terminal", "worker_route_removed"])
  }
  @park_terminal_reasons MapSet.new([
                           "operator_stop",
                           "time_budget_exhausted",
                           "token_budget_exhausted",
                           "turn_budget_exhausted"
                         ])
  @transition_schemas %{
    "runner_started" => %{required_strings: ~w(stage runner_generation)},
    "run_claimed" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier),
      required_attempt: true
    },
    "run_started" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier),
      required_attempt: true
    },
    "run_runtime_ready" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier),
      required_attempt: true
    },
    "model_resolved" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier resolved_model),
      required_attempt: true
    },
    "run_completed" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier terminal_reason),
      required_attempt: true
    },
    "run_failed" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier terminal_reason),
      required_attempt: true
    },
    "run_interrupted" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier terminal_reason),
      required_attempt: true
    },
    "run_parked" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier wait_id parked_reason),
      required_attempt: true,
      typed_wait: true
    },
    "run_stopped" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier terminal_reason),
      required_attempt: true
    },
    "retry_scheduled" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier),
      required_attempt: true,
      required_next_attempt: true
    },
    "wait_resumed" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier wait_id parked_reason),
      required_attempt: true,
      typed_wait: true
    },
    "resume_queued" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier wait_id parked_reason),
      required_attempt: true,
      typed_wait: true
    },
    "wait_rejected" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier wait_id parked_reason),
      required_attempt: true,
      typed_wait: true
    },
    "wait_released" => %{
      required_strings: ~w(stage run_id issue_id issue_identifier wait_id parked_reason terminal_reason),
      required_attempt: true,
      typed_wait: true
    },
    "dispatch_paused" => %{required_strings: ~w(stage runner_generation)},
    "dispatch_resumed" => %{required_strings: ~w(stage runner_generation)},
    "operator_cursor_initialized" => %{
      required_strings: ~w(stage issue_id comment_created_at runner_generation),
      timestamp_field: "comment_created_at"
    },
    "operator_cursor_advanced" => %{
      required_strings: ~w(stage issue_id comment_id comment_created_at runner_generation),
      timestamp_field: "comment_created_at"
    },
    "operator_command_applied" => %{
      required_strings: ~w(stage issue_id comment_id comment_created_at operator_command runner_generation),
      timestamp_field: "comment_created_at"
    },
    "operator_command_rejected" => %{
      required_strings: ~w(stage issue_id comment_id comment_created_at operator_command runner_generation),
      timestamp_field: "comment_created_at"
    }
  }
  @allowed_fields MapSet.new([
                    :allowed_actions,
                    :attempt,
                    :comment_created_at,
                    :comment_id,
                    :issue_id,
                    :issue_identifier,
                    :operator_command,
                    :parked_reason,
                    :model_catalog_source,
                    :next_action,
                    :next_attempt,
                    :reasoning_effort,
                    :resolved_model,
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
  @persisted_fields MapSet.union(
                      MapSet.new(Enum.map(@allowed_fields, &Atom.to_string/1)),
                      MapSet.new(["event_id", "occurred_at", "schema_version"])
                    )
  @optional_string_fields @allowed_fields
                          |> MapSet.delete(:allowed_actions)
                          |> MapSet.delete(:attempt)
                          |> MapSet.delete(:next_attempt)
                          |> Enum.map(&Atom.to_string/1)

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
      |> Map.put(
        :occurred_at,
        DateTime.utc_now() |> DateTime.truncate(:millisecond) |> DateTime.to_iso8601()
      )

    with :ok <- validate_event_payload(stringify_keys(payload)),
         :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- ensure_private_file(path),
         {:ok, encoded} <- Jason.encode(payload) do
      append_synced(path, encoded <> "\n")
    end
  end

  @spec reconcile_startup(Path.t(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def reconcile_startup(path, runner_generation) do
    reconcile_startup(path, runner_generation, [])
  end

  @spec reconcile_startup(Path.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def reconcile_startup(path, runner_generation, opts)
      when is_binary(path) and is_binary(runner_generation) and is_list(opts) do
    append_fn = Keyword.get(opts, :append_fn, &append/2)

    with {:ok, recovery} <- recovery_state(path),
         %{stale_runs: stale_runs, parked: parked} = recovery,
         :ok <- append_interrupted_runs(path, stale_runs, runner_generation, append_fn),
         :ok <-
           append_fn.(path, %{
             transition: "runner_started",
             stage: "startup",
             runner_generation: runner_generation
           }) do
      recovered_dispatches =
        recovery.recovered_dispatches
        |> Map.merge(recovered_dispatches_from_stale_runs(stale_runs), fn _issue_id, left, right ->
          if left.attempt >= right.attempt, do: left, else: right
        end)

      recovered_attempts =
        Map.new(recovered_dispatches, fn {issue_id, dispatch} ->
          {issue_id, dispatch.attempt}
        end)

      {:ok,
       %{
         recovered_attempts: recovered_attempts,
         recovered_dispatches: recovered_dispatches,
         queued_resumes: recovery.queued_resumes,
         parked: parked,
         dispatch_paused: recovery.dispatch_paused,
         processed_operator_comment_ids: recovery.processed_operator_comment_ids,
         operator_comment_cursors: recovery.operator_comment_cursors
       }}
    end
  end

  @spec read_events(Path.t()) :: {:ok, [map()]} | {:error, term()}
  def read_events(path) when is_binary(path) do
    case File.read(path) do
      {:ok, contents} ->
        contents
        |> ledger_lines()
        |> Enum.with_index(1)
        |> Enum.reduce_while({:ok, []}, fn {line, line_number}, {:ok, events} ->
          case decode_line(line, line_number) do
            {:ok, event} -> {:cont, {:ok, [event | events]}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
        |> case do
          {:ok, events} ->
            events = Enum.reverse(events)

            with :ok <- validate_ordered_events(events) do
              {:ok, events}
            end

          {:error, reason} ->
            {:error, reason}
        end

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

      queued_resumes = Enum.reduce(events, %{}, &update_queued_resume_state/2)

      recovered_dispatches = Enum.reduce(events, %{}, &update_recovered_dispatch_state/2)

      dispatch_paused = Enum.reduce(events, false, &update_dispatch_pause_state/2)

      processed_operator_comment_ids =
        Enum.reduce(events, MapSet.new(), &update_processed_operator_comments/2)

      operator_comment_cursors =
        Enum.reduce(events, %{}, &update_operator_comment_cursor/2)

      {:ok,
       %{
         stale_runs: unfinished,
         parked: parked,
         queued_resumes: queued_resumes,
         recovered_dispatches: recovered_dispatches,
         dispatch_paused: dispatch_paused,
         processed_operator_comment_ids: processed_operator_comment_ids,
         operator_comment_cursors: operator_comment_cursors
       }}
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
       when transition in ["wait_resumed", "resume_queued", "wait_released", "run_started"] and
              is_binary(issue_id) do
    Map.delete(acc, issue_id)
  end

  defp update_parked_state(_event, acc), do: acc

  defp update_queued_resume_state(
         %{"transition" => transition, "issue_id" => issue_id, "attempt" => attempt} = event,
         acc
       )
       when transition in ["wait_resumed", "resume_queued"] and is_binary(issue_id) and
              is_integer(attempt) and attempt >= 1 do
    Map.update(acc, issue_id, event, fn current ->
      if current["attempt"] >= attempt, do: current, else: event
    end)
  end

  defp update_queued_resume_state(
         %{"transition" => transition, "issue_id" => issue_id},
         acc
       )
       when transition in ["run_claimed", "run_started", "run_parked", "wait_released"] and
              is_binary(issue_id) do
    Map.delete(acc, issue_id)
  end

  defp update_queued_resume_state(_event, acc), do: acc

  defp update_recovered_dispatch_state(
         %{
           "transition" => transition,
           "next_action" => next_action,
           "next_attempt" => next_attempt,
           "issue_id" => issue_id
         } = event,
         acc
       )
       when transition in ["run_completed", "run_failed", "run_stopped"] and
              next_action in ["continuation", "retry"] and is_integer(next_attempt) and
              next_attempt >= 1 and is_binary(issue_id) do
    dispatch = recovered_next_action_dispatch(event, next_attempt)
    Map.update(acc, issue_id, dispatch, &prefer_recovered_dispatch(&1, dispatch))
  end

  defp update_recovered_dispatch_state(
         %{
           "transition" => "retry_scheduled",
           "next_attempt" => next_attempt,
           "issue_id" => issue_id
         } = event,
         acc
       )
       when is_integer(next_attempt) and next_attempt >= 1 and is_binary(issue_id) do
    dispatch = recovered_next_action_dispatch(event, next_attempt)
    Map.update(acc, issue_id, dispatch, &prefer_recovered_dispatch(&1, dispatch))
  end

  defp update_recovered_dispatch_state(
         %{
           "transition" => "run_interrupted",
           "terminal_reason" => "runner_restarted",
           "issue_id" => issue_id
         } = event,
         acc
       )
       when is_binary(issue_id) do
    dispatch = recovered_dispatch(event)

    Map.update(acc, issue_id, dispatch, fn current ->
      if current.attempt >= dispatch.attempt, do: current, else: dispatch
    end)
  end

  defp update_recovered_dispatch_state(
         %{"transition" => transition, "issue_id" => issue_id},
         acc
       )
       when transition in ["run_claimed", "run_started", "run_parked", "wait_released"] and
              is_binary(issue_id) do
    Map.delete(acc, issue_id)
  end

  defp update_recovered_dispatch_state(_event, acc), do: acc

  defp recovered_dispatches_from_stale_runs(stale_runs) do
    Enum.reduce(stale_runs, %{}, fn {_run_id, event}, acc ->
      issue_id = event["issue_id"]
      dispatch = recovered_dispatch(event)

      Map.update(acc, issue_id, dispatch, &prefer_recovered_dispatch(&1, dispatch))
    end)
  end

  defp prefer_recovered_dispatch(current, candidate) do
    if current.attempt >= candidate.attempt, do: current, else: candidate
  end

  defp recovered_dispatch(event) do
    %{
      attempt: max(integer_value(event["attempt"], 0) + 1, 1),
      previous_run_id: event["run_id"],
      identifier: event["issue_identifier"],
      worker_host: event["worker_host"],
      workspace_path: event["workspace_path"],
      stage: "recovery_queued"
    }
  end

  defp recovered_next_action_dispatch(event, attempt) do
    %{
      attempt: attempt,
      previous_run_id: event["run_id"],
      identifier: event["issue_identifier"],
      worker_host: event["worker_host"],
      workspace_path: event["workspace_path"],
      stage: "retry_queued"
    }
  end

  defp update_dispatch_pause_state(%{"transition" => "dispatch_paused"}, _paused),
    do: true

  defp update_dispatch_pause_state(%{"transition" => "dispatch_resumed"}, _paused),
    do: false

  defp update_dispatch_pause_state(_event, paused), do: paused

  defp update_processed_operator_comments(
         %{"transition" => transition, "comment_id" => comment_id},
         processed
       )
       when transition in ["operator_command_applied", "operator_command_rejected"] and
              is_binary(comment_id) do
    MapSet.put(processed, comment_id)
  end

  defp update_processed_operator_comments(_event, processed), do: processed

  defp update_operator_comment_cursor(
         %{
           "transition" => transition,
           "issue_id" => issue_id,
           "comment_created_at" => created_at
         } = event,
         cursors
       )
       when transition in ["operator_cursor_initialized", "operator_cursor_advanced"] and
              is_binary(issue_id) and is_binary(created_at) do
    comment_id = event["comment_id"]

    updated_cursor =
      case Map.get(cursors, issue_id) do
        %{created_at: current_created_at, comment_ids: comment_ids}
        when created_at == current_created_at ->
          %{
            created_at: created_at,
            comment_ids: maybe_put_comment_id(comment_ids, comment_id)
          }

        %{created_at: current_created_at} = current_cursor
        when created_at < current_created_at ->
          current_cursor

        _cursor ->
          %{
            created_at: created_at,
            comment_ids: maybe_put_comment_id(MapSet.new(), comment_id)
          }
      end

    Map.put(cursors, issue_id, updated_cursor)
  end

  defp update_operator_comment_cursor(_event, cursors), do: cursors

  defp maybe_put_comment_id(comment_ids, comment_id) when is_binary(comment_id),
    do: MapSet.put(comment_ids, comment_id)

  defp maybe_put_comment_id(comment_ids, _comment_id), do: comment_ids

  defp append_interrupted_runs(path, stale_runs, runner_generation, append_fn) do
    Enum.reduce_while(stale_runs, :ok, fn {run_id, event}, :ok ->
      recovery_event = %{
        transition: "run_interrupted",
        stage: "released",
        terminal_reason: "runner_restarted",
        next_action: "retry",
        next_attempt: integer_value(event["attempt"], 0) + 1,
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

  defp ledger_lines(""), do: []

  defp ledger_lines(contents) do
    lines = String.split(contents, "\n", trim: false)

    if List.last(lines) == "" do
      List.delete_at(lines, -1)
    else
      lines
    end
  end

  defp decode_line(line, line_number) do
    case Jason.decode(line) do
      {:ok, event} when is_map(event) ->
        validate_event(event, line_number)

      {:ok, _other} ->
        {:error, {:invalid_ledger_record, line_number, :not_an_object}}

      {:error, _reason} ->
        {:error, {:invalid_ledger_record, line_number, :malformed_json}}
    end
  end

  defp validate_event(event, line_number) do
    case validate_event_payload(event) do
      :ok -> {:ok, event}
      {:error, reason} -> {:error, {:invalid_ledger_record, line_number, reason}}
    end
  end

  defp validate_event_payload(event) do
    with :ok <- validate_event_fields(event),
         :ok <- validate_schema_version(event),
         :ok <- validate_required_string(event, "event_id"),
         :ok <- validate_occurred_at(event),
         :ok <- validate_required_string(event, "transition"),
         {:ok, transition_schema} <- transition_schema(event),
         :ok <- validate_optional_attempt(event),
         :ok <- validate_optional_actions(event),
         :ok <- validate_optional_strings(event) do
      validate_transition_fields(event, transition_schema)
    end
  end

  defp stringify_keys(event) do
    Map.new(event, fn {key, value} -> {to_string(key), value} end)
  end

  defp transition_schema(%{"transition" => transition}) do
    case Map.fetch(@transition_schemas, transition) do
      {:ok, schema} -> {:ok, schema}
      :error -> {:error, {:unknown_transition, transition}}
    end
  end

  defp validate_transition_fields(event, schema) do
    with :ok <- validate_required_strings(event, Map.get(schema, :required_strings, [])),
         :ok <- validate_required_attempt(event, Map.get(schema, :required_attempt, false)),
         :ok <- validate_required_next_attempt(event, Map.get(schema, :required_next_attempt, false)),
         :ok <- validate_typed_wait(event, Map.get(schema, :typed_wait, false)),
         :ok <- validate_timestamp_field(event, Map.get(schema, :timestamp_field)),
         :ok <- validate_transition_stage(event),
         :ok <- validate_terminal_reason(event) do
      validate_next_action(event)
    end
  end

  defp validate_required_strings(event, fields) do
    Enum.reduce_while(fields, :ok, fn field, :ok ->
      case validate_required_string(event, field) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp validate_required_attempt(_event, false), do: :ok

  defp validate_required_attempt(event, true) do
    case Map.fetch(event, "attempt") do
      {:ok, attempt} when is_integer(attempt) and attempt >= 0 -> :ok
      _other -> {:error, {:invalid_field, "attempt"}}
    end
  end

  defp validate_required_next_attempt(_event, false), do: :ok

  defp validate_required_next_attempt(event, true) do
    case Map.fetch(event, "next_attempt") do
      {:ok, attempt} when is_integer(attempt) and attempt >= 1 -> :ok
      _other -> {:error, {:invalid_field, "next_attempt"}}
    end
  end

  defp validate_typed_wait(_event, false), do: :ok

  defp validate_typed_wait(event, true) do
    reason = event["parked_reason"]
    expected_actions = SymphonyElixir.OperatorWait.allowed_actions(reason)

    cond do
      not SymphonyElixir.OperatorWait.valid_reason?(reason) ->
        {:error, {:invalid_field, "parked_reason"}}

      event["allowed_actions"] != expected_actions ->
        {:error, {:invalid_field, "allowed_actions"}}

      true ->
        :ok
    end
  end

  defp validate_timestamp_field(_event, nil), do: :ok

  defp validate_timestamp_field(event, field) do
    case DateTime.from_iso8601(event[field]) do
      {:ok, _datetime, _offset} -> :ok
      _other -> {:error, {:invalid_field, field}}
    end
  end

  defp validate_transition_stage(%{"transition" => transition, "stage" => stage}) do
    if stage in Map.fetch!(@transition_stages, transition) do
      :ok
    else
      {:error, {:invalid_field, "stage"}}
    end
  end

  defp validate_terminal_reason(%{"transition" => transition} = event)
       when transition in ["run_parked", "wait_resumed", "resume_queued", "wait_rejected"] do
    case event["terminal_reason"] do
      nil -> :ok
      reason -> if MapSet.member?(@park_terminal_reasons, reason), do: :ok, else: {:error, {:invalid_field, "terminal_reason"}}
    end
  end

  defp validate_terminal_reason(%{"transition" => transition} = event) do
    case Map.fetch(@terminal_reasons, transition) do
      {:ok, reasons} ->
        if MapSet.member?(reasons, event["terminal_reason"]), do: :ok, else: {:error, {:invalid_field, "terminal_reason"}}

      :error ->
        if is_nil(event["terminal_reason"]), do: :ok, else: {:error, {:invalid_field, "terminal_reason"}}
    end
  end

  defp validate_next_action(%{"transition" => transition} = event)
       when transition in ["run_completed", "run_failed", "run_interrupted", "run_stopped"] do
    action = event["next_action"]
    next_attempt = event["next_attempt"]

    allowed_actions =
      case transition do
        "run_completed" -> ["continuation"]
        "run_failed" -> ["retry"]
        "run_interrupted" -> ["retry"]
        "run_stopped" -> ["none", "retry"]
      end

    cond do
      is_nil(action) ->
        :ok

      action not in allowed_actions ->
        {:error, {:invalid_field, "next_action"}}

      action == "none" and not is_nil(next_attempt) ->
        {:error, {:invalid_field, "next_attempt"}}

      action != "none" and not (is_integer(next_attempt) and next_attempt >= 1) ->
        {:error, {:invalid_field, "next_attempt"}}

      true ->
        :ok
    end
  end

  defp validate_next_action(event) do
    if is_nil(event["next_action"]), do: :ok, else: {:error, {:invalid_field, "next_action"}}
  end

  defp validate_event_fields(event) do
    if event |> Map.keys() |> MapSet.new() |> MapSet.subset?(@persisted_fields) do
      :ok
    else
      {:error, :unknown_fields}
    end
  end

  defp validate_schema_version(%{"schema_version" => @schema_version}), do: :ok
  defp validate_schema_version(_event), do: {:error, :unsupported_schema_version}

  defp validate_required_string(event, field) do
    case Map.fetch(event, field) do
      {:ok, value} when is_binary(value) and value != "" -> :ok
      _other -> {:error, {:invalid_field, field}}
    end
  end

  defp validate_occurred_at(event) do
    with :ok <- validate_required_string(event, "occurred_at"),
         {:ok, _datetime, _offset} <- DateTime.from_iso8601(event["occurred_at"]) do
      :ok
    else
      _other -> {:error, {:invalid_field, "occurred_at"}}
    end
  end

  defp validate_optional_attempt(event) do
    with :ok <-
           validate_optional_field(event, "attempt", fn value ->
             is_integer(value) and value >= 0
           end) do
      validate_optional_field(event, "next_attempt", fn value ->
        is_integer(value) and value >= 1
      end)
    end
  end

  defp validate_optional_actions(event) do
    validate_optional_field(event, "allowed_actions", fn value ->
      is_list(value) and Enum.all?(value, &is_binary/1)
    end)
  end

  defp validate_optional_strings(event) do
    Enum.reduce_while(@optional_string_fields, :ok, fn field, :ok ->
      case validate_optional_field(event, field, &is_binary/1) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp validate_optional_field(event, field, predicate) do
    case Map.fetch(event, field) do
      :error -> :ok
      {:ok, nil} -> :ok
      {:ok, value} -> if predicate.(value), do: :ok, else: {:error, {:invalid_field, field}}
    end
  end

  defp integer_value(value, _default) when is_integer(value), do: value
  defp integer_value(_value, default), do: default

  defp validate_ordered_events(events) do
    events
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, %{runs: %{}, waits: %{}, dispatches: %{}}}, fn {event, line}, {:ok, state} ->
      case validate_ordered_event(event, state) do
        {:ok, next_state} ->
          {:cont, {:ok, next_state}}

        {:error, reason} ->
          transition = event["transition"]
          {:halt, {:error, {:invalid_ledger_record, line, {:invalid_transition_sequence, transition, reason}}}}
      end
    end)
    |> case do
      {:ok, _state} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_ordered_event(%{"transition" => "run_claimed"} = event, state) do
    run_id = event["run_id"]

    cond do
      Map.has_key?(state.runs, run_id) ->
        {:error, :duplicate_run_id}

      active_run_for_issue?(state.runs, event["issue_id"]) ->
        {:error, :issue_already_active}

      true ->
        with :ok <- validate_claimed_dispatch(event, state.dispatches) do
          run = run_identity(event) |> Map.put(:phase, :claimed) |> Map.put(:terminal, nil)

          {:ok,
           %{
             state
             | runs: Map.put(state.runs, run_id, run),
               dispatches: Map.delete(state.dispatches, event["issue_id"])
           }}
        end
    end
  end

  defp validate_ordered_event(%{"transition" => "run_started"} = event, state) do
    with {:ok, run} <- fetch_run(state, event),
         :ok <- require_phase(run, [:claimed]),
         {:ok, run} <- merge_run_affinity(run, event) do
      {:ok, put_run(state, event["run_id"], %{run | phase: :started})}
    end
  end

  defp validate_ordered_event(%{"transition" => transition} = event, state)
       when transition in ["run_runtime_ready", "model_resolved"] do
    with {:ok, run} <- fetch_run(state, event),
         :ok <- require_phase(run, [:started]),
         {:ok, run} <- merge_run_affinity(run, event) do
      {:ok, put_run(state, event["run_id"], run)}
    end
  end

  defp validate_ordered_event(%{"transition" => "run_parked"} = event, state) do
    with {:ok, run} <- fetch_run(state, event),
         :ok <- require_phase(run, [:started]),
         {:ok, run} <- merge_run_affinity(run, event),
         :ok <- ensure_wait_available(state.waits, event) do
      wait = wait_identity(event)

      {:ok,
       state
       |> put_run(event["run_id"], %{run | phase: :terminal, terminal: "run_parked"})
       |> Map.update!(:waits, &Map.put(&1, event["issue_id"], wait))}
    end
  end

  defp validate_ordered_event(%{"transition" => transition} = event, state)
       when transition in ["run_completed", "run_failed", "run_interrupted", "run_stopped"] do
    with {:ok, run} <- fetch_run(state, event),
         :ok <- require_phase(run, terminal_predecessors(transition)),
         {:ok, run} <- merge_run_affinity(run, event) do
      next_state = put_run(state, event["run_id"], %{run | phase: :terminal, terminal: transition})

      cond do
        event["next_action"] in ["continuation", "retry"] ->
          {:ok, put_dispatch(next_state, event, event["next_attempt"])}

        transition == "run_interrupted" and event["terminal_reason"] == "runner_restarted" ->
          {:ok, put_dispatch(next_state, event, event["attempt"] + 1)}

        true ->
          {:ok, next_state}
      end
    end
  end

  defp validate_ordered_event(%{"transition" => "retry_scheduled"} = event, state) do
    with {:ok, run} <- fetch_run(state, event),
         :ok <- require_terminal(run),
         :ok <- reject_parked_retry(run),
         :ok <- require_increasing_attempt(event["attempt"], event["next_attempt"]) do
      {:ok, put_dispatch(state, event, event["next_attempt"])}
    end
  end

  defp validate_ordered_event(%{"transition" => transition} = event, state)
       when transition in ["wait_resumed", "resume_queued", "wait_rejected", "wait_released"] do
    with {:ok, wait} <- fetch_wait(state.waits, event),
         :ok <- validate_wait_identity(wait, event),
         :ok <- validate_wait_attempt(wait, event) do
      case transition do
        transition when transition in ["wait_resumed", "resume_queued"] ->
          {:ok,
           state
           |> Map.update!(:waits, &Map.delete(&1, event["issue_id"]))
           |> put_dispatch(event, event["attempt"])}

        "wait_released" ->
          {:ok, Map.update!(state, :waits, &Map.delete(&1, event["issue_id"]))}

        "wait_rejected" ->
          {:ok, state}
      end
    end
  end

  defp validate_ordered_event(_event, state), do: {:ok, state}

  defp fetch_run(state, event) do
    case Map.fetch(state.runs, event["run_id"]) do
      {:ok, run} ->
        if same_run_identity?(run, event), do: {:ok, run}, else: {:error, :run_identity_mismatch}

      :error ->
        {:error, :missing_run_predecessor}
    end
  end

  defp same_run_identity?(run, event) do
    run.issue_id == event["issue_id"] and
      run.issue_identifier == event["issue_identifier"] and
      run.attempt == event["attempt"]
  end

  defp run_identity(event) do
    %{
      issue_id: event["issue_id"],
      issue_identifier: event["issue_identifier"],
      attempt: event["attempt"],
      worker_host: event["worker_host"],
      workspace_path: event["workspace_path"]
    }
  end

  defp wait_identity(event) do
    %{
      wait_id: event["wait_id"],
      run_id: event["run_id"],
      issue_id: event["issue_id"],
      issue_identifier: event["issue_identifier"],
      attempt: event["attempt"],
      parked_reason: event["parked_reason"],
      terminal_reason: event["terminal_reason"],
      worker_host: event["worker_host"],
      workspace_path: event["workspace_path"]
    }
  end

  defp merge_run_affinity(run, event) do
    with {:ok, worker_host} <- merge_identity_value(run.worker_host, event["worker_host"]),
         {:ok, workspace_path} <- merge_identity_value(run.workspace_path, event["workspace_path"]) do
      {:ok, %{run | worker_host: worker_host, workspace_path: workspace_path}}
    end
  end

  defp merge_identity_value(nil, incoming), do: {:ok, incoming}
  defp merge_identity_value(current, nil), do: {:ok, current}
  defp merge_identity_value(value, value), do: {:ok, value}
  defp merge_identity_value(_current, _incoming), do: {:error, :run_affinity_mismatch}

  defp require_phase(%{phase: phase, terminal: nil}, phases) do
    if phase in phases, do: :ok, else: {:error, :illegal_predecessor}
  end

  defp require_phase(_run, _phases), do: {:error, :illegal_predecessor}

  defp require_terminal(%{phase: :terminal, terminal: terminal}) when is_binary(terminal), do: :ok
  defp require_terminal(_run), do: {:error, :illegal_predecessor}

  defp terminal_predecessors("run_failed"), do: [:claimed, :started]
  defp terminal_predecessors("run_interrupted"), do: [:claimed, :started]
  defp terminal_predecessors(_transition), do: [:started]

  defp reject_parked_retry(%{terminal: "run_parked"}), do: {:error, :parked_run_cannot_retry}
  defp reject_parked_retry(_run), do: :ok

  defp require_increasing_attempt(attempt, next_attempt) when next_attempt > attempt, do: :ok
  defp require_increasing_attempt(_attempt, _next_attempt), do: {:error, :retry_attempt_not_increasing}

  defp ensure_wait_available(waits, event) do
    if Map.has_key?(waits, event["issue_id"]), do: {:error, :issue_already_parked}, else: :ok
  end

  defp fetch_wait(waits, event) do
    case Map.fetch(waits, event["issue_id"]) do
      {:ok, wait} -> {:ok, wait}
      :error -> {:error, :missing_parked_wait}
    end
  end

  defp validate_wait_identity(wait, event) do
    fields = [
      :wait_id,
      :run_id,
      :issue_id,
      :issue_identifier,
      :parked_reason,
      :terminal_reason,
      :worker_host,
      :workspace_path
    ]

    if Enum.all?(fields, fn field -> Map.fetch!(wait, field) == event[Atom.to_string(field)] end) do
      :ok
    else
      {:error, :wait_identity_mismatch}
    end
  end

  defp validate_wait_attempt(wait, %{"transition" => transition, "attempt" => attempt})
       when transition in ["wait_resumed", "resume_queued"] do
    if attempt == wait.attempt + 1, do: :ok, else: {:error, :wait_attempt_mismatch}
  end

  defp validate_wait_attempt(wait, %{"attempt" => attempt}) do
    if attempt == wait.attempt, do: :ok, else: {:error, :wait_attempt_mismatch}
  end

  defp validate_claimed_dispatch(event, dispatches) do
    case Map.get(dispatches, event["issue_id"]) do
      nil ->
        :ok

      dispatch ->
        fields_match? =
          dispatch.attempt == event["attempt"] and
            dispatch.issue_identifier == event["issue_identifier"] and
            identity_value_matches?(dispatch.worker_host, event["worker_host"]) and
            identity_value_matches?(dispatch.workspace_path, event["workspace_path"])

        if fields_match?, do: :ok, else: {:error, :dispatch_identity_mismatch}
    end
  end

  defp identity_value_matches?(nil, _incoming), do: true
  defp identity_value_matches?(value, value), do: true
  defp identity_value_matches?(_expected, _incoming), do: false

  defp put_dispatch(state, event, attempt) do
    dispatch = %{
      attempt: attempt,
      issue_identifier: event["issue_identifier"],
      worker_host: event["worker_host"],
      workspace_path: event["workspace_path"]
    }

    %{state | dispatches: Map.put(state.dispatches, event["issue_id"], dispatch)}
  end

  defp put_run(state, run_id, run), do: %{state | runs: Map.put(state.runs, run_id, run)}

  defp active_run_for_issue?(runs, issue_id) do
    Enum.any?(runs, fn {_run_id, run} -> run.issue_id == issue_id and run.phase != :terminal end)
  end
end
