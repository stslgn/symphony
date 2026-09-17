defmodule SymphonyElixir.OperatorCursorMigration do
  @moduledoc """
  Read-only preparation of additive operator-cursor adoption evidence.

  This module never appends events or starts a runner. Its output is NOT a write
  authorization or a concurrency lock. A future managed apply adapter must hold
  the project start-controller lock, prove the runner stopped, and revalidate the
  approved plan before every durable append. No such adapter is installed here.
  """

  alias SymphonyElixir.RunLedger

  @spec prepare(Path.t(), map()) :: {:ok, map()} | {:error, term()}
  def prepare(path, %{sha256: sha, runner_generation: generation, issue_ids: ids, boundary: boundary} = expected)
      when is_binary(path) and is_binary(sha) and is_binary(generation) and is_list(ids) and is_binary(boundary) do
    with {:ok, identity} <- file_identity(path),
         {:ok, bytes, ledger_events, recovery} <- snapshot(path, generation),
         {:ok, ^identity} <- file_identity(path),
         true <- digest(bytes) == sha,
         %{"runner_generation" => ^generation} <- Enum.find(Enum.reverse(ledger_events), &(&1["transition"] == "runner_started")),
         true <- ids != [] and Enum.sort(ids) == Enum.sort(Map.keys(recovery.recovered_dispatches)),
         true <- Enum.all?(ids, &forward_boundary?(recovery.operator_comment_cursors[&1], boundary)) do
      events =
        Enum.map(Enum.sort(expected.issue_ids), fn id ->
          %{
            transition: "operator_cursor_initialized",
            stage: "operator",
            issue_id: id,
            comment_created_at: expected.boundary,
            runner_generation: expected.runner_generation
          }
        end)

      plan = %{path: Path.expand(path), file_identity: identity, baseline_bytes: byte_size(bytes), baseline_sha256: digest(bytes), recovery: recovery, events: events}
      {:ok, Map.put(plan, :plan_sha256, plan_digest(plan))}
    else
      {:error, _} = error -> error
      _ -> {:error, :snapshot_identity_mismatch}
    end
  end

  def prepare(_, _), do: {:error, :invalid_request}

  @spec remaining(Path.t(), map()) :: {:ok, [map()]} | {:error, term()}
  def remaining(
        path,
        %{
          path: original_path,
          file_identity: identity,
          plan_sha256: seal,
          baseline_bytes: size,
          baseline_sha256: sha,
          events: events
        } = plan
      )
      when is_binary(path) and is_integer(size) and size > 0 and is_list(events) do
    with true <- Path.expand(path) == original_path and seal == plan_digest(Map.delete(plan, :plan_sha256)),
         {:ok, ^identity} <- file_identity(path),
         {:ok, bytes} <- File.read(path),
         true <- byte_size(bytes) >= plan.baseline_bytes,
         <<prefix::binary-size(plan.baseline_bytes), suffix::binary>> <- bytes,
         true <- digest(prefix) == sha,
         true <- suffix == "" or String.ends_with?(suffix, "\n"),
         {:ok, _} <- RunLedger.read_events(path),
         {:ok, ^bytes} <- File.read(path),
         {:ok, ^identity} <- file_identity(path),
         {:ok, count} <- validated_suffix(suffix, plan.events) do
      {:ok, Enum.drop(plan.events, count)}
    else
      {:error, _} = error -> error
      _ -> {:error, :ledger_drift}
    end
  end

  def remaining(_, _), do: {:error, :invalid_plan}

  defp file_identity(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular} = stat} -> {:ok, {stat.major_device, stat.minor_device, stat.inode, stat.uid}}
      {:error, _} = error -> error
      _ -> {:error, :not_regular_file}
    end
  end

  defp validated_suffix(suffix, expected) do
    actual = suffix |> String.split("\n", trim: true) |> Enum.map(&Jason.decode!/1)
    normalized = Enum.map(actual, &Map.drop(&1, ["schema_version", "event_id", "occurred_at"]))
    expected_prefix = Enum.take(expected, length(actual)) |> Enum.map(fn event -> Map.new(event, fn {k, v} -> {Atom.to_string(k), v} end) end)

    if length(actual) <= length(expected) and normalized == expected_prefix,
      do: {:ok, length(actual)},
      else: {:error, :unexpected_suffix}
  end

  defp snapshot(path, generation) do
    with {:ok, bytes} <- File.read(path),
         true <- bytes != "" and String.ends_with?(bytes, "\n"),
         {:ok, events} <- RunLedger.read_events(path),
         {:ok, recovery} <- RunLedger.reconcile_startup(path, generation, append_fn: &preview_append/2),
         {:ok, ^bytes} <- File.read(path) do
      {:ok, bytes, events, recovery}
    else
      {:error, _} = error -> error
      _ -> {:error, :unstable_or_incomplete_ledger}
    end
  end

  defp preview_append(_, %{transition: "runner_started"}), do: :ok
  defp preview_append(_, _), do: {:error, :unfinished_run}

  defp forward_boundary?(%{created_at: current}, boundary) do
    with {:ok, old, 0} <- DateTime.from_iso8601(current),
         {:ok, new, 0} <- DateTime.from_iso8601(boundary) do
      # Ledger projection compares ISO strings; require agreement with UTC time.
      boundary > current and DateTime.compare(new, old) == :gt and String.ends_with?(boundary, "Z")
    else
      _ -> false
    end
  end

  defp forward_boundary?(_, _), do: false

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  defp plan_digest(plan), do: plan |> :erlang.term_to_binary([:deterministic]) |> digest()
end
