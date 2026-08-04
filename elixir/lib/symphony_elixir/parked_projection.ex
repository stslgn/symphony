defmodule SymphonyElixir.ParkedProjection do
  @moduledoc """
  Bounded, control-safe operator projection for parked waits.

  The projection is display-only. Exact workspace affinity stays unchanged in
  the durable/internal wait and is never read back from this module for cleanup.
  Collections are stably sorted, capped at 100 rows and 65,536 encoded row
  bytes, and accompanied by exact total/omission metadata.
  """

  alias SymphonyElixir.OperatorWait

  @row_limit 100
  @byte_limit 65_536
  @field_limits %{
    issue_id: 128,
    issue_identifier: 96,
    wait_id: 128,
    tracker_state: 128,
    run_id: 128,
    worker_host: 128,
    workspace_path: 512
  }

  @type collection :: %{rows: [map()], metadata: map()}

  @spec limits() :: map()
  def limits do
    %{fields: @field_limits, row_limit: @row_limit, byte_limit: @byte_limit}
  end

  @spec collection([map()]) :: collection()
  def collection(waits) when is_list(waits) do
    projected =
      waits
      |> Enum.with_index()
      |> Enum.map(fn {wait, index} -> {row(wait), index} end)
      |> Enum.sort_by(fn {entry, index} ->
        {entry.issue_identifier || "", entry.wait_id || "", entry.issue_id || "", index}
      end)

    {rows, returned_bytes} = take_bounded_rows(projected)
    total_count = length(waits)
    returned_count = length(rows)

    %{
      rows: rows,
      metadata: %{
        total_count: total_count,
        returned_count: returned_count,
        omitted_count: total_count - returned_count,
        truncated: returned_count < total_count,
        row_limit: @row_limit,
        byte_limit: @byte_limit,
        returned_bytes: returned_bytes
      }
    }
  end

  def collection(_waits), do: collection([])

  @spec row(map()) :: map()
  def row(wait) when is_map(wait) do
    fields = [
      {:issue_id, Map.get(wait, :issue_id)},
      {:issue_identifier, Map.get(wait, :identifier)},
      {:wait_id, Map.get(wait, :wait_id)},
      {:tracker_state, Map.get(wait, :tracker_state)},
      {:run_id, Map.get(wait, :run_id)},
      {:worker_host, Map.get(wait, :worker_host)},
      {:workspace_path, Map.get(wait, :workspace_path)}
    ]

    {display_fields, truncated_fields} = project_display_fields(fields)
    reason = allowlisted(Map.get(wait, :reason), &OperatorWait.valid_reason?/1)

    Map.merge(display_fields, %{
      reason: reason,
      allowed_actions: OperatorWait.allowed_actions(reason),
      attempt: safe_attempt(Map.get(wait, :attempt)),
      stage: allowlisted(Map.get(wait, :stage), &OperatorWait.valid_stage?/1),
      terminal_reason: allowlisted(Map.get(wait, :terminal_reason), &OperatorWait.valid_terminal_reason?/1),
      parked_at: iso8601(Map.get(wait, :parked_at)),
      truncated_fields: truncated_fields
    })
  end

  def row(_wait), do: row(%{})

  defp take_bounded_rows(projected) do
    projected
    |> Enum.reduce_while({[], 0}, fn {entry, _index}, {rows, bytes} ->
      separator_bytes = if rows == [], do: 0, else: 1
      row_bytes = entry |> Jason.encode!() |> byte_size()
      next_bytes = bytes + separator_bytes + row_bytes

      if length(rows) < @row_limit and next_bytes <= @byte_limit do
        {:cont, {[entry | rows], next_bytes}}
      else
        {:halt, {rows, bytes}}
      end
    end)
    |> then(fn {rows, bytes} -> {Enum.reverse(rows), bytes} end)
  end

  defp project_display_fields(fields) do
    Enum.reduce(fields, {%{}, []}, fn {field, value}, {projected, truncated} ->
      {safe_value, truncated?} = safe_text(value, Map.fetch!(@field_limits, field))
      truncated = if truncated?, do: [Atom.to_string(field) | truncated], else: truncated
      {Map.put(projected, field, safe_value), truncated}
    end)
    |> then(fn {projected, truncated} -> {projected, Enum.reverse(truncated)} end)
  end

  defp safe_text(nil, _max_bytes), do: {nil, false}

  defp safe_text(value, max_bytes) when is_binary(value) do
    value = if String.valid?(value), do: escape_controls(value), else: "invalid-utf8"

    if byte_size(value) <= max_bytes do
      {value, false}
    else
      {truncate_utf8(value, max_bytes), true}
    end
  end

  defp safe_text(_value, _max_bytes), do: {nil, false}

  defp escape_controls(value) do
    value
    |> String.to_charlist()
    |> Enum.map_join(fn
      ?\n ->
        "\\n"

      ?\r ->
        "\\r"

      ?\t ->
        "\\t"

      codepoint when codepoint in 0x00..0x1F or codepoint in 0x7F..0x9F ->
        "\\u{" <> (codepoint |> Integer.to_string(16) |> String.upcase()) <> "}"

      codepoint ->
        <<codepoint::utf8>>
    end)
  end

  defp truncate_utf8(value, max_bytes) do
    suffix = "..."
    budget = max(max_bytes - byte_size(suffix), 0)

    value
    |> String.graphemes()
    |> Enum.reduce_while({[], 0}, fn grapheme, {kept, bytes} ->
      next_bytes = bytes + byte_size(grapheme)

      if next_bytes <= budget,
        do: {:cont, {[grapheme | kept], next_bytes}},
        else: {:halt, {kept, bytes}}
    end)
    |> elem(0)
    |> Enum.reverse()
    |> Enum.join()
    |> Kernel.<>(suffix)
  end

  defp allowlisted(nil, _validator), do: nil
  defp allowlisted(value, validator) when is_binary(value), do: if(validator.(value), do: value)
  defp allowlisted(_value, _validator), do: nil

  defp safe_attempt(attempt) when is_integer(attempt) and attempt >= 0, do: attempt
  defp safe_attempt(_attempt), do: nil

  defp iso8601(%DateTime{} = datetime) do
    datetime
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end

  defp iso8601(_datetime), do: nil
end
