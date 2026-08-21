defmodule SymphonyElixir.TrackerAdmission do
  @moduledoc """
  Builds and verifies the content evidence used by pre-model tracker admission.

  The issue state is intentionally excluded from the content snapshot so the
  exact `Agent Ready` to `Agent Running` mutation can be verified without
  accepting concurrent edits to the issue body or routing labels.
  """

  alias SymphonyElixir.Linear.Issue
  alias SymphonyElixir.Tracker.PollContext

  @snapshot_schema "symphony.issue_snapshot.v1"
  @snapshot_string_fields [:id, :identifier, :title]
  @snapshot_optional_string_fields [:description, :url]

  @type snapshot :: %{
          bytes: non_neg_integer(),
          canonical_json: String.t(),
          schema: String.t(),
          sha256: String.t()
        }

  @type snapshot_evidence :: %{
          required(:sha256) => String.t(),
          optional(atom()) => term()
        }

  @type packet :: %{
          admission_id: String.t(),
          issue_snapshot_bytes: non_neg_integer(),
          issue_snapshot_schema: String.t(),
          issue_snapshot_sha256: String.t(),
          source_state: String.t(),
          target_state: String.t(),
          tracker_authority_digest: String.t()
        }

  @spec snapshot(Issue.t()) :: {:ok, snapshot()} | {:error, term()}
  def snapshot(%Issue{} = issue) do
    with :ok <- validate_string_fields(issue, @snapshot_string_fields),
         :ok <- validate_optional_string_fields(issue, @snapshot_optional_string_fields),
         {:ok, labels} <- canonical_labels(issue.labels) do
      canonical_json =
        canonical_object([
          {"description", issue.description},
          {"id", issue.id},
          {"identifier", issue.identifier},
          {"labels", labels},
          {"title", issue.title},
          {"url", issue.url}
        ])

      {:ok,
       %{
         bytes: byte_size(canonical_json),
         canonical_json: canonical_json,
         schema: @snapshot_schema,
         sha256: sha256(canonical_json)
       }}
    end
  end

  @spec packet(Issue.t(), PollContext.t(), String.t(), String.t()) ::
          {:ok, packet(), snapshot()} | {:error, term()}
  def packet(
        %Issue{state: source_state} = issue,
        %PollContext{} = context,
        admission_id,
        target_state
      )
      when is_binary(source_state) and is_binary(admission_id) and is_binary(target_state) do
    with {:ok, snapshot} <- snapshot(issue),
         {:ok, authority_digest} <- authority_digest(context) do
      {:ok,
       %{
         admission_id: admission_id,
         issue_snapshot_bytes: snapshot.bytes,
         issue_snapshot_schema: snapshot.schema,
         issue_snapshot_sha256: snapshot.sha256,
         source_state: source_state,
         target_state: target_state,
         tracker_authority_digest: authority_digest
       }, snapshot}
    end
  end

  def packet(%Issue{}, %PollContext{}, _admission_id, _target_state),
    do: {:error, :invalid_admission_packet}

  @spec verify_readback([Issue.t()], String.t(), snapshot_evidence(), keyword()) ::
          {:ok, Issue.t()} | {:error, term()}
  def verify_readback(issues, issue_id, expected_snapshot, opts)
      when is_list(issues) and is_binary(issue_id) and is_map(expected_snapshot) and is_list(opts) do
    target_state = Keyword.fetch!(opts, :target_state)

    case Enum.find(issues, &match?(%Issue{id: ^issue_id}, &1)) do
      nil ->
        {:error, :issue_not_found}

      %Issue{state: ^target_state} = issue ->
        with {:ok, actual_snapshot} <- snapshot(issue),
             true <- actual_snapshot.sha256 == expected_snapshot.sha256 do
          {:ok, issue}
        else
          false -> {:error, :issue_snapshot_conflict}
          {:error, reason} -> {:error, reason}
        end

      %Issue{} ->
        {:error, :target_state_not_observed}
    end
  end

  @spec verify_recovery_evidence(Issue.t(), PollContext.t(), map()) :: :ok | {:error, term()}
  def verify_recovery_evidence(
        %Issue{} = issue,
        %PollContext{} = context,
        admission
      )
      when is_map(admission) do
    with {:ok, snapshot} <- snapshot(issue),
         {:ok, current_authority_digest} <- authority_digest(context),
         true <-
           current_authority_digest == Map.get(admission, :tracker_authority_digest) or
             {:error, :tracker_authority_conflict},
         true <-
           (snapshot.schema == Map.get(admission, :issue_snapshot_schema) and
              snapshot.bytes == Map.get(admission, :issue_snapshot_bytes) and
              snapshot.sha256 == Map.get(admission, :issue_snapshot_sha256)) or
             {:error, :issue_snapshot_conflict} do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp authority_digest(%PollContext{authority_generation: nil}),
    do: {:error, :tracker_authority_unavailable}

  defp authority_digest(%PollContext{} = context) do
    contract = %{
      api_key_selector: credential_selector(context.api_key_env_var, context.api_key),
      endpoint: context.endpoint,
      kind: context.kind,
      project_slug: context.project_slug,
      webhook_secret_selector: credential_selector(context.webhook_secret_env_var, nil)
    }

    {:ok,
     contract
     |> :erlang.term_to_binary([:deterministic])
     |> sha256()}
  end

  defp credential_selector(env_var, _value) when is_binary(env_var), do: "$" <> env_var
  defp credential_selector(_env_var, value), do: value

  defp validate_string_fields(issue, fields) do
    Enum.reduce_while(fields, :ok, fn field, :ok ->
      case Map.fetch!(issue, field) do
        value when is_binary(value) -> {:cont, :ok}
        _other -> {:halt, {:error, {:invalid_snapshot_field, field}}}
      end
    end)
  end

  defp validate_optional_string_fields(issue, fields) do
    Enum.reduce_while(fields, :ok, fn field, :ok ->
      case Map.fetch!(issue, field) do
        value when is_binary(value) or is_nil(value) -> {:cont, :ok}
        _other -> {:halt, {:error, {:invalid_snapshot_field, field}}}
      end
    end)
  end

  defp canonical_labels(labels) when is_list(labels) do
    if Enum.all?(labels, &is_binary/1),
      do: {:ok, labels |> Enum.uniq() |> Enum.sort()},
      else: {:error, {:invalid_snapshot_field, :labels}}
  end

  defp canonical_labels(_labels), do: {:error, {:invalid_snapshot_field, :labels}}

  defp canonical_object(fields) do
    encoded_fields =
      Enum.map_join(fields, ",", fn {key, value} ->
        Jason.encode!(key) <> ":" <> Jason.encode!(value)
      end)

    "{" <> encoded_fields <> "}"
  end

  defp sha256(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end
