defmodule SymphonyElixir.MergeLane do
  @moduledoc """
  Durable, append-only exclusion claims for reviewed pull-request merges.

  Every mutation is serialized by an atomic filesystem claim. A process crash
  leaves that operation lock in place, so later callers fail closed instead of
  guessing whether a provider-side mutation started.
  """

  alias SymphonyElixir.RunLedger

  @schema_version 1
  @binding_fields [
    :issue_id,
    :issue_identifier,
    :wait_id,
    :repository,
    :pull_request,
    :base_ref,
    :head_sha,
    :channel,
    :executor,
    :workflow_generation
  ]
  @claim_fields @binding_fields ++
                  [:claim_id, :fencing_token, :state, :version, :operator_source_state]
  @evidence_fields [
    :evidence_sha256,
    :observed_base_ref,
    :observed_head_sha,
    :observed_workflow_generation,
    :reason,
    :result,
    :recovery_action
  ]
  @terminal_states [
    "invalidated",
    "aborted_no_side_effect",
    "completed",
    "acceptance_failed"
  ]
  @states [
    "claimed",
    "executing",
    "merge_uncertain",
    "merge_observed",
    "acceptance_pending",
    "operator_required"
    | @terminal_states
  ]
  @event_fields MapSet.new(
                  Enum.map(@claim_fields ++ @evidence_fields, &Atom.to_string/1) ++
                    ~w(schema_version event_id occurred_at transition previous_state previous_version actor outcome conflicting_channel conflicting_executor)
                )
  @field_limits %{
    issue_id: 128,
    issue_identifier: 96,
    wait_id: 128,
    repository: 200,
    base_ref: 255,
    channel: 16,
    executor: 128
  }

  @spec default_path() :: Path.t()
  def default_path, do: default_path(RunLedger.default_path())

  @spec default_path(Path.t()) :: Path.t()
  def default_path(run_ledger_path) when is_binary(run_ledger_path) do
    Path.join(Path.dirname(Path.expand(run_ledger_path)), "merge-lane-ledger.jsonl")
  end

  @spec claim(Path.t(), map()) :: {:ok, map()} | {:error, term()}
  def claim(path, binding) when is_binary(path) and is_map(binding) do
    with :ok <- validate_binding(binding) do
      with_locked_events(path, &claim_from_events(path, &1, binding))
    end
  end

  def claim(_path, _binding), do: {:error, :invalid_claim}

  @spec active_claim(Path.t(), map()) :: {:ok, map() | nil} | {:error, term()}
  def active_claim(path, binding) when is_binary(path) and is_map(binding) do
    with {:ok, events} <- read_events(path) do
      claim =
        events
        |> project_claims()
        |> Map.values()
        |> Enum.reject(&terminal?/1)
        |> Enum.find(&same_lane?(&1, binding))

      {:ok, claim}
    end
  end

  def active_claim(_path, _binding), do: {:error, :invalid_claim}

  @spec guard_runner_approval(Path.t(), String.t(), String.t(), String.t()) ::
          :ok | {:error, :runner_merge_claim_required | {:claim_conflict, String.t()} | term()}
  def guard_runner_approval(path, issue_id, wait_id, executor)
      when is_binary(path) and is_binary(issue_id) and is_binary(wait_id) and
             is_binary(executor) do
    with_locked_events(
      path,
      &guard_runner_approval_from_events(path, &1, issue_id, wait_id, executor)
    )
  end

  def guard_runner_approval(_path, _issue_id, _wait_id, _executor),
    do: {:error, :invalid_runner_approval}

  @spec transition(Path.t(), String.t(), non_neg_integer(), String.t()) ::
          {:ok, map()} | {:error, term()}
  def transition(path, claim_id, fencing_token, target_state)
      when is_binary(path) and is_binary(claim_id) and is_integer(fencing_token) and
             is_binary(target_state) do
    transition(path, claim_id, fencing_token, target_state, %{})
  end

  def transition(_path, _claim_id, _fencing_token, _target_state),
    do: {:error, :invalid_transition}

  @spec transition(Path.t(), String.t(), non_neg_integer(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def transition(path, claim_id, fencing_token, target_state, evidence)
      when is_binary(path) and is_binary(claim_id) and is_integer(fencing_token) and
             is_binary(target_state) and is_map(evidence) do
    with_locked_events(
      path,
      &transition_from_events(path, &1, claim_id, fencing_token, target_state, evidence)
    )
  end

  def transition(_path, _claim_id, _fencing_token, _target_state, _evidence),
    do: {:error, :invalid_transition}

  @spec recover(Path.t(), String.t(), non_neg_integer(), String.t(), map()) ::
          {:ok, map()} | {:error, term()}
  def recover(path, claim_id, fencing_token, action, evidence)
      when is_binary(path) and is_binary(claim_id) and is_integer(fencing_token) and
             is_binary(action) and is_map(evidence) do
    with_locked_events(
      path,
      &recover_from_events(path, &1, claim_id, fencing_token, action, evidence)
    )
  end

  def recover(_path, _claim_id, _fencing_token, _action, _evidence),
    do: {:error, :invalid_recovery}

  @spec history(Path.t()) :: {:ok, [map()]} | {:error, term()}
  def history(path) when is_binary(path), do: read_events(path)

  def history(_path), do: {:error, :invalid_claim}

  defp claim_from_events(path, events, binding) do
    case conflicting_claim(events, binding) do
      nil -> create_claim(path, events, binding)
      existing -> record_claim_conflict(path, existing, binding)
    end
  end

  defp guard_runner_approval_from_events(path, events, issue_id, wait_id, executor) do
    case active_claim_for_wait(events, issue_id, wait_id) do
      nil ->
        {:error, :runner_merge_claim_required}

      %{
        channel: "runner",
        executor: ^executor,
        workflow_generation: ^executor,
        state: "claimed"
      } ->
        :ok

      existing ->
        record_claim_conflict(path, existing, %{
          channel: "runner",
          executor: executor
        })
    end
  end

  defp transition_from_events(
         path,
         events,
         claim_id,
         fencing_token,
         target_state,
         evidence
       ) do
    with {:ok, claim} <- fetch_claim(events, claim_id),
         :ok <- ensure_nonterminal(claim),
         :ok <- require_fencing_token(claim, fencing_token),
         :ok <- allow_transition(claim.state, target_state),
         :ok <- validate_transition_evidence(claim, target_state, evidence) do
      next_claim = transition_claim(claim, target_state)

      event =
        next_claim
        |> Map.merge(take_fields(evidence, @evidence_fields))
        |> Map.merge(event_metadata(claim, target_state, "state_transitioned"))

      persist_event_result(path, event, next_claim)
    end
  end

  defp recover_from_events(path, events, claim_id, fencing_token, action, evidence) do
    with {:ok, claim} <- fetch_claim(events, claim_id),
         :ok <- require_fencing_token(claim, fencing_token),
         {:ok, target_state, operator_source_state} <- recovery_target(claim, action, evidence) do
      next_claim = %{
        claim
        | state: target_state,
          version: claim.version + 1,
          fencing_token: claim.fencing_token + 1,
          operator_source_state: operator_source_state
      }

      event =
        next_claim
        |> Map.merge(take_fields(evidence, @evidence_fields))
        |> Map.merge(event_metadata(claim, target_state, "recovery_applied"))
        |> Map.put(:recovery_action, action)

      persist_event_result(path, event, next_claim)
    end
  end

  defp event_metadata(claim, transition, outcome) do
    %{
      schema_version: @schema_version,
      event_id: RunLedger.new_id("merge_evt"),
      occurred_at: occurred_at(),
      transition: transition,
      previous_state: claim.state,
      previous_version: claim.version,
      actor: claim.executor,
      outcome: outcome
    }
  end

  defp persist_event_result(path, event, claim) do
    with :ok <- append_event(path, event), do: {:ok, claim}
  end

  defp with_locked_events(path, operation) do
    with_operation_lock(path, fn ->
      with {:ok, events} <- read_events(path), do: operation.(events)
    end)
  end

  defp with_operation_lock(path, operation) do
    lock_path = path <> ".lock"

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- acquire_operation_lock(lock_path) do
      try do
        operation.()
      after
        _ = File.rmdir(lock_path)
      end
    end
  end

  defp acquire_operation_lock(lock_path, attempts \\ 100)

  defp acquire_operation_lock(_lock_path, 0), do: {:error, :operation_locked}

  defp acquire_operation_lock(lock_path, attempts) do
    case File.mkdir(lock_path) do
      :ok ->
        :ok

      {:error, :eexist} ->
        Process.sleep(5)
        acquire_operation_lock(lock_path, attempts - 1)

      {:error, reason} ->
        {:error, {:operation_lock_failed, reason}}
    end
  end

  defp read_events(path) do
    case File.read(path) do
      {:ok, ""} -> {:ok, []}
      {:ok, contents} -> decode_events(contents)
      {:error, :enoent} -> {:ok, []}
      {:error, reason} -> {:error, {:ledger_read_failed, reason}}
    end
  end

  defp decode_events(contents) do
    contents
    |> String.split("\n", trim: true)
    |> Enum.reduce_while({:ok, []}, fn line, {:ok, events} ->
      case Jason.decode(line) do
        {:ok, event} when is_map(event) -> {:cont, {:ok, [event | events]}}
        _other -> {:halt, {:error, :invalid_merge_lane_ledger}}
      end
    end)
    |> case do
      {:ok, events} ->
        events = Enum.reverse(events)

        case validate_events(events) do
          :ok -> {:ok, events}
          {:error, _reason} -> {:error, :invalid_merge_lane_ledger}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_events(events) do
    events
    |> Enum.reduce_while({:ok, %{claims: %{}, max_fence: 0}}, fn event, {:ok, state} ->
      with :ok <- validate_event_shape(event),
           {:ok, next_state} <- validate_ordered_event(event, state) do
        {:cont, {:ok, next_state}}
      else
        _error -> {:halt, {:error, :invalid_event}}
      end
    end)
    |> case do
      {:ok, _state} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_event_shape(event) do
    with true <- MapSet.subset?(MapSet.new(Map.keys(event)), @event_fields),
         1 <- event["schema_version"],
         true <- valid_string?(event["event_id"]),
         true <- valid_timestamp?(event["occurred_at"]),
         true <- valid_string?(event["claim_id"]),
         true <- event["state"] in @states,
         true <- is_integer(event["version"]) and event["version"] > 0,
         true <- is_integer(event["fencing_token"]) and event["fencing_token"] > 0,
         true <- is_integer(event["previous_version"]) and event["previous_version"] >= 0,
         :ok <- validate_binding(event) do
      :ok
    else
      _other -> {:error, :invalid_event_shape}
    end
  end

  defp validate_ordered_event(%{"claim_id" => claim_id} = event, state) do
    case Map.fetch(state.claims, claim_id) do
      :error -> validate_initial_claim_event(event, state)
      {:ok, previous} -> validate_followup_claim_event(event, previous, state)
    end
  end

  defp validate_initial_claim_event(event, state) do
    claim = event_to_claim(event)

    with "claimed" <- event["transition"],
         "claimed" <- event["state"],
         "none" <- event["previous_state"],
         0 <- event["previous_version"],
         1 <- event["version"],
         true <- event["fencing_token"] > state.max_fence,
         false <- Enum.any?(Map.values(state.claims), &(!terminal?(&1) and same_lane?(&1, claim))) do
      {:ok,
       %{
         claims: Map.put(state.claims, claim.claim_id, claim),
         max_fence: event["fencing_token"]
       }}
    else
      _other -> {:error, :invalid_initial_claim}
    end
  end

  defp validate_followup_claim_event(event, previous, state) do
    claim = event_to_claim(event)
    previous_state = previous.state
    previous_version = previous.version

    with false <- terminal?(previous),
         true <- same_binding?(claim, previous),
         ^previous_state <- event["previous_state"],
         ^previous_version <- event["previous_version"],
         true <- claim.version == previous.version + 1,
         :ok <- validate_followup_fence(event, claim, previous),
         :ok <- validate_followup_transition(event, claim, previous) do
      {:ok, %{state | claims: Map.put(state.claims, claim.claim_id, claim)}}
    else
      _other -> {:error, :invalid_followup_claim}
    end
  end

  defp validate_followup_fence(%{"outcome" => "recovery_applied"}, claim, previous) do
    if claim.fencing_token == previous.fencing_token + 1,
      do: :ok,
      else: {:error, :invalid_fence}
  end

  defp validate_followup_fence(_event, claim, previous) do
    if claim.fencing_token == previous.fencing_token,
      do: :ok,
      else: {:error, :invalid_fence}
  end

  defp validate_followup_transition(%{"transition" => "claim_conflict"} = event, claim, previous) do
    if claim.state == previous.state and event["outcome"] == "conflicting_claim_rejected",
      do: :ok,
      else: {:error, :invalid_conflict}
  end

  defp validate_followup_transition(%{"outcome" => "recovery_applied"}, claim, previous) do
    if recovery_transition?(previous.state, claim.state),
      do: :ok,
      else: {:error, :invalid_recovery_transition}
  end

  defp validate_followup_transition(event, claim, previous) do
    with true <- event["transition"] == claim.state,
         :ok <- allow_transition(previous.state, claim.state) do
      :ok
    else
      _other -> {:error, :invalid_state_transition}
    end
  end

  defp recovery_transition?("merge_uncertain", target),
    do: target in ["operator_required", "aborted_no_side_effect"]

  defp recovery_transition?("operator_required", target),
    do: target in ["claimed", "executing", "merge_observed", "acceptance_pending", "aborted_no_side_effect"]

  defp recovery_transition?(_source, _target), do: false

  defp same_binding?(left, right) do
    Enum.all?(@binding_fields, &(Map.fetch!(left, &1) == Map.fetch!(right, &1))) and
      left.claim_id == right.claim_id and left.executor == right.executor
  end

  defp append_event(path, event) do
    with :ok <- ensure_private_file(path),
         {:ok, encoded} <- Jason.encode(event) do
      File.write(path, encoded <> "\n", [:append, :sync])
    end
  end

  defp ensure_private_file(path) do
    with :ok <- File.touch(path), do: File.chmod(path, 0o600)
  end

  defp project_claims(events) do
    Enum.reduce(events, %{}, fn event, claims ->
      claim = event_to_claim(event)
      Map.put(claims, claim.claim_id, claim)
    end)
  end

  defp event_to_claim(event) do
    Map.new(@claim_fields, fn field ->
      {field, Map.get(event, Atom.to_string(field))}
    end)
  end

  defp create_claim(path, events, binding) do
    claim =
      @binding_fields
      |> Map.new(fn field -> {field, value(binding, field)} end)
      |> Map.merge(%{
        claim_id: RunLedger.new_id("merge_claim"),
        fencing_token: next_fencing_token(events),
        operator_source_state: nil,
        state: "claimed",
        version: 1
      })

    event =
      claim
      |> Map.merge(%{
        schema_version: @schema_version,
        event_id: RunLedger.new_id("merge_evt"),
        occurred_at: occurred_at(),
        transition: "claimed",
        previous_state: "none",
        previous_version: 0
      })

    with :ok <- append_event(path, event), do: {:ok, claim}
  end

  defp record_claim_conflict(path, existing, contender) do
    event =
      existing
      |> Map.put(:version, existing.version + 1)
      |> Map.merge(%{
        schema_version: @schema_version,
        event_id: RunLedger.new_id("merge_evt"),
        occurred_at: occurred_at(),
        transition: "claim_conflict",
        previous_state: existing.state,
        previous_version: existing.version,
        actor: value(contender, :executor),
        outcome: "conflicting_claim_rejected",
        conflicting_channel: value(contender, :channel),
        conflicting_executor: value(contender, :executor)
      })

    with :ok <- append_event(path, event),
         do: {:error, {:claim_conflict, existing.claim_id}}
  end

  defp conflicting_claim(events, binding) do
    events
    |> project_claims()
    |> Map.values()
    |> Enum.reject(&terminal?/1)
    |> Enum.find(&same_lane?(&1, binding))
  end

  defp active_claim_for_wait(events, issue_id, wait_id) do
    events
    |> project_claims()
    |> Map.values()
    |> Enum.reject(&terminal?/1)
    |> Enum.find(fn claim -> claim.issue_id == issue_id and claim.wait_id == wait_id end)
  end

  defp fetch_claim(events, claim_id) do
    case Map.fetch(project_claims(events), claim_id) do
      {:ok, claim} -> {:ok, claim}
      :error -> {:error, :claim_not_found}
    end
  end

  defp require_fencing_token(%{fencing_token: fencing_token}, fencing_token), do: :ok
  defp require_fencing_token(_claim, _fencing_token), do: {:error, :stale_fencing_token}

  defp ensure_nonterminal(claim) do
    if terminal?(claim), do: {:error, :claim_terminal}, else: :ok
  end

  defp allow_transition("claimed", "executing"), do: :ok
  defp allow_transition("claimed", "operator_required"), do: :ok
  defp allow_transition("claimed", "aborted_no_side_effect"), do: :ok
  defp allow_transition("executing", "merge_uncertain"), do: :ok
  defp allow_transition("executing", "merge_observed"), do: :ok
  defp allow_transition("executing", "invalidated"), do: :ok
  defp allow_transition("executing", "operator_required"), do: :ok
  defp allow_transition("executing", "aborted_no_side_effect"), do: :ok
  defp allow_transition("merge_observed", "acceptance_pending"), do: :ok
  defp allow_transition("acceptance_pending", "completed"), do: :ok
  defp allow_transition("acceptance_pending", "acceptance_failed"), do: :ok
  defp allow_transition("acceptance_pending", "operator_required"), do: :ok
  defp allow_transition(_source, _target), do: {:error, :invalid_transition}

  defp validate_transition_evidence(_claim, "merge_uncertain", evidence),
    do: validate_evidence_digest(evidence)

  defp validate_transition_evidence(claim, "merge_observed", evidence) do
    with :ok <- validate_evidence_digest(evidence),
         true <- valid_ref?(value(evidence, :observed_base_ref)),
         true <- valid_head_sha?(value(evidence, :observed_head_sha)),
         true <- value(evidence, :observed_base_ref) == claim.base_ref,
         true <- value(evidence, :observed_head_sha) == claim.head_sha do
      :ok
    else
      false -> {:error, :approved_target_mismatch}
      _other -> {:error, :invalid_evidence}
    end
  end

  defp validate_transition_evidence(_claim, "invalidated", evidence) do
    with :ok <- validate_evidence_digest(evidence),
         result when result in ["base_ref_mismatch", "head_sha_mismatch"] <-
           value(evidence, :result),
         true <- valid_ref?(value(evidence, :observed_base_ref)),
         true <- valid_head_sha?(value(evidence, :observed_head_sha)) do
      :ok
    else
      _other -> {:error, :invalid_evidence}
    end
  end

  defp validate_transition_evidence(_claim, "operator_required", evidence) do
    with :ok <- validate_evidence_digest(evidence),
         reason when reason in ["workflow_generation_drift", "provider_ambiguity", "acceptance_interrupted"] <-
           value(evidence, :reason) do
      :ok
    else
      _other -> {:error, :invalid_evidence}
    end
  end

  defp validate_transition_evidence(_claim, "aborted_no_side_effect", evidence) do
    with :ok <- validate_evidence_digest(evidence),
         "no_provider_side_effect" <- value(evidence, :result) do
      :ok
    else
      _other -> {:error, :invalid_evidence}
    end
  end

  defp validate_transition_evidence(_claim, target, evidence)
       when target in ["acceptance_pending", "completed", "acceptance_failed"],
       do: validate_evidence_digest(evidence)

  defp validate_transition_evidence(_claim, _target, _evidence), do: :ok

  defp recovery_target(%{state: "merge_uncertain"}, "reconcile", evidence) do
    with :ok <- validate_evidence_digest(evidence),
         "ambiguous" <- value(evidence, :result) do
      {:ok, "operator_required", "merge_uncertain"}
    else
      _other -> {:error, :invalid_recovery}
    end
  end

  defp recovery_target(%{state: "operator_required"} = claim, "reconcile", evidence) do
    with :ok <- validate_evidence_digest(evidence),
         "merged" <- value(evidence, :result),
         true <- value(evidence, :observed_base_ref) == claim.base_ref,
         true <- value(evidence, :observed_head_sha) == claim.head_sha do
      {:ok, "merge_observed", nil}
    else
      _other -> {:error, :invalid_recovery}
    end
  end

  defp recovery_target(%{state: state}, "abort", evidence)
       when state in ["merge_uncertain", "operator_required"] do
    with :ok <- validate_evidence_digest(evidence),
         "no_provider_side_effect" <- value(evidence, :result) do
      {:ok, "aborted_no_side_effect", nil}
    else
      _other -> {:error, :invalid_recovery}
    end
  end

  defp recovery_target(
         %{state: "operator_required", operator_source_state: source_state} = claim,
         "resume",
         evidence
       )
       when source_state in ["claimed", "executing", "acceptance_pending"] do
    with :ok <- validate_evidence_digest(evidence),
         "no_provider_side_effect" <- value(evidence, :result),
         true <- value(evidence, :observed_base_ref) == claim.base_ref,
         true <- value(evidence, :observed_head_sha) == claim.head_sha,
         true <- value(evidence, :observed_workflow_generation) == claim.workflow_generation do
      {:ok, source_state, nil}
    else
      _other -> {:error, :invalid_recovery}
    end
  end

  defp recovery_target(_claim, _action, _evidence), do: {:error, :invalid_recovery}

  defp validate_evidence_digest(evidence) do
    if Regex.match?(~r/\A[0-9a-f]{64}\z/, value(evidence, :evidence_sha256) || ""),
      do: :ok,
      else: {:error, :invalid_evidence}
  end

  defp same_lane?(claim, binding) do
    claim.issue_id == value(binding, :issue_id) or
      (claim.repository == value(binding, :repository) and
         claim.pull_request == value(binding, :pull_request))
  end

  defp transition_claim(claim, "operator_required") do
    %{
      claim
      | state: "operator_required",
        version: claim.version + 1,
        operator_source_state: claim.state
    }
  end

  defp transition_claim(claim, target_state) do
    %{claim | state: target_state, version: claim.version + 1}
  end

  defp terminal?(%{state: state}), do: state in @terminal_states

  defp next_fencing_token(events) do
    events
    |> Enum.map(&Map.get(&1, "fencing_token", 0))
    |> Enum.max(fn -> 0 end)
    |> Kernel.+(1)
  end

  defp validate_binding(binding) do
    with :ok <- validate_bounded_binding_fields(binding),
         :ok <- validate_pull_request(value(binding, :pull_request)),
         :ok <- validate_repository(value(binding, :repository)),
         :ok <- validate_base_ref(value(binding, :base_ref)),
         :ok <- validate_executor(value(binding, :executor)),
         :ok <- validate_channel(value(binding, :channel)),
         :ok <- validate_wait_id(value(binding, :wait_id), value(binding, :channel)),
         :ok <- validate_head_sha(value(binding, :head_sha)) do
      validate_workflow_generation(value(binding, :workflow_generation))
    end
  end

  defp validate_bounded_binding_fields(binding) do
    if Enum.all?(Map.keys(@field_limits), &valid_bounded_binding_field?(&1, value(binding, &1))),
      do: :ok,
      else: {:error, :invalid_claim_binding}
  end

  defp valid_bounded_binding_field?(:wait_id, nil), do: true
  defp valid_bounded_binding_field?(field, value), do: valid_bounded_text?(field, value)

  defp validate_pull_request(pull_request)
       when is_integer(pull_request) and pull_request > 0,
       do: :ok

  defp validate_pull_request(_pull_request), do: {:error, :invalid_claim_binding}

  defp validate_repository(repository) do
    if is_binary(repository) and
         Regex.match?(~r/\A[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\z/, repository),
       do: :ok,
       else: {:error, :invalid_claim_binding}
  end

  defp validate_base_ref(base_ref) do
    if valid_ref?(base_ref), do: :ok, else: {:error, :invalid_claim_binding}
  end

  defp validate_executor(executor) do
    if is_binary(executor) and Regex.match?(~r/\A[A-Za-z0-9._:@\/-]+\z/, executor),
      do: :ok,
      else: {:error, :invalid_claim_binding}
  end

  defp validate_channel(channel) when channel in ["session", "runner"], do: :ok
  defp validate_channel(_channel), do: {:error, :invalid_claim_binding}

  defp validate_wait_id(nil, "session"), do: :ok

  defp validate_wait_id(wait_id, _channel) do
    if valid_bounded_text?(:wait_id, wait_id),
      do: :ok,
      else: {:error, :invalid_claim_binding}
  end

  defp validate_head_sha(head_sha) do
    if valid_head_sha?(head_sha), do: :ok, else: {:error, :invalid_claim_binding}
  end

  defp validate_workflow_generation(workflow_generation) do
    if is_binary(workflow_generation) and
         Regex.match?(~r/\A[0-9a-f]{64}\z/, workflow_generation),
       do: :ok,
       else: {:error, :invalid_claim_binding}
  end

  defp valid_string?(value), do: is_binary(value) and value != "" and String.valid?(value)

  defp valid_bounded_text?(field, value) do
    valid_string?(value) and byte_size(value) <= Map.fetch!(@field_limits, field) and
      not Regex.match?(~r/\p{C}/u, value)
  end

  defp valid_ref?(value) do
    valid_string?(value) and
      not Regex.match?(~r/[\p{C} ~^:?*\[\\]/u, value) and
      not String.contains?(value, ["..", "@{"]) and
      not String.starts_with?(value, ["/", "."]) and
      not String.ends_with?(value, ["/", ".", ".lock"])
  end

  defp valid_head_sha?(value),
    do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{40}\z/, value)

  defp valid_timestamp?(value) when is_binary(value) do
    match?({:ok, _datetime, _offset}, DateTime.from_iso8601(value))
  end

  defp valid_timestamp?(_value), do: false

  defp value(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp take_fields(map, fields) do
    fields
    |> Map.new(fn field -> {field, value(map, field)} end)
    |> Map.reject(fn {_field, field_value} -> is_nil(field_value) end)
  end

  defp occurred_at do
    DateTime.utc_now() |> DateTime.truncate(:millisecond) |> DateTime.to_iso8601()
  end
end
