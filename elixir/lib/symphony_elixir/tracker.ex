defmodule SymphonyElixir.Tracker do
  @moduledoc """
  Adapter boundary for issue tracker reads and writes.
  """

  alias SymphonyElixir.{Config, WorkflowStore}

  defmodule PollContext do
    @moduledoc false

    @derive {Inspect, except: [:api_key, :authority_generation]}
    defstruct [
      :kind,
      :endpoint,
      :api_key,
      :api_key_env_var,
      :webhook_secret_env_var,
      :project_slug,
      :assignee,
      :authority_generation,
      active_states: [],
      terminal_states: []
    ]

    @type t :: %__MODULE__{
            kind: String.t() | nil,
            endpoint: String.t() | nil,
            api_key: String.t() | nil,
            api_key_env_var: String.t() | nil,
            webhook_secret_env_var: String.t() | nil,
            project_slug: String.t() | nil,
            assignee: String.t() | nil,
            authority_generation: term(),
            active_states: [String.t()],
            terminal_states: [String.t()]
          }
  end

  @callback fetch_candidate_issues(PollContext.t()) :: {:ok, [term()]} | {:error, term()}
  @callback fetch_issues_by_states([String.t()], PollContext.t()) ::
              {:ok, [term()]} | {:error, term()}
  @callback fetch_issue_states_by_ids([String.t()], PollContext.t()) ::
              {:ok, [term()]} | {:error, term()}
  @callback fetch_comments_since(String.t(), DateTime.t(), PollContext.t()) ::
              {:ok, [term()]} | {:error, term()}
  @callback create_comment(String.t(), String.t(), PollContext.t()) :: :ok | {:error, term()}
  @callback update_issue_state(String.t(), String.t(), PollContext.t()) ::
              :ok | {:error, term()}

  @spec fetch_candidate_issues(PollContext.t()) :: {:ok, [term()]} | {:error, term()}
  def fetch_candidate_issues(%PollContext{} = context) do
    with_authorized_context(context, fn -> adapter(context).fetch_candidate_issues(context) end)
  end

  @spec fetch_issues_by_states([String.t()], PollContext.t()) ::
          {:ok, [term()]} | {:error, term()}
  def fetch_issues_by_states(states, %PollContext{} = context) do
    with_authorized_context(context, fn -> adapter(context).fetch_issues_by_states(states, context) end)
  end

  @spec fetch_issue_states_by_ids([String.t()], PollContext.t()) ::
          {:ok, [term()]} | {:error, term()}
  def fetch_issue_states_by_ids(issue_ids, %PollContext{} = context) do
    with_authorized_context(context, fn ->
      adapter(context).fetch_issue_states_by_ids(issue_ids, context)
    end)
  end

  @spec fetch_comments_since(String.t(), DateTime.t(), PollContext.t()) ::
          {:ok, [term()]} | {:error, term()}
  def fetch_comments_since(issue_id, %DateTime{} = created_after, %PollContext{} = context) do
    with_authorized_context(context, fn ->
      adapter(context).fetch_comments_since(issue_id, created_after, context)
    end)
  end

  @spec create_comment(String.t(), String.t(), PollContext.t()) :: :ok | {:error, term()}
  def create_comment(issue_id, body, %PollContext{} = context) do
    with_authorized_context(context, fn -> adapter(context).create_comment(issue_id, body, context) end)
  end

  @spec update_issue_state(String.t(), String.t(), PollContext.t()) :: :ok | {:error, term()}
  def update_issue_state(issue_id, state_name, %PollContext{} = context) do
    with_authorized_context(context, fn ->
      adapter(context).update_issue_state(issue_id, state_name, context)
    end)
  end

  @spec adapter(PollContext.t()) :: module()
  def adapter(%PollContext{kind: "memory"}), do: SymphonyElixir.Tracker.Memory
  def adapter(%PollContext{}), do: SymphonyElixir.Linear.Adapter

  @spec poll_context(map(), term()) :: PollContext.t()
  def poll_context(tracker, authority_generation) when is_map(tracker) do
    %PollContext{
      kind: Map.get(tracker, :kind),
      endpoint: Map.get(tracker, :endpoint),
      api_key: Map.get(tracker, :api_key),
      api_key_env_var: Map.get(tracker, :api_key_env_var),
      webhook_secret_env_var: Map.get(tracker, :webhook_secret_env_var),
      project_slug: Map.get(tracker, :project_slug),
      assignee: Map.get(tracker, :assignee),
      authority_generation: authority_generation,
      active_states: Map.get(tracker, :active_states, []),
      terminal_states: Map.get(tracker, :terminal_states, [])
    }
  end

  @spec current_poll_context() :: PollContext.t()
  def current_poll_context do
    {settings, _authority_generation, tracker_authority_generation} =
      Config.settings_with_authority!()

    poll_context(settings.tracker, tracker_authority_generation)
  end

  @spec authority_valid?(PollContext.t()) :: boolean()
  def authority_valid?(%PollContext{authority_generation: nil}), do: false

  def authority_valid?(%PollContext{authority_generation: authority_generation}) do
    authority_generation == WorkflowStore.tracker_authority_generation()
  end

  defp with_authorized_context(context, operation) when is_function(operation, 0) do
    if authority_valid?(context),
      do: operation.(),
      else: {:error, :tracker_authority_invalidated}
  end
end
