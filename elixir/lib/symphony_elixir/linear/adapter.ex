defmodule SymphonyElixir.Linear.Adapter do
  @moduledoc """
  Linear-backed tracker adapter.
  """

  @behaviour SymphonyElixir.Tracker

  alias SymphonyElixir.Linear.Client
  alias SymphonyElixir.Tracker.PollContext

  @create_comment_mutation """
  mutation SymphonyCreateComment($issueId: String!, $body: String!) {
    commentCreate(input: {issueId: $issueId, body: $body}) {
      success
    }
  }
  """

  @update_state_mutation """
  mutation SymphonyUpdateIssueState($issueId: String!, $stateId: String!) {
    issueUpdate(id: $issueId, input: {stateId: $stateId}) {
      success
    }
  }
  """

  @state_lookup_query """
  query SymphonyResolveStateId($issueId: String!, $stateName: String!) {
    issue(id: $issueId) {
      team {
        states(filter: {name: {eq: $stateName}}, first: 1) {
          nodes {
            id
          }
        }
      }
    }
  }
  """

  @spec fetch_candidate_issues(PollContext.t()) :: {:ok, [term()]} | {:error, term()}
  def fetch_candidate_issues(%PollContext{} = context),
    do: client_module().fetch_candidate_issues(context)

  @spec fetch_issues_by_states([String.t()], PollContext.t()) ::
          {:ok, [term()]} | {:error, term()}
  def fetch_issues_by_states(states, %PollContext{} = context),
    do: client_module().fetch_issues_by_states(states, context)

  @spec fetch_issue_states_by_ids([String.t()], PollContext.t()) ::
          {:ok, [term()]} | {:error, term()}
  def fetch_issue_states_by_ids(issue_ids, %PollContext{} = context),
    do: client_module().fetch_issue_states_by_ids(issue_ids, context)

  @spec fetch_comments_since(String.t(), DateTime.t(), PollContext.t()) ::
          {:ok, [term()]} | {:error, term()}
  def fetch_comments_since(issue_id, %DateTime{} = created_after, %PollContext{} = context) do
    client_module().fetch_comments_since(issue_id, created_after, context)
  end

  @spec create_comment(String.t(), String.t(), PollContext.t()) :: :ok | {:error, term()}
  def create_comment(issue_id, body, %PollContext{} = context)
      when is_binary(issue_id) and is_binary(body) do
    with {:ok, response} <-
           client_module().graphql(
             @create_comment_mutation,
             %{issueId: issue_id, body: body},
             tracker_context: context
           ),
         true <- get_in(response, ["data", "commentCreate", "success"]) == true do
      :ok
    else
      false -> {:error, :comment_create_failed}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :comment_create_failed}
    end
  end

  @spec update_issue_state(String.t(), String.t(), PollContext.t()) ::
          :ok | {:error, term()}
  def update_issue_state(issue_id, state_name, %PollContext{} = context)
      when is_binary(issue_id) and is_binary(state_name) do
    with {:ok, state_id} <- resolve_state_id(issue_id, state_name, context),
         {:ok, response} <-
           client_module().graphql(
             @update_state_mutation,
             %{issueId: issue_id, stateId: state_id},
             tracker_context: context
           ),
         true <- get_in(response, ["data", "issueUpdate", "success"]) == true do
      :ok
    else
      false -> {:error, :issue_update_failed}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :issue_update_failed}
    end
  end

  defp client_module do
    Application.get_env(:symphony_elixir, :linear_client_module, Client)
  end

  defp resolve_state_id(issue_id, state_name, context) do
    with {:ok, response} <-
           client_module().graphql(
             @state_lookup_query,
             %{issueId: issue_id, stateName: state_name},
             tracker_context: context
           ),
         state_id when is_binary(state_id) <-
           get_in(response, ["data", "issue", "team", "states", "nodes", Access.at(0), "id"]) do
      {:ok, state_id}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :state_not_found}
    end
  end
end
