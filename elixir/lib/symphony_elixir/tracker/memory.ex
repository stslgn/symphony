defmodule SymphonyElixir.Tracker.Memory do
  @moduledoc """
  In-memory tracker adapter used for tests and local development.
  """

  @behaviour SymphonyElixir.Tracker

  alias SymphonyElixir.Linear.{Comment, Issue}

  @spec fetch_candidate_issues(SymphonyElixir.Tracker.PollContext.t()) ::
          {:ok, [Issue.t()]} | {:error, term()}
  def fetch_candidate_issues(_context), do: {:ok, issue_entries()}

  @spec fetch_issues_by_states(
          [String.t()],
          SymphonyElixir.Tracker.PollContext.t()
        ) :: {:ok, [Issue.t()]} | {:error, term()}
  def fetch_issues_by_states(state_names, _context) do
    normalized_states =
      state_names
      |> Enum.map(&normalize_state/1)
      |> MapSet.new()

    {:ok,
     Enum.filter(issue_entries(), fn %Issue{state: state} ->
       MapSet.member?(normalized_states, normalize_state(state))
     end)}
  end

  @spec fetch_issue_states_by_ids([String.t()], SymphonyElixir.Tracker.PollContext.t()) ::
          {:ok, [Issue.t()]} | {:error, term()}
  def fetch_issue_states_by_ids(issue_ids, _context) do
    wanted_ids = MapSet.new(issue_ids)

    {:ok,
     Enum.filter(issue_entries(), fn %Issue{id: id} ->
       MapSet.member?(wanted_ids, id)
     end)}
  end

  @spec fetch_comments_since(
          String.t(),
          DateTime.t(),
          SymphonyElixir.Tracker.PollContext.t()
        ) ::
          {:ok, [Comment.t()]} | {:error, term()}
  def fetch_comments_since(issue_id, %DateTime{} = created_after, _context) do
    comments =
      :symphony_elixir
      |> Application.get_env(:memory_tracker_comments, %{})
      |> Map.get(issue_id, [])

    {:ok,
     Enum.filter(comments, fn
       %Comment{created_at: %DateTime{} = created_at} ->
         DateTime.compare(created_at, created_after) in [:eq, :gt]

       _comment ->
         false
     end)}
  end

  @spec create_comment(String.t(), String.t(), SymphonyElixir.Tracker.PollContext.t()) ::
          :ok | {:error, term()}
  def create_comment(issue_id, body, _context) do
    send_event({:memory_tracker_comment, issue_id, body})
    :ok
  end

  @spec update_issue_state(String.t(), String.t(), SymphonyElixir.Tracker.PollContext.t()) ::
          :ok | {:error, term()}
  def update_issue_state(issue_id, state_name, _context) do
    send_event({:memory_tracker_state_update, issue_id, state_name})
    :ok
  end

  defp configured_issues do
    Application.get_env(:symphony_elixir, :memory_tracker_issues, [])
  end

  defp issue_entries do
    Enum.filter(configured_issues(), &match?(%Issue{}, &1))
  end

  defp send_event(message) do
    case Application.get_env(:symphony_elixir, :memory_tracker_recipient) do
      pid when is_pid(pid) -> send(pid, message)
      _ -> :ok
    end
  end

  defp normalize_state(state) when is_binary(state) do
    state
    |> String.trim()
    |> String.downcase()
  end

  defp normalize_state(_state), do: ""
end
