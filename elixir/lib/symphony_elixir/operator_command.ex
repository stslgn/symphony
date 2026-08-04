defmodule SymphonyElixir.OperatorCommand do
  @moduledoc """
  Parses the small, explicit operator-command vocabulary from Linear comments.

  Free-form comments are never steering input. Only a native Linear comment
  beginning with a supported `$` command, or a standalone thumbs-up approval,
  can produce an action.
  """

  alias SymphonyElixir.Linear.Comment

  @actions ["approve", "reject", "retry", "stop"]
  @command_pattern ~r/^\s*\$(approve|approved|reject|retry|stop)(?:\s|$)/i
  @max_body_bytes 4096
  @thumbs_up ["👍", ":+1:", ":+1"]

  @spec actions() :: [String.t()]
  def actions, do: @actions

  @spec parse_comment(Comment.t(), [String.t()]) :: {:ok, String.t()} | :ignore
  def parse_comment(%Comment{author_is_me: true}, _operator_user_ids), do: :ignore

  def parse_comment(%Comment{external_thread_type: type}, _operator_user_ids)
      when is_binary(type),
      do: :ignore

  def parse_comment(%Comment{author_id: author_id} = comment, operator_user_ids)
      when is_binary(author_id) and is_list(operator_user_ids) do
    if author_id in operator_user_ids, do: parse_body(comment), else: :ignore
  end

  def parse_comment(_comment, _operator_user_ids), do: :ignore

  defp parse_body(%Comment{body: body})
       when is_binary(body) and byte_size(body) <= @max_body_bytes do
    body = String.trim(body)

    cond do
      body in @thumbs_up ->
        {:ok, "approve"}

      match = Regex.run(@command_pattern, body) ->
        action = match |> Enum.at(1) |> String.downcase()
        {:ok, if(action == "approved", do: "approve", else: action)}

      true ->
        :ignore
    end
  end

  defp parse_body(_comment), do: :ignore
end
