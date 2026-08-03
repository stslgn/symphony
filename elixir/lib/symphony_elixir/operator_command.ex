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

  @spec parse_comment(Comment.t()) :: {:ok, String.t()} | :ignore
  def parse_comment(%Comment{author_is_me: true}), do: :ignore

  def parse_comment(%Comment{external_thread_type: type}) when is_binary(type),
    do: :ignore

  def parse_comment(%Comment{body: body})
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

  def parse_comment(_comment), do: :ignore
end
