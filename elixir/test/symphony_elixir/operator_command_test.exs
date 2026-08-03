defmodule SymphonyElixir.OperatorCommandTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Linear.Comment
  alias SymphonyElixir.OperatorCommand

  @operator_user_ids ["operator-1"]

  test "parses only the bounded command vocabulary" do
    for {body, action} <- [
          {"$approve", "approve"},
          {" $APPROVED looks good", "approve"},
          {"$retry after reconnect", "retry"},
          {"$reject", "reject"},
          {"$stop", "stop"},
          {"👍", "approve"},
          {":+1:", "approve"}
        ] do
      assert OperatorCommand.parse_comment(comment(body), @operator_user_ids) == {:ok, action}
    end

    assert OperatorCommand.actions() == ["approve", "reject", "retry", "stop"]
  end

  test "ignores free-form, unsupported, self-authored, and mirrored comments" do
    assert OperatorCommand.parse_comment(comment("please retry"), @operator_user_ids) == :ignore
    assert OperatorCommand.parse_comment(comment("$skip-review"), @operator_user_ids) == :ignore

    assert OperatorCommand.parse_comment(comment("$retry-acceptance"), @operator_user_ids) ==
             :ignore

    assert OperatorCommand.parse_comment(comment("$stop-now"), @operator_user_ids) == :ignore
    assert OperatorCommand.parse_comment(comment("`$retry`"), @operator_user_ids) == :ignore

    assert OperatorCommand.parse_comment(
             comment(String.duplicate("x", 4097)),
             @operator_user_ids
           ) == :ignore

    assert OperatorCommand.parse_comment(comment(nil), @operator_user_ids) == :ignore

    assert OperatorCommand.parse_comment(
             %{comment("$stop") | author_id: nil},
             @operator_user_ids
           ) == :ignore

    assert OperatorCommand.parse_comment(comment("$stop"), []) == :ignore

    assert OperatorCommand.parse_comment(
             %{comment("$stop") | author_id: "untrusted-user"},
             @operator_user_ids
           ) == :ignore

    assert OperatorCommand.parse_comment(
             %{comment("$stop") | author_is_me: true},
             @operator_user_ids
           ) == :ignore

    assert OperatorCommand.parse_comment(
             %{
               comment("$stop")
               | external_thread_type: "github"
             },
             @operator_user_ids
           ) == :ignore
  end

  defp comment(body) do
    %Comment{
      id: "comment-1",
      body: body,
      created_at: ~U[2026-08-03 10:00:00Z],
      author_id: "operator-1"
    }
  end
end
