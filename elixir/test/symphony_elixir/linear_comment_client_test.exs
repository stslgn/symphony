defmodule SymphonyElixir.LinearCommentClientTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Linear.Client

  test "fetches and normalizes paginated comments from a bounded timestamp" do
    parent = self()

    graphql_fun = fn query, variables ->
      send(parent, {:graphql_call, query, variables})

      case variables.after do
        nil ->
          {:ok,
           comment_page(
             [comment_node("comment-1", "$retry", "2026-08-03T10:00:01.000Z")],
             true,
             "cursor-1"
           )}

        "cursor-1" ->
          {:ok,
           comment_page(
             [
               comment_node(
                 "comment-2",
                 "$stop",
                 "2026-08-03T10:00:02.000Z",
                 true,
                 "github"
               )
             ],
             false,
             nil
           )}
      end
    end

    assert {:ok, [first, second]} =
             Client.fetch_comments_since_for_test(
               "issue-1",
               ~U[2026-08-03 10:00:00Z],
               graphql_fun
             )

    assert first.id == "comment-1"
    assert first.body == "$retry"
    refute first.author_is_me
    assert second.id == "comment-2"
    assert second.author_is_me
    assert second.external_thread_type == "github"

    assert_receive {:graphql_call, query, %{createdAfter: "2026-08-03T10:00:00Z", after: nil}}
    assert query =~ "DateTimeOrDuration!"
    assert query =~ "orderBy: createdAt"
    assert_receive {:graphql_call, _query, %{after: "cursor-1"}}
  end

  test "fails closed for missing issues and malformed comment pages" do
    missing = fn _query, _variables -> {:ok, %{"data" => %{"issue" => nil}}} end
    malformed = fn _query, _variables -> {:ok, %{"data" => %{}}} end

    assert {:error, :linear_issue_not_found} =
             Client.fetch_comments_since_for_test(
               "issue-missing",
               ~U[2026-08-03 10:00:00Z],
               missing
             )

    assert {:error, :linear_unknown_payload} =
             Client.fetch_comments_since_for_test(
               "issue-bad",
               ~U[2026-08-03 10:00:00Z],
               malformed
             )
  end

  defp comment_page(nodes, has_next_page, end_cursor) do
    %{
      "data" => %{
        "issue" => %{
          "comments" => %{
            "nodes" => nodes,
            "pageInfo" => %{
              "hasNextPage" => has_next_page,
              "endCursor" => end_cursor
            }
          }
        }
      }
    }
  end

  defp comment_node(id, body, created_at, is_me \\ false, external_thread \\ nil) do
    %{
      "id" => id,
      "body" => body,
      "createdAt" => created_at,
      "user" => %{"isMe" => is_me},
      "externalThread" => if(is_binary(external_thread), do: %{"type" => external_thread}, else: nil)
    }
  end
end
