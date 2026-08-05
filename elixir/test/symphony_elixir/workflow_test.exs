defmodule SymphonyElixir.WorkflowTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Workflow

  test "rejects an invalid runtime prompt mode when the heading is missing" do
    path = workflow_path("invalid-mode-without-heading")

    File.write!(path, """
    ---
    workflow:
      runtime_prompt_mode: unsupported
    ---

    Worker prompt without the managed heading.
    """)

    assert {:error, {:workflow_parse_error, {:invalid_runtime_prompt_mode, "unsupported"}}} =
             Workflow.load(path)
  end

  test "rejects an invalid runtime prompt mode when the heading is present" do
    path = workflow_path("invalid-mode-with-heading")

    File.write!(path, """
    ---
    workflow:
      runtime_prompt_mode: unsupported
    ---

    ## Symphony Runtime Prompt

    Worker prompt.
    """)

    assert {:error, {:workflow_parse_error, {:invalid_runtime_prompt_mode, "unsupported"}}} =
             Workflow.load(path)
  end

  defp workflow_path(name) do
    directory = Path.join(System.tmp_dir!(), "symphony-workflow-test-#{System.unique_integer([:positive])}")
    File.mkdir_p!(directory)
    path = Path.join(directory, "#{name}.md")
    on_exit(fn -> File.rm_rf!(directory) end)
    path
  end
end
