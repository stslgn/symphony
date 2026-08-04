defmodule SymphonyElixir.Workflow do
  @moduledoc """
  Loads workflow configuration and prompt from WORKFLOW.md.
  """

  alias SymphonyElixir.WorkflowStore

  @workflow_file_name "WORKFLOW.md"

  @spec workflow_file_path() :: Path.t()
  def workflow_file_path do
    Application.get_env(:symphony_elixir, :workflow_file_path) ||
      Path.join(File.cwd!(), @workflow_file_name)
  end

  @spec set_workflow_file_path(Path.t()) :: :ok
  def set_workflow_file_path(path) when is_binary(path) do
    Application.put_env(:symphony_elixir, :workflow_file_path, path)
    maybe_reload_store()
    :ok
  end

  @spec clear_workflow_file_path() :: :ok
  def clear_workflow_file_path do
    Application.delete_env(:symphony_elixir, :workflow_file_path)
    maybe_reload_store()
    :ok
  end

  @type loaded_workflow :: %{
          config: map(),
          prompt: String.t(),
          prompt_template: String.t(),
          runtime_prompt_mode: String.t()
        }

  @spec current() :: {:ok, loaded_workflow()} | {:error, term()}
  def current do
    case Process.whereis(WorkflowStore) do
      pid when is_pid(pid) ->
        WorkflowStore.current()

      _ ->
        load()
    end
  end

  @spec load() :: {:ok, loaded_workflow()} | {:error, term()}
  def load do
    load(workflow_file_path())
  end

  @spec load(Path.t()) :: {:ok, loaded_workflow()} | {:error, term()}
  def load(path) when is_binary(path) do
    case File.read(path) do
      {:ok, content} ->
        parse(content)

      {:error, reason} ->
        {:error, {:missing_workflow_file, path, reason}}
    end
  end

  defp parse(content) do
    {front_matter_lines, prompt_lines} = split_front_matter(content)

    case front_matter_yaml_to_map(front_matter_lines) do
      {:ok, front_matter} ->
        prompt = Enum.join(prompt_lines, "\n") |> String.trim()
        runtime_prompt_mode = runtime_prompt_mode(front_matter)

        case runtime_prompt_template(prompt, runtime_prompt_mode) do
          {:ok, prompt_template} ->
            {:ok,
             %{
               config: front_matter,
               prompt: prompt,
               prompt_template: prompt_template,
               runtime_prompt_mode: runtime_prompt_mode
             }}

          {:error, reason} ->
            {:error, {:workflow_parse_error, reason}}
        end

      {:error, :workflow_front_matter_not_a_map} ->
        {:error, :workflow_front_matter_not_a_map}

      {:error, reason} ->
        {:error, {:workflow_parse_error, reason}}
    end
  end

  defp split_front_matter(content) do
    lines = String.split(content, ~r/\R/, trim: false)

    case lines do
      ["---" | tail] ->
        {front, rest} = Enum.split_while(tail, &(&1 != "---"))

        case rest do
          ["---" | prompt_lines] -> {front, prompt_lines}
          _ -> {front, []}
        end

      _ ->
        {[], lines}
    end
  end

  defp front_matter_yaml_to_map(lines) do
    yaml = Enum.join(lines, "\n")

    if String.trim(yaml) == "" do
      {:ok, %{}}
    else
      case YamlElixir.read_from_string(yaml) do
        {:ok, decoded} when is_map(decoded) -> {:ok, decoded}
        {:ok, _} -> {:error, :workflow_front_matter_not_a_map}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp runtime_prompt_mode(front_matter) when is_map(front_matter) do
    case get_in(front_matter, ["workflow", "runtime_prompt_mode"]) do
      mode when mode in ["managed", "full_prompt_compat"] -> mode
      nil -> "managed"
      mode -> {:invalid, mode}
    end
  end

  defp runtime_prompt_template(prompt, runtime_prompt_mode) when is_binary(prompt) do
    lines = String.split(prompt, ~r/\R/, trim: false)

    case last_runtime_prompt_heading_index(lines) do
      nil ->
        case runtime_prompt_mode do
          "full_prompt_compat" -> {:ok, prompt}
          "managed" -> {:error, :missing_runtime_prompt_heading}
          {:invalid, mode} -> {:error, {:invalid_runtime_prompt_mode, mode}}
        end

      index ->
        prompt_template =
          lines
          |> Enum.drop(index)
          |> Enum.join("\n")
          |> String.trim()

        case runtime_prompt_mode do
          {:invalid, mode} -> {:error, {:invalid_runtime_prompt_mode, mode}}
          _ -> {:ok, prompt_template}
        end
    end
  end

  defp last_runtime_prompt_heading_index(lines) when is_list(lines) do
    lines
    |> Enum.with_index()
    |> Enum.reduce(nil, fn {line, index}, last_index ->
      if line == "## Symphony Runtime Prompt" do
        index
      else
        last_index
      end
    end)
  end

  defp maybe_reload_store do
    if Process.whereis(WorkflowStore) do
      _ = WorkflowStore.force_reload()
    end

    :ok
  end
end
