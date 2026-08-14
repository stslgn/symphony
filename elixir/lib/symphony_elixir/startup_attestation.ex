defmodule SymphonyElixir.StartupAttestation do
  @moduledoc false

  alias SymphonyElixir.WorkflowStore

  @expected_digest_env "SYMPHONY_EXPECTED_WORKFLOW_SHA256"
  @attestation_path_env "SYMPHONY_STARTUP_ATTESTATION_PATH"
  @managed_project_env "SYMPHONY_MANAGED_PROJECT"
  @protocol "1"

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary
    }
  end

  @spec start_link(keyword()) :: :ignore | {:error, term()}
  def start_link(opts \\ []) do
    expected = System.get_env(@expected_digest_env)

    case {managed?(), expected} do
      {true, expected} when expected in [nil, ""] ->
        {:error, :missing_managed_startup_digest}

      {false, expected} when expected in [nil, ""] ->
        :ignore

      {_managed, expected} ->
        attest(expected, opts)
    end
  end

  defp attest(expected, opts) do
    path = System.get_env(@attestation_path_env)
    actual = WorkflowStore.startup_digest()

    cond do
      path in [nil, ""] ->
        {:error, :missing_startup_attestation_path}

      expected != actual ->
        {:error, {:startup_attestation_digest_mismatch, expected, actual}}

      true ->
        with {:ok, process_start} <- process_start(opts),
             :ok <- write_attestation(path, actual, process_start) do
          :ignore
        end
    end
  end

  defp process_start(opts) do
    command_result =
      case Keyword.fetch(opts, :process_start_result) do
        {:ok, result} ->
          result

        :error ->
          System.cmd("/bin/ps", ["-p", System.pid(), "-o", "lstart="],
            env: [{"LC_ALL", "C"}, {"TZ", "UTC"}],
            stderr_to_stdout: true
          )
      end

    case command_result do
      {output, 0} ->
        case String.trim(output) do
          "" -> {:error, :empty_process_start}
          process_start -> {:ok, process_start}
        end

      {output, status} ->
        {:error, {:process_start_failed, status, String.trim(output)}}
    end
  end

  defp write_attestation(path, digest, process_start) do
    temporary_path = "#{path}.tmp.#{System.unique_integer([:positive])}"

    content =
      Enum.join(
        [
          "protocol=#{@protocol}",
          "pid=#{System.pid()}",
          "process_start=#{process_start}",
          "workflow_sha256=#{digest}"
        ],
        "\n"
      ) <> "\n"

    result =
      with :ok <- File.write(temporary_path, content, [:exclusive]),
           :ok <- File.chmod(temporary_path, 0o600) do
        File.rename(temporary_path, path)
      end

    _ = File.rm(temporary_path)

    case result do
      :ok -> :ok
      {:error, reason} -> {:error, {:startup_attestation_write_failed, reason}}
    end
  end

  defp managed?, do: System.get_env(@managed_project_env) not in [nil, ""]
end
