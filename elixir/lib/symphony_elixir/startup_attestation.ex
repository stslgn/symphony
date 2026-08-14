defmodule SymphonyElixir.StartupAttestation do
  @moduledoc false

  use GenServer

  alias SymphonyElixir.{Config, WorkflowStore}

  @expected_digest_env "SYMPHONY_EXPECTED_WORKFLOW_SHA256"
  @attestation_path_env "SYMPHONY_STARTUP_ATTESTATION_PATH"
  @managed_project_env "SYMPHONY_MANAGED_PROJECT"
  @protocol "1"

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [[{:name, __MODULE__} | opts]]},
      restart: :permanent,
      type: :worker
    }
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @spec settings_with_authority!() :: {Config.Schema.t(), term(), term()}
  def settings_with_authority! do
    GenServer.call(__MODULE__, :settings_with_authority)
  end

  @doc false
  @spec admit(keyword()) :: {:ok, map()} | {:error, term()}
  def admit(opts \\ []), do: build_admission(opts)

  @impl true
  def init(opts) do
    case admit(opts) do
      {:ok, admission} -> {:ok, admission}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:settings_with_authority, _from, admission) do
    reply =
      {admission.settings, admission.authority_generation, admission.tracker_authority_generation}

    {:reply, reply, admission}
  end

  defp build_admission(opts) do
    expected = System.get_env(@expected_digest_env)

    with {:ok, workflow, authority_generation, tracker_authority_generation, actual} <-
           WorkflowStore.startup_snapshot_with_authority(),
         settings = Config.settings_for_workflow!(workflow),
         {:ok, process_start} <- attest_if_required(expected, actual, opts) do
      {:ok,
       %{
         settings: settings,
         authority_generation: authority_generation,
         tracker_authority_generation: tracker_authority_generation,
         workflow_sha256: actual,
         process_start: process_start
       }}
    end
  end

  defp attest_if_required(expected, actual, opts) do
    case {managed?(), expected} do
      {true, expected} when expected in [nil, ""] ->
        {:error, :missing_managed_startup_digest}

      {false, expected} when expected in [nil, ""] ->
        {:ok, nil}

      {_managed, expected} ->
        attest(expected, actual, opts)
    end
  end

  defp attest(expected, actual, opts) do
    path = System.get_env(@attestation_path_env)

    cond do
      path in [nil, ""] ->
        {:error, :missing_startup_attestation_path}

      expected != actual ->
        {:error, {:startup_attestation_digest_mismatch, expected, actual}}

      true ->
        with {:ok, process_start} <- process_start(opts),
             :ok <- write_attestation(path, actual, process_start) do
          {:ok, process_start}
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
