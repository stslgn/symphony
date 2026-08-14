defmodule SymphonyElixir.RuntimeReadiness do
  @moduledoc false

  use GenServer

  alias SymphonyElixir.StartupAttestation

  @readiness_path_env "SYMPHONY_RUNTIME_READINESS_PATH"
  @managed_project_env "SYMPHONY_MANAGED_PROJECT"
  @protocol "2"

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

  @doc false
  @spec attest(keyword()) :: {:ok, map()} | {:error, term()}
  def attest(opts \\ []) do
    if managed?() do
      path = System.get_env(@readiness_path_env)
      admission = Keyword.get_lazy(opts, :admission, &StartupAttestation.evidence/0)

      with :ok <- validate_path(path),
           :ok <- validate_admission(admission),
           :ok <- write_attestation(path, admission) do
        {:ok, Map.put(admission, :path, path)}
      end
    else
      {:ok, %{path: nil}}
    end
  end

  @impl true
  def init(opts) do
    case attest(opts) do
      {:ok, state} -> {:ok, state}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, %{path: path}) when is_binary(path) do
    _ = remove_regular_file(path)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp validate_path(path) when path in [nil, ""], do: {:error, :missing_runtime_readiness_path}
  defp validate_path(_path), do: :ok

  defp validate_admission(%{
         process_start: process_start,
         runtime_sha256: runtime_sha256,
         workflow_sha256: workflow_sha256
       })
       when is_binary(process_start) and process_start != "" do
    if valid_sha256?(runtime_sha256) and valid_sha256?(workflow_sha256) do
      :ok
    else
      {:error, :invalid_startup_admission_evidence}
    end
  end

  defp validate_admission(_admission), do: {:error, :invalid_startup_admission_evidence}

  defp write_attestation(path, admission) do
    temporary_path = "#{path}.tmp.#{System.unique_integer([:positive])}"

    content =
      Enum.join(
        [
          "protocol=#{@protocol}",
          "pid=#{System.pid()}",
          "process_start=#{admission.process_start}",
          "workflow_sha256=#{admission.workflow_sha256}",
          "runtime_sha256=#{admission.runtime_sha256}"
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
      {:error, reason} -> {:error, {:runtime_readiness_write_failed, reason}}
    end
  end

  defp remove_regular_file(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular}} -> File.rm(path)
      {:ok, _stat} -> {:error, :unsafe_runtime_readiness_path}
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:runtime_readiness_invalidation_failed, reason}}
    end
  end

  defp valid_sha256?(value), do: is_binary(value) and value =~ ~r/\A[0-9a-fA-F]{64}\z/
  defp managed?, do: System.get_env(@managed_project_env) not in [nil, ""]
end
