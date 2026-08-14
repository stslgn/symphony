defmodule SymphonyElixir.StartupAttestation do
  @moduledoc false

  use GenServer

  alias SymphonyElixir.{Config, WorkflowStore}

  @expected_digest_env "SYMPHONY_EXPECTED_WORKFLOW_SHA256"
  @attestation_path_env "SYMPHONY_STARTUP_ATTESTATION_PATH"
  @runtime_readiness_path_env "SYMPHONY_RUNTIME_READINESS_PATH"
  @expected_runtime_digest_env "SYMPHONY_EXPECTED_RUNTIME_SHA256"
  @runtime_image_path_env "SYMPHONY_RUNTIME_IMAGE_PATH"
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

  @spec settings_with_authority!() :: {Config.Schema.t(), term(), term()}
  def settings_with_authority! do
    GenServer.call(__MODULE__, :settings_with_authority)
  end

  @spec evidence(GenServer.server()) :: map()
  def evidence(server \\ __MODULE__) do
    GenServer.call(server, :evidence)
  end

  @doc false
  @spec admit(keyword()) :: {:ok, map()} | {:error, term()}
  def admit(opts \\ []), do: build_admission(opts)

  @impl true
  def init(opts) do
    case invalidate_runtime_readiness() do
      :ok -> init_admission(opts)
      {:error, reason} -> {:stop, reason}
    end
  end

  defp init_admission(opts) do
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

  def handle_call(:evidence, _from, admission) do
    {:reply, Map.take(admission, [:process_start, :runtime_sha256, :workflow_sha256]), admission}
  end

  defp build_admission(opts) do
    expected = System.get_env(@expected_digest_env)

    with {:ok, workflow, authority_generation, tracker_authority_generation, actual} <-
           WorkflowStore.startup_snapshot_with_authority(),
         settings = Config.settings_for_workflow!(workflow),
         {:ok, process_start, runtime_sha256} <- attest_if_required(expected, actual, opts) do
      {:ok,
       %{
         settings: settings,
         authority_generation: authority_generation,
         tracker_authority_generation: tracker_authority_generation,
         workflow_sha256: actual,
         runtime_sha256: runtime_sha256,
         process_start: process_start
       }}
    end
  end

  defp attest_if_required(expected, actual, opts) do
    case {managed?(), expected} do
      {true, expected} when expected in [nil, ""] ->
        {:error, :missing_managed_startup_digest}

      {false, expected} when expected in [nil, ""] ->
        {:ok, nil, nil}

      {_managed, expected} ->
        attest(expected, actual, opts)
    end
  end

  defp attest(expected, actual, opts) do
    path = System.get_env(@attestation_path_env)
    runtime_sha256 = System.get_env(@expected_runtime_digest_env)

    cond do
      path in [nil, ""] ->
        {:error, :missing_startup_attestation_path}

      expected != actual ->
        {:error, {:startup_attestation_digest_mismatch, expected, actual}}

      managed?() and not valid_sha256?(runtime_sha256) ->
        {:error, :missing_managed_runtime_digest}

      true ->
        with {:ok, verified_runtime_sha256} <- verify_runtime_digest(runtime_sha256, opts),
             {:ok, process_start} <- process_start(opts),
             :ok <- write_attestation(path, actual, verified_runtime_sha256, process_start) do
          {:ok, process_start, verified_runtime_sha256}
        end
    end
  end

  defp verify_runtime_digest(expected, opts) do
    if managed?() do
      verify_managed_runtime_digest(expected, opts)
    else
      {:ok, expected}
    end
  end

  defp verify_managed_runtime_digest(expected, opts) do
    with {:ok, expected_path} <- runtime_image_path(),
         {:ok, script_path} <- executing_script_path(opts),
         :ok <- compare_runtime_paths(expected_path, script_path),
         :ok <- validate_runtime_image(expected_path),
         {:ok, content} <- read_runtime_image(expected_path) do
      actual = :sha256 |> :crypto.hash(content) |> Base.encode16(case: :lower)
      compare_runtime_digests(expected, actual)
    end
  end

  defp compare_runtime_digests(expected, actual) do
    if actual == String.downcase(expected) do
      {:ok, actual}
    else
      {:error, {:runtime_image_digest_mismatch, expected, actual}}
    end
  end

  defp runtime_image_path do
    case System.get_env(@runtime_image_path_env) do
      path when path in [nil, ""] -> {:error, :missing_managed_runtime_image_path}
      path -> {:ok, Path.expand(path)}
    end
  end

  defp executing_script_path(opts) do
    result =
      Keyword.get_lazy(opts, :script_name_result, fn ->
        {:ok, :escript.script_name() |> List.to_string()}
      end)

    case result do
      {:ok, path} when is_binary(path) and path != "" -> {:ok, Path.expand(path)}
      {:error, reason} -> {:error, {:runtime_script_name_unavailable, reason}}
      _other -> {:error, :runtime_script_name_unavailable}
    end
  end

  defp compare_runtime_paths(path, path), do: :ok

  defp compare_runtime_paths(expected, actual),
    do: {:error, {:runtime_image_path_mismatch, expected, actual}}

  defp validate_runtime_image(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular}} -> :ok
      {:ok, _stat} -> {:error, :unsafe_managed_runtime_image}
      {:error, reason} -> {:error, {:runtime_image_stat_failed, reason}}
    end
  end

  defp read_runtime_image(path) do
    case File.read(path) do
      {:ok, content} -> {:ok, content}
      {:error, reason} -> {:error, {:runtime_image_read_failed, reason}}
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

  defp write_attestation(path, digest, runtime_sha256, process_start) do
    temporary_path = "#{path}.tmp.#{System.unique_integer([:positive])}"

    content =
      Enum.join(
        [
          "protocol=#{@protocol}",
          "pid=#{System.pid()}",
          "process_start=#{process_start}",
          "workflow_sha256=#{digest}",
          "runtime_sha256=#{runtime_sha256 || "unmanaged"}"
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

  defp invalidate_runtime_readiness do
    case System.get_env(@runtime_readiness_path_env) do
      path when path in [nil, ""] -> :ok
      path -> remove_regular_file(path)
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
