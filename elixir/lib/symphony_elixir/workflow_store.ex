defmodule SymphonyElixir.WorkflowStore do
  @moduledoc """
  Caches the last known good workflow and reloads it when `WORKFLOW.md` changes.
  """

  use GenServer
  require Logger

  alias SymphonyElixir.Config.Schema
  alias SymphonyElixir.Workflow

  @poll_interval_ms 1_000

  defmodule State do
    @moduledoc false

    defstruct [
      :path,
      :stamp,
      :workflow,
      authority_epoch: 0,
      authority_contract: nil,
      tracker_authority_epoch: 0,
      tracker_authority_contract: nil
    ]
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec current() :: {:ok, Workflow.loaded_workflow()} | {:error, term()}
  def current do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        GenServer.call(__MODULE__, :current)

      _ ->
        Workflow.load()
    end
  end

  @spec current_with_authority() ::
          {:ok, Workflow.loaded_workflow(), term(), term()} | {:error, term()}
  def current_with_authority do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        GenServer.call(pid, :current_with_authority)

      _ ->
        case Workflow.load() do
          {:ok, workflow} ->
            authority_contract = authority_contract(workflow)
            tracker_contract = tracker_authority_contract(authority_contract)

            {:ok, workflow, {:standalone, authority_contract}, {:standalone, tracker_contract}}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @spec force_reload() :: :ok | {:error, term()}
  def force_reload do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        GenServer.call(__MODULE__, :force_reload)

      _ ->
        case Workflow.load() do
          {:ok, _workflow} -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @spec startup_digest() :: String.t()
  def startup_digest do
    GenServer.call(__MODULE__, :startup_digest)
  end

  @spec startup_snapshot_with_authority() ::
          {:ok, Workflow.loaded_workflow(), term(), term(), String.t()}
  def startup_snapshot_with_authority do
    GenServer.call(__MODULE__, :startup_snapshot_with_authority)
  end

  @spec authority_generation() :: {pid(), non_neg_integer()} | {:standalone, term()}
  def authority_generation do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        {pid, GenServer.call(pid, :authority_epoch)}

      _ ->
        case Workflow.load() do
          {:ok, workflow} -> {:standalone, authority_contract(workflow)}
          {:error, reason} -> {:standalone, {:unavailable, reason}}
        end
    end
  end

  @spec tracker_authority_generation() :: {pid(), non_neg_integer()} | {:standalone, term()}
  def tracker_authority_generation do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        {pid, GenServer.call(pid, :refresh_tracker_authority_epoch)}

      _ ->
        case Workflow.load() do
          {:ok, workflow} ->
            {:standalone, workflow |> authority_contract() |> tracker_authority_contract()}

          {:error, reason} ->
            {:standalone, {:unavailable, reason}}
        end
    end
  end

  @impl true
  def init(_opts) do
    with {:ok, state} <- load_state(Workflow.workflow_file_path()),
         :ok <- verify_expected_startup_digest(state) do
      schedule_poll()
      {:ok, state}
    else
      {:error, reason} ->
        invalidate_managed_startup_attestation()
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:current, _from, %State{} = state) do
    case reload_state(state) do
      {:ok, new_state} ->
        {:reply, {:ok, new_state.workflow}, new_state}

      {:error, _reason, new_state} ->
        {:reply, {:ok, new_state.workflow}, new_state}
    end
  end

  def handle_call(:startup_digest, _from, %State{stamp: stamp} = state) do
    {:reply, Base.encode16(stamp, case: :lower), state}
  end

  def handle_call(:startup_snapshot_with_authority, _from, %State{} = state) do
    {:ok, workflow, authority_generation, tracker_authority_generation} = authority_snapshot(state)
    digest = Base.encode16(state.stamp, case: :lower)

    {:reply, {:ok, workflow, authority_generation, tracker_authority_generation, digest}, state}
  end

  def handle_call(:current_with_authority, _from, %State{} = state) do
    case reload_state(state) do
      {:ok, new_state} ->
        {:reply, authority_snapshot(new_state), new_state}

      {:error, _reason, new_state} ->
        {:reply, authority_snapshot(new_state), new_state}
    end
  end

  def handle_call(:force_reload, _from, %State{} = state) do
    case reload_state(state) do
      {:ok, new_state} ->
        {:reply, :ok, new_state}

      {:error, reason, new_state} ->
        {:reply, {:error, reason}, new_state}
    end
  end

  def handle_call(:authority_epoch, _from, %State{} = state),
    do: {:reply, state.authority_epoch, state}

  def handle_call(:refresh_tracker_authority_epoch, _from, %State{} = state) do
    case reload_state(state) do
      {:ok, new_state} ->
        {:reply, new_state.tracker_authority_epoch, new_state}

      {:error, _reason, new_state} ->
        {:reply, new_state.tracker_authority_epoch, new_state}
    end
  end

  @impl true
  def handle_info(:poll, %State{} = state) do
    schedule_poll()

    case reload_state(state) do
      {:ok, new_state} -> {:noreply, new_state}
      {:error, _reason, new_state} -> {:noreply, new_state}
    end
  end

  defp schedule_poll do
    Process.send_after(self(), :poll, @poll_interval_ms)
  end

  defp authority_snapshot(%State{} = state) do
    pid = self()

    {:ok, state.workflow, {pid, state.authority_epoch}, {pid, state.tracker_authority_epoch}}
  end

  defp reload_state(%State{} = state) do
    path = Workflow.workflow_file_path()

    if path != state.path do
      reload_path(path, state)
    else
      reload_current_path(path, state)
    end
  end

  defp reload_path(path, state) do
    case read_snapshot(path) do
      {:ok, snapshot} ->
        reload_snapshot(path, snapshot, state)

      {:error, reason} ->
        log_reload_error(path, reason)
        {:error, reason, state}
    end
  end

  defp reload_snapshot(path, snapshot, state) do
    case load_state(path, snapshot) do
      {:ok, loaded_state} ->
        authority_changed =
          loaded_state.path != state.path or
            loaded_state.authority_contract != state.authority_contract

        authority_epoch =
          if authority_changed, do: state.authority_epoch + 1, else: state.authority_epoch

        tracker_authority_changed =
          loaded_state.path != state.path or
            loaded_state.tracker_authority_contract != state.tracker_authority_contract

        tracker_authority_epoch =
          if tracker_authority_changed,
            do: state.tracker_authority_epoch + 1,
            else: state.tracker_authority_epoch

        {:ok,
         %{
           loaded_state
           | authority_epoch: authority_epoch,
             tracker_authority_epoch: tracker_authority_epoch
         }}

      {:error, reason} ->
        log_reload_error(path, reason)
        {:error, reason, state}
    end
  end

  defp reload_current_path(path, state) do
    case read_snapshot(path) do
      {:ok, %{stamp: stamp}} when stamp == state.stamp ->
        {:ok, state}

      {:ok, snapshot} ->
        reload_snapshot(path, snapshot, state)

      {:error, reason} ->
        log_reload_error(path, reason)
        {:error, reason, state}
    end
  end

  defp load_state(path) do
    with {:ok, snapshot} <- read_snapshot(path) do
      load_state(path, snapshot)
    end
  end

  defp load_state(path, %{content: content, stamp: stamp}) do
    with {:ok, workflow} <- Workflow.parse(content),
         {:ok, _settings} <- Schema.parse(workflow.config) do
      authority_contract = authority_contract(workflow)

      {:ok,
       %State{
         path: path,
         stamp: stamp,
         workflow: workflow,
         authority_contract: authority_contract,
         tracker_authority_contract: tracker_authority_contract(authority_contract)
       }}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp authority_contract(%{config: config}) when is_map(config) do
    tracker = Map.get(config, "tracker", %{})

    if is_map(tracker) do
      operator_user_ids = Map.get(tracker, "operator_user_ids", [])

      %{
        kind: Map.get(tracker, "kind"),
        endpoint: Map.get(tracker, "endpoint", "https://api.linear.app/graphql"),
        api_key_selector: Map.get(tracker, "api_key"),
        webhook_secret_selector: Map.get(tracker, "webhook_secret"),
        project_slug: Map.get(tracker, "project_slug"),
        operator_user_ids: if(is_list(operator_user_ids), do: Enum.sort(operator_user_ids), else: operator_user_ids)
      }
    else
      {:invalid_tracker, tracker}
    end
  end

  defp tracker_authority_contract(contract) when is_map(contract),
    do: Map.drop(contract, [:operator_user_ids])

  defp tracker_authority_contract(contract), do: contract

  defp read_snapshot(path) when is_binary(path) do
    case File.read(path) do
      {:ok, content} ->
        {:ok, %{content: content, stamp: :crypto.hash(:sha256, content)}}

      {:error, reason} ->
        {:error, {:missing_workflow_file, path, reason}}
    end
  end

  defp verify_expected_startup_digest(%State{stamp: stamp}) do
    case System.get_env("SYMPHONY_EXPECTED_WORKFLOW_SHA256") do
      expected when expected in [nil, ""] ->
        :ok

      expected ->
        actual = Base.encode16(stamp, case: :lower)

        cond do
          not Regex.match?(~r/^[0-9a-f]{64}$/, expected) ->
            {:error, {:invalid_expected_workflow_sha256, expected}}

          expected == actual ->
            :ok

          true ->
            {:error, {:workflow_digest_mismatch, expected, actual}}
        end
    end
  end

  defp invalidate_managed_startup_attestation do
    managed_project = System.get_env("SYMPHONY_MANAGED_PROJECT")

    if managed_project not in [nil, ""] do
      Enum.each(
        ["SYMPHONY_STARTUP_ATTESTATION_PATH", "SYMPHONY_RUNTIME_READINESS_PATH"],
        &invalidate_attestation_env/1
      )
    end
  end

  defp invalidate_attestation_env(env_name) do
    case System.get_env(env_name) do
      path when path in [nil, ""] -> :ok
      path -> remove_regular_attestation(path)
    end
  end

  defp remove_regular_attestation(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular}} -> File.rm(path)
      _other -> :ok
    end
  end

  defp log_reload_error(path, reason) do
    Logger.error("Failed to reload workflow path=#{path} reason=#{inspect(reason)}; keeping last known good configuration")
  end
end
