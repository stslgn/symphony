defmodule SymphonyElixir.WorkflowStore do
  @moduledoc """
  Caches the last known good workflow and reloads it when `WORKFLOW.md` changes.
  """

  use GenServer
  require Logger

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
    case load_state(Workflow.workflow_file_path()) do
      {:ok, state} ->
        schedule_poll()
        {:ok, state}

      {:error, reason} ->
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

  def handle_call(:tracker_authority_epoch, _from, %State{} = state),
    do: {:reply, state.tracker_authority_epoch, state}

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
    case load_state(path) do
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
    case current_stamp(path) do
      {:ok, stamp} when stamp == state.stamp ->
        {:ok, state}

      {:ok, _stamp} ->
        reload_path(path, state)

      {:error, reason} ->
        log_reload_error(path, reason)
        {:error, reason, state}
    end
  end

  defp load_state(path) do
    with {:ok, workflow} <- Workflow.load(path),
         {:ok, stamp} <- current_stamp(path) do
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

  defp current_stamp(path) when is_binary(path) do
    with {:ok, stat} <- File.stat(path, time: :posix),
         {:ok, content} <- File.read(path) do
      {:ok, {stat.mtime, stat.size, :erlang.phash2(content)}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp log_reload_error(path, reason) do
    Logger.error("Failed to reload workflow path=#{path} reason=#{inspect(reason)}; keeping last known good configuration")
  end
end
