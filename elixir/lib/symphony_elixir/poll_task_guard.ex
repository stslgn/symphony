defmodule SymphonyElixir.PollTaskGuard do
  @moduledoc false

  @registry SymphonyElixir.PollTaskRegistry
  @guard_supervisor SymphonyElixir.PollGuardSupervisor
  @task_supervisor SymphonyElixir.TaskSupervisor
  @worker_stop_timeout_ms 1_000

  @spec start(pid(), term(), non_neg_integer(), term(), (term() -> term())) ::
          {:ok, pid()} | {:error, term()}
  def start(owner, owner_key, generation, request, work_fn)
      when is_pid(owner) and is_integer(generation) and generation >= 0 and
             is_function(work_fn, 1) do
    child_spec = %{
      id: {__MODULE__, make_ref()},
      start: {Task, :start_link, [fn -> run(owner, owner_key, generation, request, work_fn) end]},
      restart: :temporary
    }

    DynamicSupervisor.start_child(@guard_supervisor, child_spec)
  end

  @spec cancel(pid(), non_neg_integer()) :: :ok
  def cancel(guard_pid, generation)
      when is_pid(guard_pid) and is_integer(generation) and generation >= 0 do
    send(guard_pid, {:cancel_poll_guard, self(), generation})
    :ok
  end

  defp run(owner, owner_key, generation, request, work_fn) do
    Process.flag(:trap_exit, true)
    owner_ref = Process.monitor(owner)
    registry_key = {:orchestrator_poll, owner_key}

    case Registry.register(@registry, registry_key, nil) do
      {:ok, _registry_owner} ->
        run_registered(owner, owner_ref, registry_key, generation, request, work_fn)

      {:error, {:already_registered, existing_guard}} ->
        send(owner, {:poll_guard_busy, self(), generation, existing_guard})
        Process.demonitor(owner_ref, [:flush])
        :ok
    end
  end

  defp run_registered(owner, owner_ref, registry_key, generation, request, work_fn) do
    task = Task.Supervisor.async(@task_supervisor, fn -> work_fn.(request) end)
    send(owner, {:poll_guard_started, self(), generation, task.pid})
    guard_loop(owner, owner_ref, registry_key, generation, task, :pending)
  end

  defp guard_loop(owner, owner_ref, registry_key, generation, task, task_result) do
    receive do
      {ref, result} when ref == task.ref ->
        guard_loop(owner, owner_ref, registry_key, generation, task, {:ok, result})

      {:DOWN, ref, :process, pid, reason} when ref == task.ref and pid == task.pid ->
        Process.unlink(task.pid)
        Process.demonitor(owner_ref, [:flush])
        Registry.unregister(@registry, registry_key)
        report_worker_completion(owner, generation, task_result, reason)

      {:EXIT, pid, _reason} when pid == task.pid ->
        guard_loop(owner, owner_ref, registry_key, generation, task, task_result)

      {:DOWN, ref, :process, pid, _reason} when ref == owner_ref and pid == owner ->
        stop_worker(task)
        :ok

      {:cancel_poll_guard, pid, cancel_generation}
      when pid == owner and cancel_generation == generation ->
        Process.demonitor(owner_ref, [:flush])
        stop_worker(task)
        :ok

      _message ->
        guard_loop(owner, owner_ref, registry_key, generation, task, task_result)
    end
  end

  defp report_worker_completion(owner, generation, {:ok, result}, _reason) do
    send(owner, {:poll_guard_result, self(), generation, result})
    :ok
  end

  defp report_worker_completion(owner, generation, :pending, reason) do
    send(owner, {:poll_guard_failed, self(), generation, reason})
    :ok
  end

  defp stop_worker(task) do
    _result = Task.Supervisor.terminate_child(@task_supervisor, task.pid)

    receive do
      {:DOWN, ref, :process, pid, _reason} when ref == task.ref and pid == task.pid ->
        Process.unlink(task.pid)
        :ok
    after
      @worker_stop_timeout_ms ->
        Process.exit(task.pid, :kill)
        await_forced_worker_stop(task)
    end
  end

  defp await_forced_worker_stop(task) do
    receive do
      {:DOWN, ref, :process, pid, _reason} when ref == task.ref and pid == task.pid ->
        Process.unlink(task.pid)
        :ok
    end
  end
end
