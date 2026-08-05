defmodule SymphonyElixir.PollTaskGuardTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.PollTaskGuard

  defmodule TaskSupervisorStub do
    use GenServer

    def start_link(name), do: GenServer.start_link(__MODULE__, nil, name: name)
    def init(state), do: {:ok, state}

    def handle_call(_request, _from, state) do
      {:reply, {:error, :not_found}, state}
    end
  end

  test "reports a competing guard and ignores unrelated messages" do
    owner = self()
    owner_key = {:busy_guard, System.unique_integer([:positive])}

    work_fn = fn request ->
      send(owner, {:worker_ready, self()})

      receive do
        :complete -> {:completed, request}
      end
    end

    assert {:ok, first_guard} = PollTaskGuard.start(owner, owner_key, 1, :first, work_fn)
    assert_receive {:poll_guard_started, ^first_guard, 1, worker_pid}
    assert_receive {:worker_ready, ^worker_pid}

    assert {:ok, second_guard} = PollTaskGuard.start(owner, owner_key, 2, :second, work_fn)

    assert_receive {:poll_guard_busy, ^second_guard, 2, ^first_guard}
    refute Process.alive?(second_guard)

    send(first_guard, :unrelated_message)
    send(worker_pid, :complete)

    assert_receive {:poll_guard_result, ^first_guard, 1, {:completed, :first}}
  end

  test "forces a worker down when graceful supervisor termination produces no DOWN" do
    owner = self()
    owner_key = {:forced_stop, System.unique_integer([:positive])}
    supervisor_name = SymphonyElixir.TaskSupervisor
    real_supervisor = Process.whereis(supervisor_name)

    work_fn = fn _request ->
      send(owner, {:blocking_worker_ready, self()})

      receive do
        :never -> :ok
      end
    end

    assert {:ok, guard} = PollTaskGuard.start(owner, owner_key, 3, :request, work_fn)
    assert_receive {:poll_guard_started, ^guard, 3, worker_pid}
    assert_receive {:blocking_worker_ready, ^worker_pid}

    guard_ref = Process.monitor(guard)
    worker_ref = Process.monitor(worker_pid)

    assert Process.unregister(supervisor_name)
    assert {:ok, stub} = TaskSupervisorStub.start_link(supervisor_name)

    try do
      assert :ok = PollTaskGuard.cancel(guard, 3)
      assert_receive {:DOWN, ^worker_ref, :process, ^worker_pid, :killed}, 1_500
      assert_receive {:DOWN, ^guard_ref, :process, ^guard, :normal}, 500
    after
      if Process.alive?(stub), do: GenServer.stop(stub)
      if Process.alive?(real_supervisor), do: Process.register(real_supervisor, supervisor_name)
    end
  end
end
