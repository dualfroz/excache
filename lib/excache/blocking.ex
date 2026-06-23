defmodule Excache.Blocking do
  @moduledoc false

  use GenServer

  alias Excache.Store
  alias Excache.Command.Reply

  @name __MODULE__

  def start_link(_arg), do: GenServer.start_link(__MODULE__, nil, name: @name)

  def acquire(request), do: GenServer.call(@name, {:acquire, request, :block})

  def attempt(request) do
    {:served, term} = GenServer.call(@name, {:acquire, request, :immediate})
    term
  end

  def signal([]), do: []
  def signal(keys), do: GenServer.call(@name, {:ready, keys})

  def takeWrites, do: GenServer.call(@name, :takeWrites)

  def waitingCount(key), do: GenServer.call(@name, {:waitingCount, key})

  @impl true
  def init(_arg), do: {:ok, %{queues: %{}, waiters: %{}, monitors: %{}, writes: []}}

  @impl true
  def handle_call({:acquire, request, mode}, {pid, _tag}, state) do
    waiter = %{
      ref: make_ref(),
      pid: pid,
      keys: request.keys,
      op: request.op,
      monitor: nil,
      timer: nil
    }

    state = enqueue(state, waiter)
    {state, served} = drain(state, request.keys, [])

    case {deliver(served, waiter.ref), mode} do
      {nil, :immediate} -> {:reply, {:served, missReply(request.op)}, dropWaiter(state, waiter)}
      {nil, :block} -> {:reply, {:blocked, waiter.ref}, arm(state, waiter, request.timeout)}
      {term, _mode} -> {:reply, {:served, term}, state}
    end
  end

  def handle_call({:ready, keys}, _from, state) do
    {state, served} = drain(state, keys, [])
    deliver(served, nil)
    takeWrites(state)
  end

  def handle_call(:takeWrites, _from, state), do: takeWrites(state)

  def handle_call({:waitingCount, key}, _from, state) do
    count =
      case Map.fetch(state.queues, key) do
        {:ok, queue} -> :queue.len(queue)
        :error -> 0
      end

    {:reply, count, state}
  end

  @impl true
  def handle_info({:blockingTimeout, ref}, state) do
    case Map.fetch(state.waiters, ref) do
      {:ok, waiter} ->
        send(waiter.pid, {:excacheUnblocked, ref, {:array, nil}})
        {:noreply, dropWaiter(state, waiter)}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    ref = Map.fetch!(state.monitors, monitor)
    {:noreply, dropWaiter(state, Map.fetch!(state.waiters, ref))}
  end

  defp drain(state, [], served), do: {state, served}

  defp drain(state, [key | rest], served) do
    {state, served, chained} = drainKey(state, key, served, [])
    drain(state, chained ++ rest, served)
  end

  defp drainKey(state, key, served, chained) do
    case headWaiter(state, key) do
      nil ->
        {state, served, chained}

      waiter ->
        case serveWaiter(waiter) do
          :empty ->
            {state, served, chained}

          {:ok, term, ready, writes} ->
            state = recordWrites(dropWaiter(state, waiter), writes)
            drainKey(state, key, [{waiter, term} | served], ready ++ chained)

          {:error, term} ->
            drainKey(dropWaiter(state, waiter), key, [{waiter, term} | served], chained)
        end
    end
  end

  defp recordWrites(state, writes), do: %{state | writes: state.writes ++ writes}

  defp takeWrites(state), do: {:reply, state.writes, %{state | writes: []}}

  defp serveWaiter(waiter) do
    Enum.reduce_while(waiter.keys, :empty, fn key, _pending ->
      case serve(waiter.op, key) do
        :empty -> {:cont, :empty}
        outcome -> {:halt, outcome}
      end
    end)
  end

  defp serve({:pop, side}, key) do
    case popOne(key, side) do
      {:ok, value} ->
        {:ok, {:array, [{:bulk, key}, {:bulk, value}]}, [], [{popName(side), [key]}]}

      nil ->
        :empty

      {:error, reason} ->
        {:error, errorTerm(reason)}
    end
  end

  defp serve({:mpop, side, count}, key) do
    case popMany(key, side, count) do
      {:ok, [_ | _] = values} ->
        taken = Integer.to_string(length(values))

        {:ok, {:array, [{:bulk, key}, {:array, Reply.bulks(values)}]}, [],
         [{popName(side), [key, taken]}]}

      {:ok, []} ->
        :empty

      nil ->
        :empty

      {:error, reason} ->
        {:error, errorTerm(reason)}
    end
  end

