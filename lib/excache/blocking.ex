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

  defp serve({:move, fromSide, toSide, destination}, key) do
    case popOne(key, fromSide) do
      {:ok, value} ->
        moveInto(key, destination, fromSide, toSide, value)

      nil ->
        :empty

      {:error, reason} ->
        {:error, errorTerm(reason)}
    end
  end

  defp moveInto(source, destination, fromSide, toSide, value) do
    case pushOne(destination, toSide, value) do
      {:ok, _length} ->
        {:ok, {:bulk, value}, [destination],
         moveWrites(source, destination, fromSide, toSide, value)}

      {:error, reason} ->
        pushOne(source, fromSide, value)
        {:error, errorTerm(reason)}
    end
  end

  defp popOne(key, :left), do: Store.lpop(key, nil)
  defp popOne(key, :right), do: Store.rpop(key, nil)

  defp popMany(key, :left, count), do: Store.lpop(key, count)
  defp popMany(key, :right, count), do: Store.rpop(key, count)

  defp pushOne(key, :left, value), do: Store.lpush(key, [value])
  defp pushOne(key, :right, value), do: Store.rpush(key, [value])

  defp popName(:left), do: "LPOP"
  defp popName(:right), do: "RPOP"

  defp pushName(:left), do: "LPUSH"
  defp pushName(:right), do: "RPUSH"

  defp moveWrites(source, destination, :right, :left, _value),
    do: [{"RPOPLPUSH", [source, destination]}]

  defp moveWrites(source, destination, fromSide, toSide, value),
    do: [{popName(fromSide), [source]}, {pushName(toSide), [destination, value]}]

  defp missReply({:move, _fromSide, _toSide, _destination}), do: {:bulk, nil}
  defp missReply(_op), do: {:array, nil}

  defp errorTerm(:wrongtype), do: Reply.wrongtype()

  defp deliver(served, inlineRef) do
    Enum.reduce(served, nil, fn {waiter, term}, inline ->
      if waiter.ref == inlineRef do
        term
      else
        send(waiter.pid, {:excacheUnblocked, waiter.ref, term})
        inline
      end
    end)
  end

  defp headWaiter(state, key) do
    with {:ok, queue} <- Map.fetch(state.queues, key),
         {:value, ref} <- :queue.peek(queue) do
      Map.fetch!(state.waiters, ref)
    else
      _missing -> nil
    end
  end

  defp enqueue(state, waiter) do
    queues =
      Enum.reduce(Enum.uniq(waiter.keys), state.queues, fn key, acc ->
        Map.put(acc, key, :queue.in(waiter.ref, Map.get(acc, key, :queue.new())))
      end)

    %{state | queues: queues, waiters: Map.put(state.waiters, waiter.ref, waiter)}
  end

  defp dropWaiter(state, waiter) do
    armed = Map.fetch!(state.waiters, waiter.ref)
    if armed.monitor, do: Process.demonitor(armed.monitor, [:flush])
    if armed.timer, do: Process.cancel_timer(armed.timer)

    queues =
      Enum.reduce(Enum.uniq(armed.keys), state.queues, fn key, acc ->
        dropFromQueue(acc, key, armed.ref)
      end)

    %{
      state
      | queues: queues,
        waiters: Map.delete(state.waiters, armed.ref),
        monitors: Map.delete(state.monitors, armed.monitor)
    }
  end

  defp dropFromQueue(queues, key, ref) do
    pruned = :queue.filter(&(&1 != ref), Map.fetch!(queues, key))

    if :queue.is_empty(pruned) do
      Map.delete(queues, key)
    else
      Map.put(queues, key, pruned)
    end
  end

  defp arm(state, waiter, timeout) do
    monitor = Process.monitor(waiter.pid)

    timer =
      if timeout > 0, do: Process.send_after(self(), {:blockingTimeout, waiter.ref}, timeout)

    armed = %{waiter | monitor: monitor, timer: timer}

    %{
      state
      | waiters: Map.put(state.waiters, waiter.ref, armed),
        monitors: Map.put(state.monitors, monitor, waiter.ref)
    }
  end
end
