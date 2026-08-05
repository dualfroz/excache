defmodule Excache.Eviction.Tracker do
  @moduledoc false

  # Per key access metadata for the LRU and LFU policies.
  #
  # The LFU counter follows Redis evict.c: it starts at LFU_INIT_VAL, grows
  # logarithmically so a key read a million times cannot bury one read a
  # thousand times, and decays by one unit for every lfu-decay-time minutes a
  # key goes untouched. Both knobs and the random source are injectable so the
  # behaviour is reproducible in tests; every timestamp is epoch milliseconds
  # supplied by the caller, the tracker never reads the clock itself.

  use GenServer

  @lfuInitCounter 5
  @lfuMaxCounter 255
  @defaultLogFactor 10
  @defaultDecayTime 1
  @defaultSeed {1721, 4093, 8677}

  def lfuInitCounter, do: @lfuInitCounter
  def lfuMaxCounter, do: @lfuMaxCounter

  def start_link(opts \\ []) do
    {serverOpts, initOpts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, initOpts, serverOpts)
  end

  def touch(server, key, now), do: GenServer.call(server, {:touch, key, now})

  def touchAsync(server, key, now), do: GenServer.cast(server, {:touch, key, now})

  def touchMany(server, keys, now), do: GenServer.call(server, {:touchMany, keys, now})

  def forget(server, key), do: GenServer.call(server, {:forget, key})

  def retain(server, keys), do: GenServer.call(server, {:retain, keys})

  def clear(server), do: GenServer.call(server, :clear)

  def size(server), do: GenServer.call(server, :size)

  def lookup(server, key, now), do: GenServer.call(server, {:lookup, key, now})

  def metadata(server, keys, now), do: GenServer.call(server, {:metadata, keys, now})

  def snapshot(server, now), do: GenServer.call(server, {:snapshot, now})

  def counter(server, key, now) do
    case lookup(server, key, now) do
      nil -> 0
      meta -> meta.counter
    end
  end

  def idleMs(server, key, now) do
    case lookup(server, key, now) do
      nil -> nil
      meta -> meta.idleMs
    end
  end

  def settings(server), do: GenServer.call(server, :settings)

  @impl true
  def init(opts) do
    {:ok,
     %{
       access: %{},
       logFactor: Keyword.get(opts, :logFactor, @defaultLogFactor),
       decayTime: Keyword.get(opts, :decayTime, @defaultDecayTime),
       randState: :rand.seed_s(:exsss, Keyword.get(opts, :seed, @defaultSeed))
     }}
  end

  @impl true
  def handle_call({:touch, key, now}, _from, state) do
    {counter, state} = register(state, key, now)
    {:reply, counter, state}
  end

  @impl true
  def handle_call({:touchMany, keys, now}, _from, state) do
    {counters, state} =
      Enum.reduce(keys, {%{}, state}, fn key, {acc, current} ->
        {counter, next} = register(current, key, now)
        {Map.put(acc, key, counter), next}
      end)

    {:reply, counters, state}
  end

  @impl true
  def handle_call({:forget, key}, _from, state),
    do: {:reply, :ok, %{state | access: Map.delete(state.access, key)}}

  @impl true
  def handle_call({:retain, keys}, _from, state),
    do: {:reply, :ok, %{state | access: Map.take(state.access, keys)}}

  @impl true
  def handle_call(:clear, _from, state), do: {:reply, :ok, %{state | access: %{}}}

  @impl true
  def handle_call(:size, _from, state), do: {:reply, map_size(state.access), state}

  @impl true
  def handle_call({:lookup, key, now}, _from, state) do
    reply =
      case Map.fetch(state.access, key) do
        :error -> nil
        {:ok, meta} -> present(meta, now, state.decayTime)
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:metadata, keys, now}, _from, state) do
    reply =
      state.access
      |> Map.take(keys)
      |> Map.new(fn {key, meta} -> {key, present(meta, now, state.decayTime)} end)

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:snapshot, now}, _from, state) do
    reply =
      Map.new(state.access, fn {key, meta} -> {key, present(meta, now, state.decayTime)} end)

    {:reply, reply, state}
  end

  @impl true
  def handle_call(:settings, _from, state) do
    {:reply, %{logFactor: state.logFactor, decayTime: state.decayTime}, state}
  end

  @impl true
  def handle_cast({:touch, key, now}, state) do
    {_counter, state} = register(state, key, now)
    {:noreply, state}
  end

  defp register(state, key, now) do
    {counter, randState} =
      case Map.fetch(state.access, key) do
        :error ->
          {@lfuInitCounter, state.randState}

        {:ok, meta} ->
          meta.counter
          |> decay(meta.lastAccess, now, state.decayTime)
          |> logIncrement(state.logFactor, state.randState)
      end

    access = Map.put(state.access, key, %{lastAccess: now, counter: counter})
    {counter, %{state | access: access, randState: randState}}
  end

  defp logIncrement(@lfuMaxCounter, _logFactor, randState), do: {@lfuMaxCounter, randState}

  defp logIncrement(counter, logFactor, randState) do
    {roll, nextState} = :rand.uniform_s(randState)
    baseval = max(counter - @lfuInitCounter, 0)
    probability = 1.0 / (baseval * logFactor + 1)

    if roll < probability, do: {counter + 1, nextState}, else: {counter, nextState}
  end

  defp decay(counter, _lastAccess, _now, decayTime) when decayTime <= 0, do: counter

  defp decay(counter, lastAccess, now, decayTime) do
    periods = div(div(max(now - lastAccess, 0), 60_000), decayTime)
    max(counter - periods, 0)
  end

  defp present(meta, now, decayTime) do
    %{
      lastAccess: meta.lastAccess,
      idleMs: max(now - meta.lastAccess, 0),
      counter: decay(meta.counter, meta.lastAccess, now, decayTime)
    }
  end
end
