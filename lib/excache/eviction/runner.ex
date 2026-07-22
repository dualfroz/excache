defmodule Excache.Eviction.Runner do
  @moduledoc false

  use GenServer

  alias Excache.Aof
  alias Excache.Config
  alias Excache.Eviction
  alias Excache.Eviction.Tracker
  alias Excache.Memory
  alias Excache.Notify
  alias Excache.Store

  @meter {__MODULE__, :meter}
  @used 1
  @dirty 2
  @checkedAt 3
  @verdict 4
  @slots 4

  @defaultInterval 200
  @defaultRecheck 20

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def admit do
    limit = Config.maxmemory()
    if limit <= 0, do: :ok, else: decide(limit)
  end

  def account(args) do
    if Config.maxmemory() > 0, do: :counters.add(meter(), @dirty, argBytes(args))
    :ok
  end

  def enforce, do: GenServer.call(__MODULE__, :enforce, 30_000)

  def invalidate do
    meter = meter()
    :counters.put(meter, @dirty, 0)
    :counters.put(meter, @checkedAt, 0)
    :ok
  end

  def reloadTracker, do: GenServer.call(__MODULE__, :reloadTracker)

  def stats, do: GenServer.call(__MODULE__, :stats)

  def resetStats, do: GenServer.call(__MODULE__, :resetStats)

  @impl true
  def init(_opts) do
    :persistent_term.put(@meter, :counters.new(@slots, [:write_concurrency]))
    schedule()
    {:ok, %{evictedKeys: 0, oomRejections: 0}}
  end

  @impl true
  def handle_call(:enforce, _from, state) do
    {verdict, state} = sweep(state)
    {:reply, verdict, state}
  end

  @impl true
  def handle_call(:reloadTracker, _from, state) do
    wanted = %{logFactor: Config.lfuLogFactor(), decayTime: Config.lfuDecayTime()}
    if Tracker.settings(Tracker) != wanted, do: restartTracker()
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    {:reply, %{evictedKeys: state.evictedKeys, oomRejections: state.oomRejections}, state}
  end

  @impl true
  def handle_call(:resetStats, _from, state),
    do: {:reply, :ok, %{state | evictedKeys: 0, oomRejections: 0}}

  @impl true
  def handle_info(:tick, state) do
    state = maintain(state)
    schedule()
    {:noreply, state}
  end

  defp maintain(state) do
    cond do
      Config.maxmemory() > 0 ->
        elem(sweep(state), 1)

      Config.tracksAccess?() ->
        pruneMetadata(Store.dbsize(), fn -> Store.keys("*") end)
        state

      true ->
        state
    end
  end

  defp decide(limit) do
    meter = meter()
    checkedAt = :counters.get(meter, @checkedAt)
    estimated = :counters.get(meter, @used) + :counters.get(meter, @dirty)

    cond do
      checkedAt == 0 -> enforce()
      estimated <= limit -> :ok
      recent?(checkedAt) -> cached(meter)
      true -> enforce()
    end
  end

  defp recent?(checkedAt) do
    window = Application.get_env(:excache, :eviction_recheck, @defaultRecheck)
    window > 0 and System.system_time(:millisecond) - checkedAt < window
  end

  defp cached(meter), do: if(:counters.get(meter, @verdict) == 0, do: :ok, else: :oom)

  defp sweep(state) do
    entries = Store.dump()
    pruneMetadata(length(entries), fn -> Enum.map(entries, &elem(&1, 0)) end)

    used = Memory.usedMemory(entries)
    limit = Config.maxmemory()
    policy = Config.policy()

    cond do
      limit <= 0 or used <= limit ->
        record(used, :ok)
        {:ok, state}

      policy == :noeviction ->
        record(used, :oom)
        {:oom, %{state | oomRejections: state.oomRejections + 1}}

      true ->
        evict(entries, policy, limit, state)
    end
  end

  defp evict(entries, policy, limit, state) do
    {status, report} =
      Eviction.run(
        policy: policy,
        maxmemory: limit,
        sampleSize: Config.maxmemorySamples(),
        tracker: Tracker,
        keyspace: keyspace(entries)
      )

    verdict = if status == :ok, do: :ok, else: :oom
    record(report.usedMemory, verdict)

    state = %{
      state
      | evictedKeys: state.evictedKeys + report.evictedCount,
        oomRejections: state.oomRejections + if(verdict == :oom, do: 1, else: 0)
    }

    {verdict, state}
  end

  defp keyspace(entries) do
    %{
      entries: fn -> entries end,
      delete: fn key ->
        deleted = Store.del([key])
        Aof.append("DEL", [key])
        Notify.key_event(key, "evicted")
        deleted
      end
    }
  end

  defp pruneMetadata(keyCount, keysFun) do
    if Config.tracksAccess?() and Tracker.size(Tracker) > keyCount do
      Tracker.retain(Tracker, keysFun.())
    end

    :ok
  end

  defp restartTracker do
    Supervisor.terminate_child(Excache.Supervisor, Tracker)
    Supervisor.delete_child(Excache.Supervisor, Tracker)
    Supervisor.start_child(Excache.Supervisor, Excache.Application.trackerSpec())
  end

  defp record(used, verdict) do
    meter = meter()
    :counters.put(meter, @used, used)
    :counters.put(meter, @dirty, 0)
    :counters.put(meter, @checkedAt, System.system_time(:millisecond))
    :counters.put(meter, @verdict, if(verdict == :ok, do: 0, else: 1))
  end

  defp argBytes(args),
    do: Enum.reduce(args, Memory.entryOverhead(), fn arg, total -> total + byte_size(arg) end)

  defp meter, do: :persistent_term.get(@meter)

  defp schedule do
    interval = Application.get_env(:excache, :eviction_interval, @defaultInterval)
    Process.send_after(self(), :tick, interval)
  end
end
