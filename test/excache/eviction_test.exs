defmodule Excache.EvictionTest do
  use ExUnit.Case, async: false

  alias Excache.Eviction
  alias Excache.Eviction.Tracker
  alias Excache.Memory
  alias Excache.Store

  @now 1_000_000

  setup do
    Store.flushdb()
    on_exit(&Store.flushdb/0)
    :ok
  end

  test "policy names round trip through the Redis spelling" do
    for policy <- Eviction.policies() do
      assert {:ok, ^policy} = Eviction.parsePolicy(Eviction.policyName(policy))
    end

    assert Enum.sort(Enum.map(Eviction.policies(), &Eviction.policyName/1)) ==
             Enum.sort(~w(
               noeviction allkeys-lru allkeys-lfu allkeys-random
               volatile-lru volatile-lfu volatile-ttl volatile-random
             ))

    assert Eviction.parsePolicy("lru") == :error
    assert Eviction.volatile?(:volatileTtl)
    assert Eviction.allkeys?(:allkeysLfu)
    assert Eviction.random?(:volatileRandom)
    refute Eviction.volatile?(:allkeysLru)
  end

  test "every policy picks a different victim from the same keyspace" do
    opts = [now: @now, metadata: mixedMetadata(), sampleSize: 8, sampler: firstSampler()]
    entries = mixedEntries()

    assert Eviction.selectVictim(entries, [policy: :allkeysLru] ++ opts) == {:ok, "idle-all"}
    assert Eviction.selectVictim(entries, [policy: :allkeysLfu] ++ opts) == {:ok, "cold-all"}
    assert Eviction.selectVictim(entries, [policy: :allkeysRandom] ++ opts) == {:ok, "random-all"}
    assert Eviction.selectVictim(entries, [policy: :volatileLru] ++ opts) == {:ok, "idle-ttl"}
    assert Eviction.selectVictim(entries, [policy: :volatileLfu] ++ opts) == {:ok, "cold-ttl"}
    assert Eviction.selectVictim(entries, [policy: :volatileTtl] ++ opts) == {:ok, "soon-ttl"}

    assert Eviction.selectVictim(entries, [policy: :volatileRandom] ++ opts) ==
             {:ok, "random-ttl"}

    assert Eviction.selectVictim(entries, [policy: :noeviction] ++ opts) ==
             {:error, :noCandidates}

    victims =
      for policy <- Eviction.policies() -- [:noeviction] do
        {:ok, key} = Eviction.selectVictim(entries, [policy: policy] ++ opts)
        key
      end

    assert length(Enum.uniq(victims)) == 7
  end

  test "an untracked key is evicted before any tracked one" do
    entries = stringEntries(["tracked", "unseen"], "payload")
    metadata = %{"tracked" => %{lastAccess: @now - 500_000, counter: 5}}
    opts = [now: @now, metadata: metadata, sampleSize: 8, sampler: firstSampler()]

    assert Eviction.selectVictim(entries, [policy: :allkeysLru] ++ opts) == {:ok, "unseen"}
    assert Eviction.selectVictim(entries, [policy: :allkeysLfu] ++ opts) == {:ok, "unseen"}
  end

  test "noeviction refuses to free anything and reports OOM" do
    entries = stringEntries(~w(k1 k2 k3), "payload")
    {table, keyspace} = fixtureKeyspace(entries)

    assert {:error, report} =
             Eviction.run(policy: :noeviction, maxmemory: 1, keyspace: keyspace, now: @now)

    assert report.reason == :noeviction
    assert report.evicted == []
    assert report.evictedCount == 0
    assert report.freedBytes == 0
    assert report.usedMemory == Memory.usedMemory(entries)
    assert :ets.info(table, :size) == 3
  end

  test "volatile policies leave a keyspace without any TTL untouched" do
    entries = stringEntries(~w(k1 k2 k3), "payload")
    {table, keyspace} = fixtureKeyspace(entries)

    for policy <- [:volatileLru, :volatileLfu, :volatileTtl, :volatileRandom] do
      assert {:error, report} =
               Eviction.run(
                 policy: policy,
                 maxmemory: 1,
                 keyspace: keyspace,
                 now: @now,
                 metadata: %{}
               )

      assert report.reason == :noVolatileKeys
      assert report.evictedCount == 0
      assert report.freedBytes == 0
    end

    assert :ets.info(table, :size) == 3
  end

  test "maxmemory of zero means no limit at all" do
    entries = stringEntries(~w(k1 k2 k3), "payload")
    {table, keyspace} = fixtureKeyspace(entries)

    assert {:ok, report} =
             Eviction.run(policy: :allkeysRandom, maxmemory: 0, keyspace: keyspace, now: @now)

    assert report.reason == :withinLimit
    assert report.evicted == []
    assert :ets.info(table, :size) == 3
  end

  test "the loop stops the moment usage drops under the limit" do
    keys = for index <- 1..10, do: "k#{String.pad_leading("#{index}", 2, "0")}"
    entries = stringEntries(keys, "payload")
    perEntry = Memory.entryBytes(hd(entries))
    {table, keyspace} = fixtureKeyspace(entries)

    assert {:ok, report} =
             Eviction.run(
               policy: :allkeysRandom,
               maxmemory: 7 * perEntry,
               keyspace: keyspace,
               now: @now,
               seed: {2, 4, 8}
             )

    assert report.reason == :evicted
    assert report.evictedCount == 3
    assert report.freedBytes == 3 * perEntry
    assert report.usedMemory == 7 * perEntry
    assert report.passes == 3
    assert :ets.info(table, :size) == 7
    assert Enum.all?(report.evicted, fn key -> :ets.lookup(table, key) == [] end)
  end

  test "the candidate pool carries a strong candidate between passes" do
    entries = stringEntries(~w(k1 k2 k3 k4), "payload")
    perEntry = Memory.entryBytes(hd(entries))
    {_table, keyspace} = fixtureKeyspace(entries)

    metadata = %{
      "k1" => %{lastAccess: @now - 90_000, counter: 9},
      "k2" => %{lastAccess: @now - 80_000, counter: 9},
      "k3" => %{lastAccess: @now - 20_000, counter: 9},
      "k4" => %{lastAccess: @now - 30_000, counter: 9}
    }

    assert {:ok, report} =
             Eviction.run(
               policy: :allkeysLru,
               maxmemory: 2 * perEntry,
               keyspace: keyspace,
               now: @now,
               metadata: metadata,
               sampleSize: 2,
               sampler: windowSampler()
             )

    assert report.evicted == ["k1", "k2"]
  end

  test "sampling scores a window instead of the whole keyspace" do
    entries = stringEntries(~w(k1 k2 k3 k4), "payload")

    metadata = %{
      "k1" => %{lastAccess: @now - 1_000, counter: 9},
      "k2" => %{lastAccess: @now - 2_000, counter: 9},
      "k3" => %{lastAccess: @now - 3_000, counter: 9},
      "k4" => %{lastAccess: @now - 900_000, counter: 9}
    }

    opts = [now: @now, metadata: metadata, sampler: firstSampler()]

    assert Eviction.selectVictim(entries, [policy: :allkeysLru, sampleSize: 1] ++ opts) ==
             {:ok, "k1"}

    assert Eviction.selectVictim(entries, [policy: :allkeysLru, sampleSize: 4] ++ opts) ==
             {:ok, "k4"}
  end

  test "volatile-ttl evicts in expiry order and ignores keys without a TTL" do
    entries = [
      {"far", :string, "payload", @now + 900_000},
      {"immortal", :string, "payload", nil},
      {"near", :string, "payload", @now + 1_000},
      {"mid", :string, "payload", @now + 60_000}
    ]

    perEntry = Memory.entryBytes(hd(entries))
    {table, keyspace} = fixtureKeyspace(entries)

    assert {:ok, report} =
             Eviction.run(
               policy: :volatileTtl,
               maxmemory: 2 * perEntry,
               keyspace: keyspace,
               now: @now,
               sampleSize: 8
             )

    assert report.evicted == ["near", "mid"]
    assert :ets.lookup(table, "immortal") != []
  end

  test "volatile candidates can run out before the limit is reached" do
    entries =
      stringEntries(~w(a1 a2 a3), "payload") ++
        [
          {"t1", :string, "payload", @now + 900_000},
          {"t2", :string, "payload", @now + 900_000}
        ]

    {table, keyspace} = fixtureKeyspace(entries)

    assert {:error, report} =
             Eviction.run(
               policy: :volatileRandom,
               maxmemory: 1,
               keyspace: keyspace,
               now: @now,
               sampleSize: 8
             )

    assert report.reason == :exhausted
    assert Enum.sort(report.evicted) == ["t1", "t2"]
    assert report.usedMemory > 1
    assert :ets.info(table, :size) == 3
  end

  test "allkeys-lfu backed by a live tracker keeps the hot key and forgets the victim" do
    tracker = startTracker(logFactor: 0, decayTime: 0)
    Enum.each(1..50, fn _index -> Tracker.touch(tracker, "hot", @now) end)
    Tracker.touch(tracker, "cold", @now)

    entries = stringEntries(["cold", "hot"], "payload")
    coldBytes = Memory.entryBytes(hd(entries))
    {table, keyspace} = fixtureKeyspace(entries)

    assert {:ok, report} =
             Eviction.run(
               policy: :allkeysLfu,
               maxmemory: Memory.usedMemory(entries) - coldBytes,
               keyspace: keyspace,
               tracker: tracker,
               now: @now,
               sampleSize: 8
             )

    assert report.evicted == ["cold"]
    assert :ets.lookup(table, "hot") != []
    assert Tracker.lookup(tracker, "cold", @now) == nil
    assert Tracker.counter(tracker, "hot", @now) == 54
  end

  test "volatile-lru backed by a live tracker evicts the idle TTL key" do
    tracker = startTracker(logFactor: 0, decayTime: 0)
    Tracker.touch(tracker, "stale", @now - 300_000)
    Tracker.touch(tracker, "fresh", @now - 1_000)
    Tracker.touch(tracker, "idlest", @now - 900_000)

    entries = [
      {"fresh", :string, "payload", @now + 900_000},
      {"stale", :string, "payload", @now + 900_000},
      {"idlest", :string, "payload", nil}
    ]

    assert Eviction.selectVictim(entries,
             policy: :volatileLru,
             tracker: tracker,
             now: @now,
             sampleSize: 8
           ) == {:ok, "stale"}

    assert Eviction.selectVictim(entries,
             policy: :allkeysLru,
             tracker: tracker,
             now: @now,
             sampleSize: 8
           ) == {:ok, "idlest"}
  end

  test "a run against the live store really removes the keys" do
    for index <- 1..20 do
      :ok = Store.set("evict:#{index}", String.duplicate("payload", 20), [])
    end

    used = Memory.usedMemory()
    target = div(used, 2)

    assert {:ok, report} =
             Eviction.run(policy: :allkeysRandom, maxmemory: target, seed: {1, 2, 3})

    assert report.reason == :evicted
    assert report.evictedCount >= 10
    assert Memory.usedMemory() <= target
    assert Memory.usedMemory() == report.usedMemory
    assert Store.dbsize() == 20 - report.evictedCount

    for key <- report.evicted do
      assert Store.get(key) == nil
    end
  end

  test "candidates carry the estimated size of the entry they stand for" do
    entries = [
      {"small", :string, "v", nil},
      {"large", :string, :binary.copy("z", 4096), @now + 1_000}
    ]

    [small, large] = Eviction.candidates(entries, :allkeysLru)

    assert small.bytes == Memory.entryBytes({"small", :string, "v", nil})
    assert large.bytes > 4096
    assert large.expiresAt == @now + 1_000
    assert Eviction.candidates(entries, :volatileLru) |> Enum.map(& &1.key) == ["large"]
    assert Eviction.candidates(entries, :noeviction) == []
  end

  defp mixedEntries do
    [
      {"random-all", :string, "payload", nil},
      {"idle-all", :string, "payload", nil},
      {"cold-all", :string, "payload", nil},
      {"random-ttl", :string, "payload", @now + 900_000},
      {"idle-ttl", :string, "payload", @now + 900_000},
      {"cold-ttl", :string, "payload", @now + 900_000},
      {"soon-ttl", :string, "payload", @now + 1_000},
      {"filler", :string, "payload", nil}
    ]
  end

  defp mixedMetadata do
    %{
      "random-all" => %{lastAccess: @now - 1_000, counter: 200},
      "idle-all" => %{lastAccess: @now - 100_000, counter: 200},
      "cold-all" => %{lastAccess: @now - 1_000, counter: 1},
      "random-ttl" => %{lastAccess: @now - 1_000, counter: 200},
      "idle-ttl" => %{lastAccess: @now - 90_000, counter: 200},
      "cold-ttl" => %{lastAccess: @now - 1_000, counter: 2},
      "soon-ttl" => %{lastAccess: @now - 1_000, counter: 200},
      "filler" => %{lastAccess: @now - 500, counter: 250}
    }
  end

  defp stringEntries(keys, value), do: Enum.map(keys, fn key -> {key, :string, value, nil} end)

  defp fixtureKeyspace(entries) do
    table = :ets.new(:evictionFixture, [:ordered_set, :public])
    Enum.each(entries, &:ets.insert(table, &1))

    {table,
     %{
       entries: fn -> :ets.tab2list(table) end,
       delete: fn key -> :ets.delete(table, key) end
     }}
  end

  defp firstSampler, do: fn pool, count, randState -> {Enum.take(pool, count), randState} end

  defp windowSampler do
    {:ok, cursor} = Agent.start_link(fn -> 0 end)

    fn pool, count, randState ->
      offset = Agent.get_and_update(cursor, fn pass -> {pass, pass + 1} end)
      {pool |> Enum.drop(offset) |> Enum.take(count), randState}
    end
  end

  defp startTracker(opts) do
    id = {Tracker, System.unique_integer([:positive])}
    start_supervised!(Supervisor.child_spec({Tracker, opts}, id: id))
  end
end
