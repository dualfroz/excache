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

