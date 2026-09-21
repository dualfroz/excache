defmodule Excache.Eviction do
  @moduledoc false

  alias Excache.Eviction.Tracker
  alias Excache.Memory
  alias Excache.Store

  @policies [
    :noeviction,
    :allkeysLru,
    :allkeysLfu,
    :allkeysRandom,
    :volatileLru,
    :volatileLfu,
    :volatileTtl,
    :volatileRandom
  ]

  @policyNames %{
    noeviction: "noeviction",
    allkeysLru: "allkeys-lru",
    allkeysLfu: "allkeys-lfu",
    allkeysRandom: "allkeys-random",
    volatileLru: "volatile-lru",
    volatileLfu: "volatile-lfu",
    volatileTtl: "volatile-ttl",
    volatileRandom: "volatile-random"
  }

  @volatilePolicies [:volatileLru, :volatileLfu, :volatileTtl, :volatileRandom]
  @allkeysPolicies [:allkeysLru, :allkeysLfu, :allkeysRandom]
  @randomPolicies [:allkeysRandom, :volatileRandom]
  @lruPolicies [:allkeysLru, :volatileLru]
  @lfuPolicies [:allkeysLfu, :volatileLfu]

  @defaultSampleSize 5
  @defaultPoolSize 16
  @defaultSeed {5, 9, 27}
  @lfuMaxCounter 255

  def policies, do: @policies

  def policyName(policy), do: Map.fetch!(@policyNames, policy)

  def parsePolicy(name) when is_binary(name) do
    case Enum.find(@policies, fn policy -> @policyNames[policy] == name end) do
      nil -> :error
      policy -> {:ok, policy}
    end
  end

  def volatile?(policy), do: policy in @volatilePolicies
  def allkeys?(policy), do: policy in @allkeysPolicies
  def random?(policy), do: policy in @randomPolicies

  def defaultSampleSize, do: @defaultSampleSize
  def defaultPoolSize, do: @defaultPoolSize

  def storeKeyspace do
    %{entries: &Store.dump/0, delete: fn key -> Store.del([key]) end}
  end

  def candidates(entries, policy) do
    entries
    |> Enum.map(fn {key, kind, value, expiresAt} ->
      %{
        key: key,
        kind: kind,
        expiresAt: expiresAt,
        bytes: Memory.estimateEntry(key, {kind, value, expiresAt})
      }
    end)
    |> Enum.filter(&eligible?(&1, policy))
  end

  def selectVictim(entries, opts) do
    config = configure(opts)
    pool = candidates(entries, config.policy)

    case pool do
      [] ->
        {:error, emptyReason(config.policy)}

      _ ->
        {victim, _pool, _config} = pickVictim(pool, [], config)
        {:ok, victim.key}
    end
  end

  def run(opts) do
    config = configure(opts)
    keyspace = Keyword.get_lazy(opts, :keyspace, &storeKeyspace/0)
    entries = keyspace.entries.()
    pool = candidates(entries, config.policy)

    acc = %{used: Memory.usedMemory(entries), evicted: [], freed: 0, passes: 0}

    cond do
      config.maxmemory <= 0 or acc.used <= config.maxmemory ->
        {:ok, report(config, acc, :withinLimit)}

      config.policy == :noeviction ->
        {:error, report(config, acc, :noeviction)}

      pool == [] ->
        {:error, report(config, acc, emptyReason(config.policy))}

      true ->
        evictLoop(pool, [], config, acc, keyspace)
    end
  end

  defp evictLoop(remaining, pool, config, acc, keyspace) do
    cond do
      acc.used <= config.maxmemory ->
        {:ok, report(config, acc, :evicted)}

      remaining == [] ->
        {:error, report(config, acc, :exhausted)}

      true ->
        {victim, carried, config} = pickVictim(remaining, pool, config)
        keyspace.delete.(victim.key)
        if config.tracker, do: Tracker.forget(config.tracker, victim.key)

        acc = %{
          acc
          | used: acc.used - victim.bytes,
            freed: acc.freed + victim.bytes,
            evicted: [victim.key | acc.evicted],
            passes: acc.passes + 1
        }

        nextRemaining = Enum.reject(remaining, &(&1.key == victim.key))
        nextPool = Enum.reject(carried, fn {_score, held} -> held.key == victim.key end)
        evictLoop(nextRemaining, nextPool, config, acc, keyspace)
    end
  end

  defp pickVictim(remaining, pool, config) do
    {sampled, randState} = config.sampler.(remaining, config.sampleSize, config.randState)
    config = %{config | randState: randState}

    if random?(config.policy) do
      {hd(sampled), pool, config}
    else
      metadata = config.metaFun.(Enum.map(sampled, & &1.key), config.now)

      scored =
        Enum.map(sampled, fn candidate ->
          {score(config.policy, candidate, Map.get(metadata, candidate.key), config.now),
           candidate}
        end)

      [{_score, victim} | rest] = mergePool(pool, scored, config.poolSize)
      {victim, rest, config}
    end
  end

  defp mergePool(pool, scored, poolSize) do
    (scored ++ pool)
    |> Enum.uniq_by(fn {_score, candidate} -> candidate.key end)
    |> Enum.sort_by(fn {candidateScore, _candidate} -> candidateScore end, :desc)
    |> Enum.take(poolSize)
  end

  defp score(policy, _candidate, meta, now) when policy in @lruPolicies, do: lruScore(meta, now)

  defp score(policy, _candidate, meta, _now) when policy in @lfuPolicies,
    do: @lfuMaxCounter - counterOf(meta)

  defp score(:volatileTtl, candidate, _meta, _now), do: -candidate.expiresAt

  defp lruScore(nil, _now), do: {1, 0}
  defp lruScore(meta, now), do: {0, max(now - meta.lastAccess, 0)}

  defp counterOf(nil), do: 0
  defp counterOf(meta), do: meta.counter

  defp eligible?(_candidate, :noeviction), do: false

  defp eligible?(candidate, policy) do
    if volatile?(policy), do: candidate.expiresAt != nil, else: true
  end

  defp emptyReason(policy) do
    if volatile?(policy), do: :noVolatileKeys, else: :noCandidates
  end

  defp configure(opts) do
    %{
      policy: Keyword.fetch!(opts, :policy),
      maxmemory: Keyword.get(opts, :maxmemory, 0),
      now: Keyword.get_lazy(opts, :now, fn -> System.system_time(:millisecond) end),
      sampleSize: Keyword.get(opts, :sampleSize, @defaultSampleSize),
      poolSize: Keyword.get(opts, :poolSize, @defaultPoolSize),
      sampler: Keyword.get(opts, :sampler, &sampleRandom/3),
      randState: :rand.seed_s(:exsss, Keyword.get(opts, :seed, @defaultSeed)),
      tracker: Keyword.get(opts, :tracker),
      metaFun: metaFun(opts)
    }
  end

  defp metaFun(opts) do
    cond do
      Keyword.has_key?(opts, :metadata) ->
        metadata = Keyword.fetch!(opts, :metadata)
        fn keys, _now -> Map.take(metadata, keys) end

      Keyword.has_key?(opts, :tracker) ->
        tracker = Keyword.fetch!(opts, :tracker)
        fn keys, now -> Tracker.metadata(tracker, keys, now) end

      true ->
        fn _keys, _now -> %{} end
    end
  end

  defp sampleRandom(pool, count, randState) when length(pool) <= count, do: {pool, randState}

  defp sampleRandom(pool, count, randState) do
    {taken, _rest, _size, nextState} =
      Enum.reduce(1..count, {[], pool, length(pool), randState}, fn _index,
                                                                    {acc, left, size, state} ->
        {index, drawn} = :rand.uniform_s(size, state)
        {picked, rest} = List.pop_at(left, index - 1)
        {[picked | acc], rest, size - 1, drawn}
      end)

    {Enum.reverse(taken), nextState}
  end

  defp report(config, acc, reason) do
    %{
      reason: reason,
      policy: config.policy,
      maxmemory: config.maxmemory,
      usedMemory: acc.used,
      freedBytes: acc.freed,
      evicted: Enum.reverse(acc.evicted),
      evictedCount: length(acc.evicted),
      passes: acc.passes
    }
  end
end
