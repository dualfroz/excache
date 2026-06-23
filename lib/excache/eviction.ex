defmodule Excache.Eviction do
  @moduledoc false

  # Redis style maxmemory policies over the Excache keyspace.
  #
  # Nothing here sorts the whole keyspace. Every pass draws maxmemory-samples
  # random candidates, scores them under the active policy and merges them into
  # a bounded pool that survives between passes, so a strong candidate seen in
  # an earlier pass still wins against a weak fresh sample. The victim is the
  # pool head; the loop repeats until the estimated usage drops under the limit
  # or the candidate set runs dry.
  #
  # Both the clock and the sampling function are injectable, so a run is fully
  # reproducible. The default keyspace adapter reads the whole store once per
  # run and then accounts freed bytes incrementally, the same estimate driven
  # approach Redis uses inside performEvictions.

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

