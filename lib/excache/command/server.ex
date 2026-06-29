defmodule Excache.Command.Server do
  @moduledoc false

  alias Excache.Command.Reply
  alias Excache.Config
  alias Excache.Eviction
  alias Excache.Eviction.Runner
  alias Excache.Eviction.Tracker
  alias Excache.Memory
  alias Excache.Store

  @doctorHeavyKey 25
  @doctorNearLimit 80

  def dispatch("MEMORY", [subcommand | rest], state),
    do: memory(String.upcase(subcommand), rest, state)

  def dispatch("MEMORY", [], state), do: {[wrongArity("memory")], state}

  def dispatch("CONFIG", [subcommand | rest], state),
    do: config(String.upcase(subcommand), rest, state)

  def dispatch("CONFIG", [], state), do: {[wrongArity("config")], state}

  defp memory("USAGE", [key], state), do: {[usage(key)], state}

  defp memory("USAGE", [key, option, count], state) do
    cond do
      String.upcase(option) != "SAMPLES" -> {[Reply.syntax()], state}
      not nonNegativeInteger?(count) -> {[Reply.not_integer()], state}
      true -> {[usage(key)], state}
    end
  end

  defp memory("STATS", [], state), do: {[{:array, statsArray()}], state}

  defp memory("DOCTOR", [], state), do: {[{:bulk, doctor()}], state}

  defp memory(subcommand, _args, state), do: {[unknownSubcommand("MEMORY", subcommand)], state}

  defp config("GET", [_ | _] = patterns, state) do
    values =
      patterns
      |> Enum.flat_map(&Config.getGlob/1)
      |> Enum.uniq_by(fn {name, _value} -> name end)
      |> Enum.sort()
      |> Enum.flat_map(fn {name, value} -> [{:bulk, name}, {:bulk, value}] end)

    {[{:array, values}], state}
  end

  defp config("GET", [], state), do: {[wrongArity("config|get")], state}

  defp config("SET", [_, _ | _] = args, state) do
    if rem(length(args), 2) == 0 do
      {[applySettings(Reply.pairs(args))], state}
    else
      {[wrongArity("config|set")], state}
    end
  end

  defp config("SET", _args, state), do: {[wrongArity("config|set")], state}

  defp config("RESETSTAT", [], state) do
    Runner.resetStats()
    {[{:simple, "OK"}], state}
  end

  defp config(subcommand, _args, state), do: {[unknownSubcommand("CONFIG", subcommand)], state}

  defp applySettings(pairs) do
    case Config.setMany(pairs) do
      :ok -> {:simple, "OK"}
      {:error, name, reason} -> settingError(name, reason)
    end
  end

  defp settingError(name, :unknown),
    do: {:error, "ERR Unknown option or number of arguments for CONFIG SET - '#{name}'"}

  defp settingError(name, :immutable),
    do: {:error, "ERR CONFIG SET failed - can't set immutable config '#{name}'"}

  defp settingError(_name, {:invalid, hint}), do: {:error, "ERR CONFIG SET failed - #{hint}"}

  defp usage(key) do
    case Memory.usage(key) do
      nil -> {:bulk, nil}
      bytes -> {:integer, bytes}
    end
  end

  defp statsArray do
    stats = Memory.stats(Store.dump())
    runner = Runner.stats()

    stats
    |> statsPairs(runner)
    |> Enum.flat_map(fn {name, term} -> [{:bulk, name}, term] end)
  end

  defp statsPairs(stats, runner) do
    [
      {"keys.count", {:integer, stats.keys}},
      {"keys.volatile", {:integer, stats.volatileKeys}},
      {"keys.bytes-per-key", {:integer, stats.bytesPerKey}},
      {"total.allocated", {:integer, stats.usedMemory}},
      {"dataset.bytes", {:integer, stats.datasetBytes}},
      {"overhead.total", {:integer, stats.overheadBytes}},
      {"largest.key", {:bulk, largestName(stats.largestKey)}},
      {"largest.key.bytes", {:integer, largestBytes(stats.largestKey)}},
      {"maxmemory", {:integer, Config.maxmemory()}},
      {"maxmemory.policy", {:bulk, Eviction.policyName(Config.policy())}},
      {"maxmemory.samples", {:integer, Config.maxmemorySamples()}},
      {"evicted.keys", {:integer, runner.evictedKeys}},
      {"oom.rejections", {:integer, runner.oomRejections}},
      {"tracker.keys", {:integer, Tracker.size(Tracker)}}
    ] ++ typePairs(stats.byType)
  end

  defp typePairs(byType) do
    Enum.flat_map(Memory.kinds(), fn kind ->
      usage = Map.fetch!(byType, kind)

      [
        {"type.#{kind}.keys", {:integer, usage.keys}},
        {"type.#{kind}.bytes", {:integer, usage.bytes}}
      ]
    end)
  end

  defp largestName(nil), do: nil
  defp largestName({key, _bytes}), do: key

  defp largestBytes(nil), do: 0
  defp largestBytes({_key, bytes}), do: bytes

  defp doctor do
    entries = Store.dump()
    stats = Memory.stats(entries)

    if stats.keys == 0 do
      "Excache holds no keys, there is nothing to report."
    else
      report = %{
        stats: stats,
        limit: Config.maxmemory(),
        policy: Config.policy(),
        runner: Runner.stats(),
        tracked: Tracker.size(Tracker)
      }

      case findings(report) do
        [] -> headline(report) <> " Nothing looks wrong."
        found -> Enum.join([headline(report) | found], "\n")
      end
    end
  end

  defp headline(%{stats: stats, policy: policy, limit: limit}) do
    limitText = if limit == 0, do: "no limit", else: "#{limit} byte limit"

    "#{stats.keys} keys, #{stats.usedMemory} bytes used, #{limitText}, " <>
      "maxmemory-policy #{Eviction.policyName(policy)}."
  end

  defp findings(report) do
    [
      &noLimit/1,
      &overLimit/1,
      &nearLimit/1,
      &noVolatileKeys/1,
      &dominantKey/1,
      &overheadHeavy/1,
      &untrackedKeys/1,
      &evictionActivity/1,
      &refusedWrites/1
    ]
    |> Enum.map(& &1.(report))
    |> Enum.reject(&is_nil/1)
  end

  defp noLimit(%{limit: 0, stats: stats}) do
    "maxmemory is not set, so nothing bounds the keyspace; it already holds " <>
      "#{stats.usedMemory} bytes and will keep growing until the host runs out."
  end

  defp noLimit(_report), do: nil

  defp overLimit(%{limit: limit, stats: stats, policy: :noeviction}) when limit > 0 do
    if stats.usedMemory > limit do
      "used memory #{stats.usedMemory} is over the #{limit} byte limit and " <>
        "maxmemory-policy is noeviction, so every command that allocates is being refused."
    end
  end

  defp overLimit(%{limit: limit, stats: stats}) when limit > 0 do
    if stats.usedMemory > limit do
      "used memory #{stats.usedMemory} is over the #{limit} byte limit, " <>
        "the next write will free keys."
    end
  end

  defp overLimit(_report), do: nil

  defp nearLimit(%{limit: limit, stats: stats}) when limit > 0 do
    share = percent(stats.usedMemory, limit)

    if share >= @doctorNearLimit and stats.usedMemory <= limit do
      "used memory is #{share} percent of the #{limit} byte limit, eviction is close."
    end
  end

  defp nearLimit(_report), do: nil

  defp noVolatileKeys(%{policy: policy, stats: stats, limit: limit}) when limit > 0 do
    if Eviction.volatile?(policy) and stats.volatileKeys == 0 do
      "maxmemory-policy #{Eviction.policyName(policy)} only evicts keys with a TTL and " <>
        "none of the #{stats.keys} keys has one, so eviction cannot free anything."
    end
  end

  defp noVolatileKeys(_report), do: nil

  defp dominantKey(%{stats: %{largestKey: {key, bytes}} = stats}) do
    share = percent(bytes, stats.usedMemory)

    if share >= @doctorHeavyKey and stats.keys > 1 do
      "key '#{key}' holds #{bytes} bytes, #{share} percent of everything stored."
    end
  end

  defp dominantKey(_report), do: nil

  defp overheadHeavy(%{stats: stats}) do
    share = percent(stats.overheadBytes, stats.usedMemory)

    if share > 50 do
      "per key overhead is #{share} percent of the total at #{stats.bytesPerKey} bytes per key, " <>
        "the keyspace is dominated by very small values."
    end
  end

  defp overheadHeavy(_report), do: nil

  defp untrackedKeys(%{policy: policy, stats: stats, tracked: tracked}) do
    if policy in [:allkeysLru, :allkeysLfu, :volatileLru, :volatileLfu] and tracked < stats.keys do
      "access metadata covers #{tracked} of #{stats.keys} keys, " <>
        "the keys nobody has read yet are the first to go."
    end
  end

  defp untrackedKeys(_report), do: nil

  defp evictionActivity(%{runner: %{evictedKeys: evicted}}) when evicted > 0,
    do: "#{evicted} keys have been evicted since the last CONFIG RESETSTAT."

  defp evictionActivity(_report), do: nil

  defp refusedWrites(%{runner: %{oomRejections: rejected}}) when rejected > 0,
    do: "#{rejected} commands hit the memory limit since the last CONFIG RESETSTAT."

  defp refusedWrites(_report), do: nil

  defp percent(_part, 0), do: 0
  defp percent(part, whole), do: round(part * 100 / whole)

  defp nonNegativeInteger?(raw) do
    match?({value, ""} when value >= 0, Integer.parse(raw))
  end

  defp wrongArity(name),
    do: {:error, "ERR wrong number of arguments for '#{name}' command"}

  defp unknownSubcommand(command, subcommand) do
    {:error,
     "ERR Unknown subcommand or wrong number of arguments for '#{subcommand}'. " <>
       "Try #{command} HELP."}
  end
end
