defmodule Excache.Command.Introspection.Info do
  @moduledoc false

  alias Excache.Aof
  alias Excache.Clients
  alias Excache.Config
  alias Excache.Eviction
  alias Excache.Eviction.Runner
  alias Excache.Memory
  alias Excache.Persistence
  alias Excache.Store

  @redisCompat "7.2.0"

  @sections %{
    "server" => :server,
    "clients" => :clients,
    "memory" => :memory,
    "persistence" => :persistence,
    "stats" => :stats,
    "replication" => :replication,
    "keyspace" => :keyspace,
    "cpu" => :cpu
  }

  @default [:server, :clients, :memory, :persistence, :stats, :replication, :keyspace]
  @everything @default ++ [:cpu]

  @titles %{
    server: "Server",
    clients: "Clients",
    memory: "Memory",
    persistence: "Persistence",
    stats: "Stats",
    replication: "Replication",
    keyspace: "Keyspace",
    cpu: "Cpu"
  }

  @clientSections [:server, :clients, :stats]
  @keyspaceSections [:memory, :keyspace]

  def render(args), do: args |> requested() |> emit()

  defp requested([]), do: @default

  defp requested(args) do
    args
    |> Enum.flat_map(fn arg ->
      case String.downcase(arg) do
        "all" -> @everything
        "everything" -> @everything
        "default" -> @default
        name -> @sections |> Map.get(name) |> List.wrap()
      end
    end)
    |> Enum.uniq()
  end

  defp emit([]), do: ""

  defp emit(sections) do
    context = context(sections)

    sections
    |> Enum.map(fn section -> block(Map.fetch!(@titles, section), lines(section, context)) end)
    |> IO.iodata_to_binary()
  end

  defp block(title, fields) do
    [
      "# ",
      title,
      "\r\n",
      Enum.map(fields, fn {key, value} -> [key, ":", text(value), "\r\n"] end),
      "\r\n"
    ]
  end

  defp text(value) when is_binary(value), do: value
  defp text(value) when is_integer(value), do: Integer.to_string(value)
  defp text(value) when is_atom(value), do: Atom.to_string(value)
  defp text(value) when is_list(value), do: List.to_string(value)

  defp context(sections) do
    %{}
    |> withClients(Enum.any?(sections, &(&1 in @clientSections)))
    |> withKeyspace(Enum.any?(sections, &(&1 in @keyspaceSections)))
  end

  defp withClients(context, false), do: context
  defp withClients(context, true), do: Map.put(context, :clients, Clients.snapshot())

  defp withKeyspace(context, false), do: context

  defp withKeyspace(context, true) do
    entries = Store.dump()
    now = System.system_time(:millisecond)
    remaining = for {_key, _kind, _value, at} <- entries, at != nil and at > now, do: at - now

    Map.merge(context, %{
      memory: Memory.stats(entries),
      volatile: length(remaining),
      avgTtl: averageTtl(remaining)
    })
  end

  defp averageTtl([]), do: 0
  defp averageTtl(remaining), do: div(Enum.sum(remaining), length(remaining))

  defp lines(:server, context) do
    snapshot = context.clients
    uptime = div(System.system_time(:millisecond) - snapshot.startedAt, 1000)

    [
      {"excache_version", version()},
      {"redis_version", @redisCompat},
      {"redis_mode", "standalone"},
      {"os", :erlang.system_info(:system_architecture)},
      {"arch_bits", :erlang.system_info(:wordsize) * 8},
      {"process_id", :os.getpid()},
      {"run_id", snapshot.runId},
      {"tcp_port", tcpPort()},
      {"uptime_in_seconds", uptime},
      {"uptime_in_days", div(uptime, 86_400)},
      {"otp_release", :erlang.system_info(:otp_release)},
      {"erts_version", :erlang.system_info(:version)},
      {"elixir_version", System.version()}
    ]
  end

  defp lines(:clients, context) do
    clients = context.clients.clients

    [
      {"connected_clients", length(clients)},
      {"blocked_clients", Enum.count(clients, fn client -> Clients.blocked?(client.pid) end)},
      {"pubsub_clients",
       Enum.count(clients, fn client -> Clients.subscriptionCount(client.pid) > 0 end)}
    ]
  end

  defp lines(:memory, context) do
    stats = context.memory
    limit = Config.maxmemory()

    [
      {"used_memory", stats.usedMemory},
      {"used_memory_human", humanBytes(stats.usedMemory)},
      {"used_memory_dataset", stats.datasetBytes},
      {"used_memory_overhead", stats.overheadBytes},
      {"used_memory_beam", :erlang.memory(:total)},
      {"used_memory_beam_processes", :erlang.memory(:processes)},
      {"used_memory_beam_binary", :erlang.memory(:binary)},
      {"used_memory_beam_ets", :erlang.memory(:ets)},
      {"maxmemory", limit},
      {"maxmemory_human", humanBytes(limit)},
      {"maxmemory_policy", Eviction.policyName(Config.policy())},
      {"maxmemory_samples", Config.maxmemorySamples()},
      {"mem_allocator", "erts"}
    ]
  end

  defp lines(:persistence, _context) do
    snapshotPath = Persistence.snapshot_path()

    [
      {"loading", 0},
      {"rdb_last_save_time", fileMtime(snapshotPath)},
      {"rdb_snapshot_size", fileSize(snapshotPath)},
      {"aof_enabled", 1},
      {"aof_rewrite_in_progress", 0},
      {"aof_fsync_policy", Aof.fsync_policy()},
      {"aof_current_size", fileSize(Aof.path())}
    ]
  end

  defp lines(:stats, context) do
    snapshot = context.clients
    runner = Runner.stats()

    [
      {"total_connections_received", snapshot.connectionsReceived},
      {"total_net_input_bytes", snapshot.netIn},
      {"total_net_output_bytes", snapshot.netOut},
      {"evicted_keys", runner.evictedKeys},
      {"oom_rejections", runner.oomRejections},
      {"pubsub_channels", Clients.subscribedChannels()},
      {"pubsub_patterns", 0}
    ]
  end

  defp lines(:replication, _context) do
    [
      {"role", "master"},
      {"connected_slaves", 0},
      {"master_failover_state", "no-failover"},
      {"master_repl_offset", 0},
      {"second_repl_offset", -1},
      {"repl_backlog_active", 0},
      {"repl_backlog_size", 0},
      {"repl_backlog_first_byte_offset", 0},
      {"repl_backlog_histlen", 0}
    ]
  end

  defp lines(:keyspace, context) do
    case Store.dbsize() do
      0 ->
        []

      keys ->
        [{"db0", "keys=#{keys},expires=#{context.volatile},avg_ttl=#{context.avgTtl}"}]
    end
  end

  defp lines(:cpu, _context) do
    {runtimeMillis, _sinceLast} = :erlang.statistics(:runtime)
    {wallMillis, _sinceLast} = :erlang.statistics(:wall_clock)

    [
      {"used_cpu_total", seconds(runtimeMillis)},
      {"wall_clock_seconds", seconds(wallMillis)},
      {"schedulers", :erlang.system_info(:schedulers)},
      {"schedulers_online", :erlang.system_info(:schedulers_online)},
      {"run_queue", :erlang.statistics(:total_run_queue_lengths)}
    ]
  end

  defp version, do: :excache |> Application.spec(:vsn) |> List.to_string()

  defp tcpPort do
    {:ok, port} = Config.get("port")
    port || 0
  end

  defp seconds(millis), do: :erlang.float_to_binary(millis / 1000, decimals: 2)

  defp fileSize(path) do
    case File.stat(path) do
      {:ok, stat} -> stat.size
      {:error, _reason} -> 0
    end
  end

  defp fileMtime(path) do
    case File.stat(path, time: :posix) do
      {:ok, stat} -> stat.mtime
      {:error, _reason} -> 0
    end
  end

  defp humanBytes(bytes) when bytes < 1024, do: "#{bytes}B"
  defp humanBytes(bytes) when bytes < 1_048_576, do: scaled(bytes, 1024, "K")
  defp humanBytes(bytes) when bytes < 1_073_741_824, do: scaled(bytes, 1_048_576, "M")
  defp humanBytes(bytes), do: scaled(bytes, 1_073_741_824, "G")

  defp scaled(bytes, divisor, suffix),
    do: :erlang.float_to_binary(bytes / divisor, decimals: 2) <> suffix
end
