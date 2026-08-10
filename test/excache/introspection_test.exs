defmodule Excache.IntrospectionTest do
  use Excache.Test.ServerCase, async: false

  alias Excache.Aof
  alias Excache.Blocking
  alias Excache.Command
  alias Excache.Command.Introspection
  alias Excache.Command.Introspection.Catalog
  alias Excache.Command.Transaction
  alias Excache.Config
  alias Excache.Eviction.Runner
  alias Excache.Memory

  setup do
    Config.reset()
    Runner.resetStats()
    on_exit(&Config.reset/0)
    :ok
  end

  describe "INFO" do
    test "answers with every default section in the documented order", %{conn: conn} do
      assert {text, _conn} = Client.command(conn, ["INFO"])

      assert sections(text) == [
               "Server",
               "Clients",
               "Memory",
               "Persistence",
               "Stats",
               "Replication",
               "Keyspace"
             ]

      assert String.ends_with?(text, "\r\n\r\n")
    end

    test "the server section reports the running system, not placeholders", %{conn: conn} do
      assert {text, _conn} = Client.command(conn, ["INFO", "server"])
      assert sections(text) == ["Server"]

      values = fields(text)

      assert values["redis_mode"] == "standalone"
      assert values["excache_version"] == to_string(Application.spec(:excache, :vsn))
      assert values["run_id"] =~ ~r/^[0-9a-f]{40}$/
      assert values["arch_bits"] == Integer.to_string(:erlang.system_info(:wordsize) * 8)
      assert values["process_id"] == to_string(:os.getpid())
      assert values["elixir_version"] == System.version()
      assert values["otp_release"] == to_string(:erlang.system_info(:otp_release))
      assert String.to_integer(values["uptime_in_seconds"]) >= 0
    end

    test "used_memory matches what the memory accountant reports", %{conn: conn} do
      assert {{:simple, "OK"}, conn} =
               Client.command(conn, ["SET", "solo", String.duplicate("payload", 40)])

      assert {text, _conn} = Client.command(conn, ["INFO", "memory"])
      values = fields(text)

      assert sections(text) == ["Memory"]
      assert String.to_integer(values["used_memory"]) == Memory.usedMemory()
      assert values["maxmemory"] == "0"
      assert values["maxmemory_policy"] == "noeviction"
      assert values["used_memory_human"] =~ ~r/^[0-9.]+[BKMG]$/

      dataset = String.to_integer(values["used_memory_dataset"])
      overhead = String.to_integer(values["used_memory_overhead"])
      assert dataset + overhead == String.to_integer(values["used_memory"])
    end

    test "the keyspace section counts live keys, volatile keys and the mean ttl", %{conn: conn} do
      assert {text, conn} = Client.command(conn, ["INFO", "keyspace"])
      refute String.contains?(text, "db0")

      assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "plain", "one"])
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "other", "two"])
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "timed", "three"])
      assert {1, conn} = Client.command(conn, ["EXPIRE", "timed", "120"])

      assert {text, _conn} = Client.command(conn, ["INFO", "keyspace"])
      assert %{"db0" => summary} = fields(text)

      assert [keys, expires, avgTtl] =
               Regex.run(~r/keys=(\d+),expires=(\d+),avg_ttl=(\d+)/, summary,
                 capture: :all_but_first
               )

      assert keys == "3"
      assert expires == "1"
      assert String.to_integer(avgTtl) > 118_000
      assert String.to_integer(avgTtl) <= 120_000
    end

    test "the persistence section reads the real append only file", %{conn: conn} do
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "durable", "value"])
      :ok = Aof.flush()

      assert {text, _conn} = Client.command(conn, ["INFO", "persistence"])
      values = fields(text)

      assert values["aof_enabled"] == "1"
      assert values["aof_fsync_policy"] == "everysec"
      assert values["loading"] == "0"

      size = String.to_integer(values["aof_current_size"])
      assert size > 0
      assert size == File.stat!(Aof.path()).size
    end

    test "the stats section counts connections and eviction refusals", %{conn: conn, port: port} do
      assert {text, conn} = Client.command(conn, ["INFO", "stats"])
      before = String.to_integer(fields(text)["total_connections_received"])

      extra = Client.connect(port)
      assert {{:simple, "PONG"}, _extra} = Client.command(extra, ["PING"])

      assert {text, conn} = Client.command(conn, ["INFO", "stats"])
      values = fields(text)

      assert String.to_integer(values["total_connections_received"]) == before + 1
      assert String.to_integer(values["total_net_input_bytes"]) > 0
      assert values["evicted_keys"] == "0"
      assert values["oom_rejections"] == "0"

      assert {{:simple, "OK"}, conn} =
               Client.command(conn, ["SET", "filler", String.duplicate("x", 128)])

      assert {{:simple, "OK"}, conn} = Client.command(conn, ["CONFIG", "SET", "maxmemory", "1"])
      assert {{:error, "OOM" <> _}, conn} = Client.command(conn, ["SET", "toobig", "value"])
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["CONFIG", "SET", "maxmemory", "0"])

      assert {text, _conn} = Client.command(conn, ["INFO", "stats"])
      assert String.to_integer(fields(text)["oom_rejections"]) > 0

      Client.close(extra)
    end

    test "the replication section tells the truth about being a lone master", %{conn: conn} do
      assert {text, _conn} = Client.command(conn, ["INFO", "replication"])
      values = fields(text)

      assert values["role"] == "master"
      assert values["connected_slaves"] == "0"
      assert values["master_failover_state"] == "no-failover"
      assert values["repl_backlog_active"] == "0"
    end

    test "INFO all and INFO everything add the cpu section", %{conn: conn} do
      assert {plain, conn} = Client.command(conn, ["INFO"])
      refute "Cpu" in sections(plain)

      assert {all, conn} = Client.command(conn, ["INFO", "all"])
      assert "Cpu" in sections(all)

      assert {everything, conn} = Client.command(conn, ["INFO", "everything"])
      assert sections(everything) == sections(all)

      values = fields(all)
      assert String.to_integer(values["schedulers"]) == :erlang.system_info(:schedulers)
      assert values["used_cpu_total"] =~ ~r/^\d+\.\d\d$/

      assert {defaults, _conn} = Client.command(conn, ["INFO", "default"])
      assert sections(defaults) == sections(plain)
    end

    test "several sections can be asked for at once and unknown ones are dropped", %{conn: conn} do
      assert {text, conn} = Client.command(conn, ["INFO", "replication", "clients"])
      assert sections(text) == ["Replication", "Clients"]

      assert {"", conn} = Client.command(conn, ["INFO", "nosuchsection"])
      assert {text, _conn} = Client.command(conn, ["INFO", "clients", "nosuchsection"])
      assert sections(text) == ["Clients"]
    end

    test "blocked_clients counts a connection parked in BLPOP", %{conn: conn, port: port} do
      assert clientField(conn, "blocked_clients") == 0

      waiter = blockingTask(port, ["BLPOP", "parked", "0"])
      awaitUntil(fn -> Blocking.waitingCount("parked") == 1 end)

      assert clientField(conn, "blocked_clients") == 1

      assert {1, conn} = Client.command(conn, ["LPUSH", "parked", "wake"])
      assert Task.await(waiter) == ["parked", "wake"]
      awaitUntil(fn -> clientField(conn, "blocked_clients") == 0 end)
    end

    test "pubsub_clients and pubsub_channels follow real subscriptions", %{
      conn: conn,
      port: port
    } do
      assert clientField(conn, "pubsub_clients") == 0

      listener = Client.connect(port)

      assert {["subscribe", "news", 1], _listener} =
               Client.command(listener, ["SUBSCRIBE", "news"])

      awaitUntil(fn -> clientField(conn, "pubsub_clients") == 1 end)

      assert {text, _conn} = Client.command(conn, ["INFO", "stats"])
      assert fields(text)["pubsub_channels"] == "1"
      assert fields(text)["pubsub_patterns"] == "0"

      Client.close(listener)
    end
  end

  describe "CLIENT" do
    test "ID hands out a distinct rising number per connection", %{conn: conn, port: port} do
      assert {mine, conn} = Client.command(conn, ["CLIENT", "ID"])
      assert is_integer(mine) and mine > 0

      other = Client.connect(port)
      assert {theirs, _other} = Client.command(other, ["CLIENT", "ID"])
      assert theirs > mine

      assert {^mine, _conn} = Client.command(conn, ["CLIENT", "ID"])
      Client.close(other)
    end

    test "SETNAME and GETNAME round trip and reject illegal names", %{conn: conn} do
      assert {"", conn} = Client.command(conn, ["CLIENT", "GETNAME"])
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["CLIENT", "SETNAME", "importer"])
      assert {"importer", conn} = Client.command(conn, ["CLIENT", "GETNAME"])

      assert {{:error, "ERR Client names cannot contain spaces" <> _}, conn} =
               Client.command(conn, ["CLIENT", "SETNAME", "two words"])

      assert {"importer", conn} = Client.command(conn, ["CLIENT", "GETNAME"])
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["CLIENT", "SETNAME", ""])
      assert {"", _conn} = Client.command(conn, ["CLIENT", "GETNAME"])
    end

    test "LIST shows one line per connection with the real address and name", %{
      conn: conn,
      port: port
    } do
      assert {mine, conn} = Client.command(conn, ["CLIENT", "ID"])

      other = Client.connect(port)
      assert {theirs, other} = Client.command(other, ["CLIENT", "ID"])
      assert {{:simple, "OK"}, _other} = Client.command(other, ["CLIENT", "SETNAME", "worker"])

      assert {text, conn} = Client.command(conn, ["CLIENT", "LIST"])
      lines = listLines(text)

      assert %{"name" => "worker", "addr" => address} = lines[theirs]
      assert address =~ ~r/^127\.0\.0\.1:\d+$/
      assert lines[theirs]["laddr"] =~ ~r/^127\.0\.0\.1:#{port}$/
      assert lines[mine]["name"] == ""
      assert lines[mine]["db"] == "0"
      assert String.to_integer(lines[mine]["tot-net-in"]) > 0

      assert {filtered, conn} =
               Client.command(conn, ["CLIENT", "LIST", "ID", Integer.to_string(theirs)])

      assert Map.keys(listLines(filtered)) == [theirs]

      assert {{:error, "ERR syntax error"}, _conn} =
               Client.command(conn, ["CLIENT", "LIST", "NOPE", "1"])

      Client.close(other)
    end

    test "LIST TYPE separates subscribers from plain connections", %{conn: conn, port: port} do
      assert {mine, conn} = Client.command(conn, ["CLIENT", "ID"])

      listener = Client.connect(port)
      assert {theirs, listener} = Client.command(listener, ["CLIENT", "ID"])

      assert {["subscribe", "feed", 1], _listener} =
               Client.command(listener, ["SUBSCRIBE", "feed"])

      awaitUntil(fn ->
        {text, _conn} = Client.command(conn, ["CLIENT", "LIST", "TYPE", "pubsub"])
        Map.has_key?(listLines(text), theirs)
      end)

      assert {text, conn} = Client.command(conn, ["CLIENT", "LIST", "TYPE", "pubsub"])
      assert Map.keys(listLines(text)) == [theirs]
      assert listLines(text)[theirs]["sub"] == "1"

      assert {text, conn} = Client.command(conn, ["CLIENT", "LIST", "TYPE", "normal"])
      assert Map.has_key?(listLines(text), mine)
      refute Map.has_key?(listLines(text), theirs)

      assert {"", conn} = Client.command(conn, ["CLIENT", "LIST", "TYPE", "replica"])

      assert {{:error, "ERR syntax error"}, _conn} =
               Client.command(conn, ["CLIENT", "LIST", "TYPE", "nonsense"])

      Client.close(listener)
    end

    test "INFO describes the calling connection only", %{conn: conn} do
      assert {mine, conn} = Client.command(conn, ["CLIENT", "ID"])
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["CLIENT", "SETNAME", "solo"])
      assert {text, _conn} = Client.command(conn, ["CLIENT", "INFO"])

      assert [{id, fieldMap}] = Map.to_list(listLines(text <> "\n"))
      assert id == mine
      assert fieldMap["name"] == "solo"
    end

    test "idle grows while a connection stays quiet and age tracks the connect time", %{
      conn: conn,
      port: port
    } do
      quiet = Client.connect(port)
      assert {quietId, _quiet} = Client.command(quiet, ["CLIENT", "ID"])

      Process.sleep(1_200)

      assert {mine, conn} = Client.command(conn, ["CLIENT", "ID"])
      assert {text, _conn} = Client.command(conn, ["CLIENT", "LIST"])
      lines = listLines(text)

      assert String.to_integer(lines[quietId]["idle"]) >= 1
      assert String.to_integer(lines[quietId]["age"]) >= 1
      assert lines[mine]["idle"] == "0"

      Client.close(quiet)
    end

    test "KILL by ID tears down another live connection", %{conn: conn, port: port} do
      victim = Client.connect(port)
      assert {victimId, victim} = Client.command(victim, ["CLIENT", "ID"])
      assert {{:simple, "PONG"}, victim} = Client.command(victim, ["PING"])

      assert {1, conn} =
               Client.command(conn, ["CLIENT", "KILL", "ID", Integer.to_string(victimId)])

      assert {:error, :closed} = :gen_tcp.recv(victim.socket, 0, 1_000)

      assert {0, conn} =
               Client.command(conn, ["CLIENT", "KILL", "ID", Integer.to_string(victimId)])

      assert {text, _conn} = Client.command(conn, ["CLIENT", "LIST"])
      refute Map.has_key?(listLines(text), victimId)
    end

    test "KILL by address accepts the legacy single argument form", %{conn: conn, port: port} do
      victim = Client.connect(port)
      assert {victimId, victim} = Client.command(victim, ["CLIENT", "ID"])

      assert {text, conn} = Client.command(conn, ["CLIENT", "LIST"])
      address = listLines(text)[victimId]["addr"]

      assert {{:simple, "OK"}, conn} = Client.command(conn, ["CLIENT", "KILL", address])
      assert {:error, :closed} = :gen_tcp.recv(victim.socket, 0, 1_000)

      assert {{:error, "ERR No such client address in the client list"}, _conn} =
               Client.command(conn, ["CLIENT", "KILL", address])
    end

    test "KILL skips the caller unless SKIPME no is given", %{conn: conn, port: port} do
      assert {mine, conn} = Client.command(conn, ["CLIENT", "ID"])
      bystander = Client.connect(port)
      assert {_id, _bystander} = Client.command(bystander, ["CLIENT", "ID"])

      assert {0, conn} = Client.command(conn, ["CLIENT", "KILL", "ID", Integer.to_string(mine)])
      assert {{:simple, "PONG"}, conn} = Client.command(conn, ["PING"])

      assert {1, conn} =
               Client.command(conn, [
                 "CLIENT",
                 "KILL",
                 "ID",
                 Integer.to_string(mine),
                 "SKIPME",
                 "no"
               ])

      assert {:error, :closed} = :gen_tcp.recv(conn.socket, 0, 1_000)
      Client.close(bystander)
    end

    test "KILL rejects filters it does not understand", %{conn: conn} do
      assert {{:error, "ERR syntax error"}, conn} =
               Client.command(conn, ["CLIENT", "KILL", "WHATEVER", "1"])

      assert {{:error, "ERR syntax error"}, conn} =
               Client.command(conn, ["CLIENT", "KILL", "ID", "notanumber"])

      assert {{:error, "ERR Unknown subcommand" <> _}, _conn} =
               Client.command(conn, ["CLIENT", "NOSUCHTHING"])
    end

    test "HELP lists the supported subcommands", %{conn: conn} do
      assert {lines, _conn} = Client.command(conn, ["CLIENT", "HELP"])
      assert {:simple, "KILL <ip:port>"} in lines
      assert {:simple, "ID"} in lines
    end

    test "CLIENT off a real connection reports that it needs one" do
      assert {[{:error, "ERR CLIENT is only available on a server connection"}], _state} =
               Introspection.dispatch("CLIENT", ["ID"], commandState())
    end
  end

  describe "COMMAND" do
    test "the catalog covers exactly the commands the server dispatches" do
      known = MapSet.new(Command.known_commands())
      assert MapSet.new(Catalog.tableNames()) == known
    end

    test "the full reply carries one six element entry per command" do
      assert {[{:array, entries}], _state} = Introspection.dispatch("COMMAND", [], commandState())

      assert length(entries) == length(Enum.uniq(Command.known_commands()))

      Enum.each(entries, fn {:array, entry} ->
        assert [{:bulk, name}, {:integer, arity}, {:array, flags}, first, last, step] = entry
        assert name == String.downcase(name)
        assert arity != 0
        assert Enum.all?(flags, &match?({:simple, _flag}, &1))
        assert match?({:integer, _value}, first)
        assert match?({:integer, _value}, last)
        assert match?({:integer, _value}, step)
      end)
    end

    test "COUNT and LIST agree with the full reply" do
      assert {[{:integer, count}], _state} =
               Introspection.dispatch("COMMAND", ["COUNT"], commandState())

      assert {[{:array, names}], _state} =
               Introspection.dispatch("COMMAND", ["LIST"], commandState())

      assert count == length(names)
      assert count == length(Enum.uniq(Command.known_commands()))
      assert {:bulk, "append"} in names
    end

    test "INFO gives the real arity and key positions per command" do
      assert commandInfo("GET") == {"get", 2, ["readonly", "fast"], 1, 1, 1}
      assert commandInfo("SET") == {"set", -3, ["write", "denyoom"], 1, 1, 1}
      assert commandInfo("MSET") == {"mset", -3, ["write", "denyoom"], 1, -1, 2}
      assert commandInfo("DEL") == {"del", -2, ["write"], 1, -1, 1}
      assert commandInfo("BLPOP") == {"blpop", -3, ["write", "noscript", "blocking"], 1, -2, 1}
      assert commandInfo("BITOP") == {"bitop", -4, ["write", "denyoom"], 2, -1, 1}
      assert commandInfo("SMOVE") == {"smove", 4, ["write", "fast"], 1, 2, 1}
      assert commandInfo("PING") == {"ping", -1, ["fast"], 0, 0, 0}

      assert {"xread", -4, flags, 0, 0, 0} = commandInfo("XREAD")
      assert "movablekeys" in flags
    end

    test "INFO is case insensitive and answers nil for what it does not have" do
      assert {[{:array, [entry, nil?]}], _state} =
               Introspection.dispatch("COMMAND", ["INFO", "get", "nosuchcommand"], commandState())

      assert {:array, [{:bulk, "get"} | _rest]} = entry
      assert nil? == {:array, nil}
    end

    test "the declared arity matches what the dispatcher actually accepts", %{conn: conn} do
      assert {{:error, "ERR wrong number of arguments for 'get' command"}, conn} =
               Client.command(conn, ["GET", "a", "b"])

      assert {{:error, "ERR wrong number of arguments for 'smove' command"}, conn} =
               Client.command(conn, ["SMOVE", "a", "b"])

      assert {{:error, "ERR wrong number of arguments for 'linsert' command"}, _conn} =
               Client.command(conn, ["LINSERT", "a", "BEFORE", "b"])
    end

    test "GETKEYS walks plain ranges" do
      assert getkeys(["GET", "one"]) == ["one"]
      assert getkeys(["MSET", "a", "1", "b", "2", "c", "3"]) == ["a", "b", "c"]
      assert getkeys(["DEL", "a", "b", "c"]) == ["a", "b", "c"]
      assert getkeys(["BLPOP", "left", "right", "0"]) == ["left", "right"]
      assert getkeys(["BITOP", "AND", "dest", "src1", "src2"]) == ["dest", "src1", "src2"]
      assert getkeys(["SMOVE", "from", "to", "member"]) == ["from", "to"]
      assert getkeys(["EXPIRE", "solo", "10"]) == ["solo"]
    end

    test "GETKEYS resolves the commands whose keys move" do
      assert getkeys(["XREAD", "COUNT", "2", "STREAMS", "s1", "s2", "0", "0"]) == ["s1", "s2"]

      assert getkeys([
               "XREADGROUP",
               "GROUP",
               "g",
               "c",
               "STREAMS",
               "stream",
               ">"
             ]) == ["stream"]

      assert getkeys(["SINTERCARD", "2", "a", "b", "LIMIT", "1"]) == ["a", "b"]
      assert getkeys(["BLMPOP", "0", "2", "a", "b", "LEFT"]) == ["a", "b"]
      assert getkeys(["OBJECT", "ENCODING", "thing"]) == ["thing"]
      assert getkeys(["MEMORY", "USAGE", "thing"]) == ["thing"]
      assert getkeys(["XINFO", "STREAM", "events"]) == ["events"]
      assert getkeys(["XGROUP", "CREATE", "events", "group", "$"]) == ["events"]
    end

    test "GETKEYS refuses what it cannot answer" do
      assert getkeys(["PING"]) == {:error, "ERR The command has no key arguments"}
      assert getkeys(["MEMORY", "DOCTOR"]) == {:error, "ERR The command has no key arguments"}
      assert getkeys(["NOSUCHCOMMAND", "a"]) == {:error, "ERR Invalid command specified"}

      assert getkeys(["GET"]) ==
               {:error, "ERR Invalid number of arguments specified for command"}

      assert getkeys(["XREAD", "COUNT", "2", "s1", "0"]) ==
               {:error, "ERR Invalid arguments specified for command"}

      assert getkeys(["SINTERCARD", "3", "a", "b"]) ==
               {:error, "ERR Invalid arguments specified for command"}
    end

    test "DOCS answers with the group and arity taken from the same table" do
      assert {[{:array, entries}], _state} =
               Introspection.dispatch(
                 "COMMAND",
                 ["DOCS", "zadd", "nosuchcommand"],
                 commandState()
               )

      assert entries == [
               {:bulk, "zadd"},
               {:array, [{:bulk, "group"}, {:bulk, "zset"}, {:bulk, "arity"}, {:integer, -4}]}
             ]

      assert {[{:array, everything}], _state} =
               Introspection.dispatch("COMMAND", ["DOCS"], commandState())

      assert length(everything) == 2 * length(Enum.uniq(Command.known_commands()))
    end

    test "an unknown subcommand is named in the error" do
      assert {[{:error, message}], _state} =
               Introspection.dispatch("COMMAND", ["NOPE"], commandState())

      assert message =~ "Unknown subcommand or wrong number of arguments for 'NOPE'"
    end

    test "the extended dispatcher already routes COMMAND to this module" do
      assert {[{:integer, count}], _state} =
               Excache.Command.Extended.dispatch("COMMAND", ["COUNT"], commandState())

      assert count == Catalog.count()
    end
  end

  defp commandState,
    do: Map.put(Transaction.initial_state(), :subscriptions, MapSet.new())

  defp commandInfo(name) do
    assert {[{:array, [{:array, entry}]}], _state} =
             Introspection.dispatch("COMMAND", ["INFO", name], commandState())

    [
      {:bulk, lowered},
      {:integer, arity},
      {:array, flags},
      {:integer, first},
      {:integer, last},
      {:integer, step}
    ] = entry

    {lowered, arity, Enum.map(flags, fn {:simple, flag} -> flag end), first, last, step}
  end

  defp getkeys(argv) do
    case Introspection.dispatch("COMMAND", ["GETKEYS" | argv], commandState()) do
      {[{:array, keys}], _state} -> Enum.map(keys, fn {:bulk, key} -> key end)
      {[error], _state} -> error
    end
  end

  defp clientField(conn, name) do
    {text, _conn} = Client.command(conn, ["INFO", "clients"])
    String.to_integer(fields(text)[name])
  end

  defp sections(text) do
    for "# " <> title <- String.split(text, "\r\n"), do: title
  end

  defp fields(text) do
    text
    |> String.split("\r\n")
    |> Enum.reject(fn line -> line == "" or String.starts_with?(line, "#") end)
    |> Map.new(fn line ->
      [key, value] = String.split(line, ":", parts: 2)
      {key, value}
    end)
  end

  defp listLines(text) do
    text
    |> String.split("\n")
    |> Enum.reject(&(&1 == ""))
    |> Map.new(fn line ->
      parsed =
        Map.new(String.split(line, " "), fn pair ->
          [key, value] = String.split(pair, "=", parts: 2)
          {key, value}
        end)

      {String.to_integer(parsed["id"]), parsed}
    end)
  end

  defp blockingTask(port, args) do
    Task.async(fn ->
      client = Client.connect(port)
      {reply, _client} = Client.command(client, args)
      Client.close(client)
      reply
    end)
  end

  defp awaitUntil(check, attempts \\ 200) do
    cond do
      check.() -> :ok
      attempts == 0 -> flunk("condition never held")
      true -> Process.sleep(10) && awaitUntil(check, attempts - 1)
    end
  end
end
