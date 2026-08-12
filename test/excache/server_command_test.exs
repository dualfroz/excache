defmodule Excache.ServerCommandTest do
  use Excache.Test.ServerCase, async: false

  alias Excache.Config
  alias Excache.Eviction.Runner

  setup do
    Config.reset()
    Runner.resetStats()
    on_exit(&Config.reset/0)
    :ok
  end

  test "CONFIG GET answers with the live values and follows the pattern", %{conn: conn} do
    assert {["maxmemory", "0"], conn} = Client.command(conn, ["CONFIG", "GET", "maxmemory"])

    assert {values, conn} = Client.command(conn, ["CONFIG", "GET", "maxmemory*"])

    assert values == [
             "maxmemory",
             "0",
             "maxmemory-policy",
             "noeviction",
             "maxmemory-samples",
             "5"
           ]

    assert {[], conn} = Client.command(conn, ["CONFIG", "GET", "nothing-like-this"])

    assert {merged, conn} = Client.command(conn, ["CONFIG", "GET", "lfu-log-factor", "maxmemory"])
    assert merged == ["lfu-log-factor", "10", "maxmemory", "0"]

    assert {deduped, conn} = Client.command(conn, ["CONFIG", "GET", "maxmemory", "maxmemory"])
    assert deduped == ["maxmemory", "0"]

    assert {{:error, "ERR wrong number of arguments for 'config|get' command"}, _conn} =
             Client.command(conn, ["CONFIG", "GET"])
  end

  test "CONFIG SET stores what CONFIG GET reads back", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["CONFIG", "SET", "maxmemory", "1mb"])
    assert {["maxmemory", "1048576"], conn} = Client.command(conn, ["CONFIG", "GET", "maxmemory"])
    assert Config.maxmemory() == 1_048_576

    assert {{:simple, "OK"}, conn} =
             Client.command(conn, ["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])

    assert {["maxmemory-policy", "allkeys-lru"], conn} =
             Client.command(conn, ["CONFIG", "GET", "maxmemory-policy"])

    assert Config.policy() == :allkeysLru

    assert {{:simple, "OK"}, conn} =
             Client.command(conn, [
               "CONFIG",
               "SET",
               "maxmemory",
               "0",
               "maxmemory-samples",
               "16"
             ])

    assert {["maxmemory", "0", "maxmemory-samples", "16"], _conn} =
             Client.command(conn, ["CONFIG", "GET", "maxmemory", "maxmemory-samples"])
  end

  test "CONFIG SET reports every way it can be wrong", %{conn: conn} do
    assert {{:error, "ERR CONFIG SET failed - argument must be a memory value" <> _}, conn} =
             Client.command(conn, ["CONFIG", "SET", "maxmemory", "plenty"])

    assert {{:error, "ERR CONFIG SET failed - argument must be one of the following:" <> _}, conn} =
             Client.command(conn, ["CONFIG", "SET", "maxmemory-policy", "lru"])

    assert {{:error, "ERR Unknown option or number of arguments for CONFIG SET - 'nosuch'"}, conn} =
             Client.command(conn, ["CONFIG", "SET", "nosuch", "1"])

    assert {{:error, "ERR CONFIG SET failed - can't set immutable config 'port'"}, conn} =
             Client.command(conn, ["CONFIG", "SET", "port", "6380"])

    assert {{:error, "ERR wrong number of arguments for 'config|set' command"}, conn} =
             Client.command(conn, ["CONFIG", "SET", "maxmemory"])

    assert {{:error, "ERR wrong number of arguments for 'config|set' command"}, conn} =
             Client.command(conn, ["CONFIG", "SET", "maxmemory", "1kb", "lfu-log-factor"])

    assert {{:error, "ERR Unknown subcommand" <> _}, conn} =
             Client.command(conn, ["CONFIG", "REWRITE"])

    assert {["maxmemory", "0"], _conn} = Client.command(conn, ["CONFIG", "GET", "maxmemory"])
  end

  test "a rejected pair leaves the whole CONFIG SET unapplied", %{conn: conn} do
    assert {{:error, _reason}, conn} =
             Client.command(conn, [
               "CONFIG",
               "SET",
               "maxmemory",
               "2mb",
               "maxmemory-policy",
               "nope"
             ])

    assert {["maxmemory", "0", "maxmemory-policy", "noeviction"], _conn} =
             Client.command(conn, ["CONFIG", "GET", "maxmemory", "maxmemory-policy"])
  end

  test "MEMORY USAGE reports the real cost of a key", %{conn: conn} do
    {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "small", "x"])
    {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "large", String.duplicate("x", 4096)])

    assert {small, conn} = Client.command(conn, ["MEMORY", "USAGE", "small"])
    assert {large, conn} = Client.command(conn, ["MEMORY", "USAGE", "large"])

    assert small > 0
    assert large > small + 4000
    assert large == Excache.Memory.usage("large")

    assert {nil, conn} = Client.command(conn, ["MEMORY", "USAGE", "ghost"])
    assert {^large, conn} = Client.command(conn, ["MEMORY", "USAGE", "large", "SAMPLES", "5"])

    assert {{:error, "ERR value is not an integer or out of range"}, conn} =
             Client.command(conn, ["MEMORY", "USAGE", "large", "SAMPLES", "many"])

    assert {{:error, "ERR syntax error"}, conn} =
             Client.command(conn, ["MEMORY", "USAGE", "large", "COUNT", "5"])

    assert {{:error, "ERR Unknown subcommand" <> _}, _conn} =
             Client.command(conn, ["MEMORY", "USAGE"])
  end

  test "MEMORY STATS adds up to the same numbers the store reports", %{conn: conn} do
    {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "plain", String.duplicate("a", 200)])
    {2, conn} = Client.command(conn, ["RPUSH", "queue", "one", "two"])
    {1, conn} = Client.command(conn, ["SADD", "tags", "red"])
    {1, conn} = Client.command(conn, ["EXPIRE", "plain", "600"])

    {stats, conn} = stats(conn)

    assert stats["keys.count"] == 3
    assert stats["keys.volatile"] == 1
    assert stats["type.string.keys"] == 1
    assert stats["type.list.keys"] == 1
    assert stats["type.set.keys"] == 1
    assert stats["type.hash.keys"] == 0
    assert stats["maxmemory"] == 0
    assert stats["maxmemory.policy"] == "noeviction"
    assert stats["maxmemory.samples"] == 5
    assert stats["evicted.keys"] == 0
    assert stats["oom.rejections"] == 0
    assert stats["largest.key"] == "plain"

    assert stats["total.allocated"] ==
             stats["type.string.bytes"] + stats["type.list.bytes"] + stats["type.set.bytes"]

    assert stats["total.allocated"] == Excache.Memory.usedMemory()
    assert stats["dataset.bytes"] + stats["overhead.total"] == stats["total.allocated"]
    assert stats["keys.bytes-per-key"] == div(stats["total.allocated"], 3)
    assert stats["largest.key.bytes"] == Excache.Memory.usage("plain")

    {_, conn} = Client.command(conn, ["CONFIG", "SET", "maxmemory", "4mb"])
    {updated, _conn} = stats(conn)
    assert updated["maxmemory"] == 4_194_304
  end

  test "MEMORY DOCTOR describes the store it is actually looking at", %{conn: conn} do
    assert {"Excache holds no keys, there is nothing to report.", conn} =
             Client.command(conn, ["MEMORY", "DOCTOR"])

    {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "hog", String.duplicate("z", 20_000)])

    conn =
      Enum.reduce(1..5, conn, fn index, acc ->
        {{:simple, "OK"}, acc} = Client.command(acc, ["SET", "tiny:#{index}", "v"])
        acc
      end)

    {report, conn} = Client.command(conn, ["MEMORY", "DOCTOR"])

    assert report =~ "6 keys"
    assert report =~ "no limit"
    assert report =~ "maxmemory-policy noeviction"
    assert report =~ "maxmemory is not set"
    assert report =~ "key 'hog' holds"
    assert report =~ "percent of everything stored"

    {_, conn} = Client.command(conn, ["CONFIG", "SET", "maxmemory-policy", "volatile-ttl"])
    {_, conn} = Client.command(conn, ["CONFIG", "SET", "maxmemory", "1mb"])
    {volatile, conn} = Client.command(conn, ["MEMORY", "DOCTOR"])

    assert volatile =~ "1048576 byte limit"
    assert volatile =~ "only evicts keys with a TTL and none of the 6 keys has one"
    refute volatile =~ "maxmemory is not set"

    {1, conn} = Client.command(conn, ["EXPIRE", "hog", "600"])
    {withTtl, _conn} = Client.command(conn, ["MEMORY", "DOCTOR"])
    refute withTtl =~ "only evicts keys with a TTL"
  end

  test "MEMORY and CONFIG are known commands inside transactions", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])

    assert {{:simple, "QUEUED"}, conn} =
             Client.command(conn, ["CONFIG", "SET", "maxmemory", "2kb"])

    assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["CONFIG", "GET", "maxmemory"])
    assert {[{:simple, "OK"}, ["maxmemory", "2048"]], _conn} = Client.command(conn, ["EXEC"])
    assert Config.maxmemory() == 2048
  end

  defp stats(conn) do
    {flat, conn} = Client.command(conn, ["MEMORY", "STATS"])
    {flat |> Enum.chunk_every(2) |> Map.new(fn [name, value] -> {name, value} end), conn}
  end
end
