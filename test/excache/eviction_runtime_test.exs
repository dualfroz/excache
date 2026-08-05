defmodule Excache.EvictionRuntimeTest do
  use Excache.Test.ServerCase, async: false

  alias Excache.Aof
  alias Excache.Config
  alias Excache.Eviction.Runner
  alias Excache.Eviction.Tracker
  alias Excache.Store

  @payload String.duplicate("x", 1000)
  @oom {:error, "OOM command not allowed when used memory > 'maxmemory'"}

  setup_all do
    Application.put_env(:excache, :eviction_interval, 60_000)
    Process.sleep(250)

    on_exit(fn -> Application.delete_env(:excache, :eviction_interval) end)
    :ok
  end

  setup do
    Application.put_env(:excache, :eviction_recheck, 0)
    Config.reset()
    Tracker.clear(Tracker)
    Runner.resetStats()

    on_exit(fn ->
      Config.reset()
      Application.delete_env(:excache, :eviction_recheck)
    end)

    :ok
  end

  test "allkeys-lru drops the coldest keys when a write crosses maxmemory", %{conn: conn} do
    conn = configure(conn, ["maxmemory-policy", "allkeys-lru", "maxmemory-samples", "64"])
    conn = fill(conn, 1..40)

    {before, conn} = stats(conn)
    assert before["keys.count"] == 40
    assert before["total.allocated"] > 40_000

    Process.sleep(5)
    conn = touch(conn, 1..10)

    limit = div(before["total.allocated"], 2)
    conn = configure(conn, ["maxmemory", Integer.to_string(limit)])

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "trigger", @payload])

    {evicted, conn} = stats(conn)
    freed = evicted["evicted.keys"]

    assert freed >= 15 and freed <= 25
    assert evicted["keys.count"] == 41 - freed
    assert evicted["total.allocated"] < before["total.allocated"]
    assert evicted["total.allocated"] <= limit + 2 * before["keys.bytes-per-key"]
    assert evicted["oom.rejections"] == 0
    assert Store.dbsize() == evicted["keys.count"]
    assert Tracker.size(Tracker) == evicted["keys.count"]

    assert {10, conn} = Client.command(conn, ["EXISTS" | keys(1..10)])
    assert {cold, conn} = Client.command(conn, ["EXISTS" | keys(11..40)])
    assert cold == 30 - freed

    Aof.flush()
    deletes = Aof.path() |> File.read!() |> String.split("$3\r\nDEL\r\n") |> length()
    assert deletes - 1 == freed

    {doctor, _conn} = Client.command(conn, ["MEMORY", "DOCTOR"])
    assert doctor =~ "#{freed} keys have been evicted"
  end

  test "noeviction refuses growing writes but keeps freeing ones", %{conn: conn} do
    conn = fill(conn, 1..30)
    {before, conn} = stats(conn)
    limit = div(before["total.allocated"], 2)
    conn = configure(conn, ["maxmemory", Integer.to_string(limit)])

    assert {@oom, conn} = Client.command(conn, ["SET", "blocked", @payload])
    assert {0, conn} = Client.command(conn, ["EXISTS", "blocked"])
    assert {@oom, conn} = Client.command(conn, ["APPEND", "key:1", "more"])
    assert {@oom, conn} = Client.command(conn, ["RPUSH", "fresh-list", "value"])

    assert {@payload, conn} = Client.command(conn, ["GET", "key:1"])
    assert {30, conn} = Client.command(conn, ["DBSIZE"])

    {blocked, conn} = stats(conn)
    assert blocked["evicted.keys"] == 0
    assert blocked["oom.rejections"] >= 3
    assert blocked["total.allocated"] == before["total.allocated"]

    assert {20, conn} = Client.command(conn, ["DEL" | keys(1..20)])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "allowed", @payload])
    assert {11, conn} = Client.command(conn, ["DBSIZE"])

    {recovered, _conn} = stats(conn)
    assert recovered["total.allocated"] < limit
  end

  test "maxmemory 0 lifts the limit again", %{conn: conn} do
    conn = fill(conn, 1..20)
    {before, conn} = stats(conn)
    conn = configure(conn, ["maxmemory", Integer.to_string(div(before["total.allocated"], 2))])

    assert {@oom, conn} = Client.command(conn, ["SET", "blocked", @payload])

    conn = configure(conn, ["maxmemory", "0"])

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "blocked", @payload])
    assert {21, _conn} = Client.command(conn, ["DBSIZE"])
  end

  test "a volatile policy with no TTL anywhere cannot free a thing", %{conn: conn} do
    conn = configure(conn, ["maxmemory-policy", "volatile-lru", "maxmemory-samples", "64"])
    conn = fill(conn, 1..20)

    {before, conn} = stats(conn)
    limit = div(before["total.allocated"], 2)
    conn = configure(conn, ["maxmemory", Integer.to_string(limit)])

    assert {@oom, conn} = Client.command(conn, ["SET", "blocked", @payload])
    assert {20, conn} = Client.command(conn, ["DBSIZE"])

    conn =
      Enum.reduce(1..15, conn, fn index, acc ->
        {1, acc} = Client.command(acc, ["EXPIRE", "key:#{index}", "600"])
        acc
      end)

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "allowed", @payload])

    {after_, conn} = stats(conn)
    assert after_["evicted.keys"] >= 10
    assert after_["keys.count"] == 21 - after_["evicted.keys"]
    assert {5, _conn} = Client.command(conn, ["EXISTS" | keys(16..20)])
  end

  test "a read refreshes access metadata only while a tracking policy is set", %{conn: conn} do
    conn = fill(conn, 1..2)
    assert Tracker.size(Tracker) == 0

    conn = configure(conn, ["maxmemory-policy", "allkeys-lru"])
    conn = fill(conn, 1..2)
    assert Tracker.size(Tracker) == 2

    Process.sleep(5)
    conn = touch(conn, 1..1)

    now = System.system_time(:millisecond)
    assert Tracker.idleMs(Tracker, "key:1", now) < Tracker.idleMs(Tracker, "key:2", now)

    conn = configure(conn, ["maxmemory-policy", "noeviction"])
    Tracker.clear(Tracker)
    _conn = touch(conn, 1..2)
    assert Tracker.size(Tracker) == 0
  end

  test "a sweep drops metadata for keys that are gone", %{conn: conn} do
    conn = configure(conn, ["maxmemory-policy", "allkeys-lru"])
    conn = fill(conn, 1..10)
    assert Tracker.size(Tracker) == 10

    assert {5, conn} = Client.command(conn, ["DEL" | keys(1..5)])
    assert Tracker.size(Tracker) == 10

    conn = configure(conn, ["maxmemory", "1gb"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "fresh", "value"])

    assert Store.dbsize() == 6
    assert Tracker.size(Tracker) == 6
    {reported, _conn} = stats(conn)
    assert reported["tracker.keys"] == 6
    assert reported["evicted.keys"] == 0
  end

  defp configure(conn, args) do
    {{:simple, "OK"}, conn} = Client.command(conn, ["CONFIG", "SET" | args])
    conn
  end

  defp fill(conn, range) do
    Enum.reduce(range, conn, fn index, acc ->
      {{:simple, "OK"}, acc} = Client.command(acc, ["SET", "key:#{index}", @payload])
      acc
    end)
  end

  defp touch(conn, range) do
    Enum.reduce(range, conn, fn index, acc ->
      {@payload, acc} = Client.command(acc, ["GET", "key:#{index}"])
      acc
    end)
  end

  defp keys(range), do: Enum.map(range, &"key:#{&1}")

  defp stats(conn) do
    {flat, conn} = Client.command(conn, ["MEMORY", "STATS"])
    {flat |> Enum.chunk_every(2) |> Map.new(fn [name, value] -> {name, value} end), conn}
  end
end
