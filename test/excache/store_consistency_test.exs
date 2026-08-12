defmodule Excache.StoreConsistencyTest do
  use Excache.Test.ServerCase, async: false

  alias Excache.Aof
  alias Excache.Config
  alias Excache.Eviction.Tracker
  alias Excache.Store

  @ttlMs 200
  @holdMs 400

  setup do
    Tracker.clear(Tracker)

    on_exit(fn ->
      Config.reset()
      Tracker.clear(Tracker)
    end)

    :ok
  end

  describe "RENAME must not drop the source when the destination write is refused" do
    test "an expired source is reported instead of being silently swallowed" do
      source = "cons:rename:src"
      dest = keyInOtherShard("cons:rename:dst-", source)

      assert :ok = Store.set(source, "payload", px: @ttlMs)

      assert {:error, :nokey} =
               duringSuspendedShard(dest, fn -> Store.rename(source, dest) end)

      assert Store.exists([dest]) == 0
      assert Store.exists([source]) == 0
    end

    test "an existing destination is never reported as overwritten when it was not" do
      source = "cons:rename:live-src"
      dest = keyInOtherShard("cons:rename:live-dst-", source)

      assert :ok = Store.set(dest, "stale", [])
      assert :ok = Store.set(source, "fresh", px: @ttlMs)

      assert {:error, :nokey} =
               duringSuspendedShard(dest, fn -> Store.rename(source, dest) end)

      assert {:ok, "stale"} = Store.get(dest)
    end

    test "a healthy rename still carries the value and the remaining ttl" do
      source = "cons:rename:ok-src"
      dest = keyInOtherShard("cons:rename:ok-dst-", source)

      assert :ok = Store.set(source, "payload", px: 60_000)

      assert {:ok, "OK"} = Store.rename(source, dest)
      assert {:ok, "payload"} = Store.get(dest)
      assert Store.exists([source]) == 0

      remaining = Store.ttl(dest, :seconds)
      assert remaining > 0 and remaining <= 60
    end
  end

  describe "RENAMENX must not drop the source when the destination write is refused" do
    test "an expired source is reported instead of counted as a rename" do
      source = "cons:renamenx:src"
      dest = keyInOtherShard("cons:renamenx:dst-", source)

      assert :ok = Store.set(source, "payload", px: @ttlMs)

      assert {:error, :nokey} =
               duringSuspendedShard(dest, fn -> Store.renamenx(source, dest) end)

      assert Store.exists([dest]) == 0
    end

    test "an occupied destination still refuses the rename and keeps both keys" do
      source = "cons:renamenx:busy-src"
      dest = keyInOtherShard("cons:renamenx:busy-dst-", source)

      assert :ok = Store.set(source, "payload", [])
      assert :ok = Store.set(dest, "taken", [])

      assert {:ok, 0} = Store.renamenx(source, dest)
      assert {:ok, "payload"} = Store.get(source)
      assert {:ok, "taken"} = Store.get(dest)
    end
  end

  describe "COPY must not destroy the destination when the write is refused" do
    test "an expiring source leaves the existing destination intact" do
      source = "cons:copy:src"
      dest = keyInOtherShard("cons:copy:dst-", source)

      assert :ok = Store.set(dest, "keep-me", [])
      assert :ok = Store.set(source, "payload", px: @ttlMs)

      assert {:ok, 0} =
               duringSuspendedShard(dest, fn -> Store.copy(source, dest, true) end)

      assert {:ok, "keep-me"} = Store.get(dest)
    end

    test "replacing a destination of a different type still overwrites it" do
      source = "cons:copy:swap-src"
      dest = keyInOtherShard("cons:copy:swap-dst-", source)

      assert {:ok, 1} = Store.rpush(dest, ["old"])
      assert :ok = Store.set(source, "fresh", [])

      assert {:ok, 1} = Store.copy(source, dest, true)
      assert {:ok, "fresh"} = Store.get(dest)
      assert Store.type(dest) == "string"
    end

    test "copying onto an occupied destination without replace changes nothing" do
      source = "cons:copy:keep-src"
      dest = keyInOtherShard("cons:copy:keep-dst-", source)

      assert :ok = Store.set(source, "fresh", [])
      assert :ok = Store.set(dest, "untouched", [])

      assert {:ok, 0} = Store.copy(source, dest, false)
      assert {:ok, "untouched"} = Store.get(dest)
    end
  end

  describe "SPOP must reach the log as the removal it actually performed" do
    test "the replayed log rebuilds exactly the surviving members", %{conn: conn} do
      key = "cons:pop:members"
      members = Enum.map(1..20, &"member#{&1}")

      assert {20, conn} = Client.command(conn, ["SADD", key | members])

      {taken, conn} = Client.command(conn, ["SPOP", key, "5"])
      assert length(taken) == 5

      {single, conn} = Client.command(conn, ["SPOP", key])
      assert single in members

      {survivors, conn} = Client.command(conn, ["SMEMBERS", key])
      assert length(survivors) == 14

      log = flushedLog()
      refute log =~ "SPOP"
      assert log =~ "SREM"

      conn = replayFromScratch(conn)

      {replayed, _conn} = Client.command(conn, ["SMEMBERS", key])
      assert Enum.sort(replayed) == Enum.sort(survivors)
    end

    test "draining the whole set replays as the key being gone", %{conn: conn} do
      key = "cons:pop:drained"

      assert {3, conn} = Client.command(conn, ["SADD", key, "a", "b", "c"])

      {taken, conn} = Client.command(conn, ["SPOP", key, "3"])
      assert Enum.sort(taken) == ["a", "b", "c"]
      assert {0, conn} = Client.command(conn, ["EXISTS", key])

      log = flushedLog()
      refute log =~ "SPOP"

      conn = replayFromScratch(conn)

      assert {0, _conn} = Client.command(conn, ["EXISTS", key])
    end

    test "popping from a missing set replays as a no-op", %{conn: conn} do
      assert {nil, conn} = Client.command(conn, ["SPOP", "cons:pop:ghost"])
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "cons:pop:marker", "v"])

      flushedLog()
      conn = replayFromScratch(conn)

      assert {0, conn} = Client.command(conn, ["EXISTS", "cons:pop:ghost"])
      assert {"v", _conn} = Client.command(conn, ["GET", "cons:pop:marker"])
    end
  end

  describe "OBJECT IDLETIME must report the tracked idle time" do
    test "a key left alone for five seconds reports five", %{conn: conn} do
      assert :ok = Config.set("maxmemory-policy", "allkeys-lru")

      key = "cons:idle:tracked"
      assert :ok = Store.set(key, "v", [])
      Tracker.touch(Tracker, key, System.system_time(:millisecond) - 5_000)

      assert {5, _conn} = Client.command(conn, ["OBJECT", "IDLETIME", key])
    end

    test "a key without access metadata reports zero rather than an error", %{conn: conn} do
      key = "cons:idle:untracked"
      assert :ok = Store.set(key, "v", [])

      assert {0, _conn} = Client.command(conn, ["OBJECT", "IDLETIME", key])
    end

    test "a missing key is still refused", %{conn: conn} do
      assert {{:error, "ERR no such key"}, _conn} =
               Client.command(conn, ["OBJECT", "IDLETIME", "cons:idle:ghost"])
    end
  end

  defp duringSuspendedShard(shardKey, work) do
    shard = shardPid(shardKey)
    parent = self()

    :sys.suspend(shard)

    worker =
      Task.async(fn ->
        send(parent, :entered)
        work.()
      end)

    queued =
      try do
        assert_receive :entered, 100
        waiting = awaitQueuedCall(shard, worker.pid)
        Process.sleep(@holdMs)
        waiting
      after
        :sys.resume(shard)
      end

    assert queued, "the worker never reached the suspended shard, the expiry window was not open"

    Task.await(worker)
  end

  defp awaitQueuedCall(shard, workerPid) do
    Enum.any?(1..500, fn _attempt ->
      if callPending?(shard, workerPid) do
        true
      else
        Process.sleep(1)
        false
      end
    end)
  end

  defp callPending?(shard, workerPid) do
    {:messages, pending} = Process.info(shard, :messages)
    Enum.any?(pending, &match?({:"$gen_call", {^workerPid, _ref}, _request}, &1))
  end

  defp flushedLog do
    Aof.flush()
    {:ok, log} = File.read(Application.get_env(:excache, :aof_path))
    log
  end

  defp replayFromScratch(conn) do
    Store.flushdb()
    Aof.replay(Application.get_env(:excache, :aof_path))
    conn
  end

  defp shardIndex(key), do: rem(:erlang.phash2(key), Store.shard_count())

  defp shardPid(key), do: Process.whereis(Store.shard_name(shardIndex(key)))

  defp keyInOtherShard(prefix, reference) do
    Enum.find_value(1..1024, fn suffix ->
      candidate = prefix <> Integer.to_string(suffix)
      if shardIndex(candidate) != shardIndex(reference), do: candidate
    end)
  end
end
