defmodule Excache.AofTest do
  use Excache.Test.ServerCase, async: false

  test "writes are appended live and replay reconstructs every data type", %{conn: conn} do
    aof_path = Application.get_env(:excache, :aof_path)

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "durable", "v1"])
    assert {1, conn} = Client.command(conn, ["LPUSH", "aof-list", "only"])
    assert {1, conn} = Client.command(conn, ["HSET", "aof-hash", "field", "value"])
    assert {1, conn} = Client.command(conn, ["SADD", "aof-set", "member"])
    assert {1, conn} = Client.command(conn, ["ZADD", "aof-zset", "5", "ranked"])
    assert {1, conn} = Client.command(conn, ["EXPIRE", "durable", "1000"])

    Excache.Aof.flush()
    Excache.Store.flushdb()

    assert {nil, conn} = Client.command(conn, ["GET", "durable"])

    Excache.Aof.replay(aof_path)

    assert {"v1", conn} = Client.command(conn, ["GET", "durable"])
    assert {["only"], conn} = Client.command(conn, ["LRANGE", "aof-list", "0", "-1"])
    assert {"value", conn} = Client.command(conn, ["HGET", "aof-hash", "field"])
    assert {["member"], conn} = Client.command(conn, ["SMEMBERS", "aof-set"])
    assert {"5", conn} = Client.command(conn, ["ZSCORE", "aof-zset", "ranked"])
    {ttl, _conn} = Client.command(conn, ["TTL", "durable"])
    assert ttl > 0
  end

  test "a DEL logged after a SET is replayed in order and wins", %{conn: conn} do
    aof_path = Application.get_env(:excache, :aof_path)

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "temp", "v"])
    assert {1, conn} = Client.command(conn, ["DEL", "temp"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "temp", "final"])

    Excache.Aof.flush()
    Excache.Store.flushdb()
    Excache.Aof.replay(aof_path)

    assert {"final", _conn} = Client.command(conn, ["GET", "temp"])
  end

  test "read-only commands are not appended to the log", %{conn: conn} do
    aof_path = Application.get_env(:excache, :aof_path)

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "readable", "v"])
    assert {"v", _conn} = Client.command(conn, ["GET", "readable"])

    Excache.Aof.flush()
    {:ok, binary} = File.read(aof_path)

    refute binary =~ "GET"
    assert binary =~ "SET"
  end

  test "commands executed inside MULTI/EXEC are logged for replay", %{conn: conn} do
    aof_path = Application.get_env(:excache, :aof_path)

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])
    assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["SET", "txkey", "txvalue"])
    assert {[{:simple, "OK"}], conn} = Client.command(conn, ["EXEC"])

    Excache.Aof.flush()
    Excache.Store.flushdb()
    Excache.Aof.replay(aof_path)

    assert {"txvalue", _conn} = Client.command(conn, ["GET", "txkey"])
  end

  test "a restart after SAVE applies every write exactly once", %{conn: conn} do
    assert {1, conn} = Client.command(conn, ["INCR", "hits"])
    assert {1, conn} = Client.command(conn, ["RPUSH", "queue", "job1"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SAVE"])
    assert {2, conn} = Client.command(conn, ["INCR", "hits"])
    assert {2, conn} = Client.command(conn, ["RPUSH", "queue", "job2"])

    restartFromDisk()

    assert {"2", conn} = Client.command(conn, ["GET", "hits"])
    assert {["job1", "job2"], _conn} = Client.command(conn, ["LRANGE", "queue", "0", "-1"])
  end

  test "a restart after BGSAVE applies every write exactly once", %{conn: conn} do
    snapshotPath = Application.get_env(:excache, :snapshot_path)

    assert {1, conn} = Client.command(conn, ["INCR", "hits"])
    assert {1, conn} = Client.command(conn, ["RPUSH", "queue", "job1"])

    assert {{:simple, "Background saving started"}, conn} =
             Client.command(conn, ["BGSAVE"])

    awaitFile(snapshotPath)
    assert {2, conn} = Client.command(conn, ["INCR", "hits"])
    assert {2, conn} = Client.command(conn, ["RPUSH", "queue", "job2"])

    restartFromDisk()

    assert {"2", conn} = Client.command(conn, ["GET", "hits"])
    assert {["job1", "job2"], _conn} = Client.command(conn, ["LRANGE", "queue", "0", "-1"])
  end

  test "SAVE leaves only later writes in the log", %{conn: conn} do
    aofPath = Application.get_env(:excache, :aof_path)

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "before", "v"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SAVE"])
    assert {{:simple, "OK"}, _conn} = Client.command(conn, ["SET", "after", "v"])

    Excache.Aof.flush()
    {:ok, binary} = File.read(aofPath)

    refute binary =~ "before"
    assert binary =~ "after"
  end

  test "snapshots taken during concurrent writes lose and repeat nothing", %{
    conn: conn,
    port: port
  } do
    stop = :atomics.new(1, [])

    writers =
      for _writer <- 1..8 do
        Task.async(fn ->
          writerConn = Client.connect(port)
          incrementUntil(stop, writerConn, 0)
        end)
      end

    conn =
      Enum.reduce(1..15, conn, fn _step, current ->
        Process.sleep(2)
        {{:simple, "OK"}, current} = Client.command(current, ["SAVE"])
        current
      end)

    :atomics.put(stop, 1, 1)
    increments = writers |> Task.await_many(30_000) |> Enum.sum()
    restartFromDisk()

    assert increments > 0
    assert {total, _conn} = Client.command(conn, ["GET", "contended"])
    assert String.to_integer(total) == increments
  end

  defp incrementUntil(stop, conn, count) do
    if :atomics.get(stop, 1) == 1 do
      Client.close(conn)
      count
    else
      {_value, conn} = Client.command(conn, ["INCR", "contended"])
      incrementUntil(stop, conn, count + 1)
    end
  end

  defp restartFromDisk do
    Excache.Aof.flush()
    Excache.Store.restart()
    Excache.Aof.restart()
    Excache.Aof.replay(Application.get_env(:excache, :aof_path))
  end

  defp awaitFile(path) do
    unless File.exists?(path) do
      Process.sleep(5)
      awaitFile(path)
    end
  end
end
