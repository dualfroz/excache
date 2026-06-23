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
end
