defmodule ExcacheTest do
  use Excache.Test.ServerCase, async: false

  test "PING and ECHO", %{conn: conn} do
    assert {{:simple, "PONG"}, conn} = Client.command(conn, ["PING"])
    assert {"hi", conn} = Client.command(conn, ["PING", "hi"])
    assert {"hello world", _conn} = Client.command(conn, ["ECHO", "hello world"])
  end

  test "inline PING command over the socket", %{conn: conn} do
    conn = Client.send_raw(conn, "PING\r\n")
    assert {{:simple, "PONG"}, _conn} = Client.read(conn)
  end

  test "SET, GET, TYPE and missing key", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "greeting", "hello"])
    assert {"hello", conn} = Client.command(conn, ["GET", "greeting"])
    assert {{:simple, "string"}, conn} = Client.command(conn, ["TYPE", "greeting"])
    assert {nil, conn} = Client.command(conn, ["GET", "absent"])
    assert {{:simple, "none"}, _conn} = Client.command(conn, ["TYPE", "absent"])
  end

  test "SET with NX and XX", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "lock", "a", "NX"])
    assert {nil, conn} = Client.command(conn, ["SET", "lock", "b", "NX"])
    assert {"a", conn} = Client.command(conn, ["GET", "lock"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "lock", "c", "XX"])
    assert {nil, _conn} = Client.command(conn, ["SET", "fresh", "z", "XX"])
  end

  test "INCR, DECR, INCRBY and non-integer error", %{conn: conn} do
    assert {1, conn} = Client.command(conn, ["INCR", "counter"])
    assert {3, conn} = Client.command(conn, ["INCRBY", "counter", "2"])
    assert {2, conn} = Client.command(conn, ["DECR", "counter"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "word", "abc"])

    assert {{:error, "ERR value is not an integer or out of range"}, _conn} =
             Client.command(conn, ["INCR", "word"])
  end

  test "APPEND and GETSET", %{conn: conn} do
    assert {5, conn} = Client.command(conn, ["APPEND", "buf", "hello"])
    assert {11, conn} = Client.command(conn, ["APPEND", "buf", " world"])
    assert {"hello world", conn} = Client.command(conn, ["GETSET", "buf", "fresh"])
    assert {"fresh", _conn} = Client.command(conn, ["GET", "buf"])
  end

  test "MSET and MGET", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MSET", "a", "1", "b", "2"])
    assert {["1", "2", nil], _conn} = Client.command(conn, ["MGET", "a", "b", "missing"])
  end

  test "DEL, EXISTS, KEYS, DBSIZE and FLUSHDB", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "user:1", "ann"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "user:2", "ben"])
    assert {2, conn} = Client.command(conn, ["EXISTS", "user:1", "user:2"])
    assert {2, conn} = Client.command(conn, ["DBSIZE"])

    {keys, conn} = Client.command(conn, ["KEYS", "user:*"])
    assert Enum.sort(keys) == ["user:1", "user:2"]

    assert {1, conn} = Client.command(conn, ["DEL", "user:1"])
    assert {0, conn} = Client.command(conn, ["EXISTS", "user:1"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["FLUSHDB"])
    assert {0, _conn} = Client.command(conn, ["DBSIZE"])
  end

  test "EXPIRE, TTL, PTTL and PERSIST", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "ephemeral", "v", "EX", "100"])
    {ttl, conn} = Client.command(conn, ["TTL", "ephemeral"])
    assert ttl > 0 and ttl <= 100

    assert {1, conn} = Client.command(conn, ["PERSIST", "ephemeral"])
    assert {-1, conn} = Client.command(conn, ["TTL", "ephemeral"])

    assert {1, conn} = Client.command(conn, ["EXPIRE", "ephemeral", "50"])
    {pttl, conn} = Client.command(conn, ["PTTL", "ephemeral"])
    assert pttl > 40_000 and pttl <= 50_000

    assert {-2, _conn} = Client.command(conn, ["TTL", "never-existed"])
  end

  test "PX expiry actually removes the key", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "blink", "v", "PX", "40"])
    Process.sleep(120)
    assert {nil, _conn} = Client.command(conn, ["GET", "blink"])
  end

  test "WRONGTYPE guard across data types", %{conn: conn} do
    assert {3, conn} = Client.command(conn, ["RPUSH", "aList", "a", "b", "c"])
    assert {{:error, "WRONGTYPE" <> _}, _conn} = Client.command(conn, ["GET", "aList"])
  end

  test "pub/sub delivers a published message", %{port: port, conn: subscriber} do
    channel = "room:#{System.unique_integer([:positive])}"

    assert {["subscribe", ^channel, 1], subscriber} =
             Client.command(subscriber, ["SUBSCRIBE", channel])

    publisher = Client.connect(port)
    assert {1, _publisher} = Client.command(publisher, ["PUBLISH", channel, "ping"])
    Client.close(publisher)

    assert {["message", ^channel, "ping"], _subscriber} = Client.read(subscriber)
  end

  test "SAVE then restart reloads from disk", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "durable", "kept"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SAVE"])

    Excache.Store.flushdb()
    assert {nil, conn} = Client.command(conn, ["GET", "durable"])

    Excache.Store.restart()
    assert {"kept", _conn} = Client.command(conn, ["GET", "durable"])
  end

  test "partial frames are buffered until complete", %{conn: conn} do
    conn = Client.send_raw(conn, "*3\r\n$3\r\nSET\r\n$4\r\npart")
    conn = Client.send_raw(conn, "\r\n$2\r\nok\r\n")
    assert {{:simple, "OK"}, conn} = Client.read(conn)
    assert {"ok", _conn} = Client.command(conn, ["GET", "part"])
  end
end
