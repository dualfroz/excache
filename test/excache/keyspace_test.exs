defmodule Excache.KeyspaceTest do
  use Excache.Test.ServerCase, async: false

  test "STRLEN, GETRANGE and SETRANGE follow Redis offsets", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "greeting", "Hello World"])
    assert {11, conn} = Client.command(conn, ["STRLEN", "greeting"])
    assert {0, conn} = Client.command(conn, ["STRLEN", "absent"])

    assert {"Hello", conn} = Client.command(conn, ["GETRANGE", "greeting", "0", "4"])
    assert {"World", conn} = Client.command(conn, ["GETRANGE", "greeting", "-5", "-1"])
    assert {"", conn} = Client.command(conn, ["GETRANGE", "greeting", "10", "4"])
    assert {"", conn} = Client.command(conn, ["GETRANGE", "absent", "0", "-1"])

    assert {11, conn} = Client.command(conn, ["SETRANGE", "greeting", "6", "Redis"])
    assert {"Hello Redis", conn} = Client.command(conn, ["GETRANGE", "greeting", "0", "-1"])

    assert {8, conn} = Client.command(conn, ["SETRANGE", "padded", "5", "abc"])
    {padded, conn} = Client.command(conn, ["GET", "padded"])
    assert padded == :binary.copy(<<0>>, 5) <> "abc"

    assert {0, conn} = Client.command(conn, ["SETRANGE", "untouched", "0", ""])
    assert {0, conn} = Client.command(conn, ["EXISTS", "untouched"])

    assert {{:error, "ERR offset is out of range"}, _conn} =
             Client.command(conn, ["SETRANGE", "greeting", "-1", "x"])
  end

  test "SETNX and GETDEL", %{conn: conn} do
    assert {1, conn} = Client.command(conn, ["SETNX", "slot", "first"])
    assert {0, conn} = Client.command(conn, ["SETNX", "slot", "second"])
    assert {"first", conn} = Client.command(conn, ["GET", "slot"])

    assert {"first", conn} = Client.command(conn, ["GETDEL", "slot"])
    assert {0, conn} = Client.command(conn, ["EXISTS", "slot"])
    assert {nil, conn} = Client.command(conn, ["GETDEL", "slot"])

    assert {1, conn} = Client.command(conn, ["RPUSH", "queue", "x"])

    assert {{:error, "WRONGTYPE" <> _}, _conn} = Client.command(conn, ["GETDEL", "queue"])
  end

  test "INCRBYFLOAT accumulates and rejects non numeric values", %{conn: conn} do
    assert {"10.5", conn} = Client.command(conn, ["INCRBYFLOAT", "balance", "10.5"])
    assert {"10.6", conn} = Client.command(conn, ["INCRBYFLOAT", "balance", "0.1"])
    assert {"8", conn} = Client.command(conn, ["INCRBYFLOAT", "balance", "-2.6"])
    assert {"5", conn} = Client.command(conn, ["INCRBYFLOAT", "counter", "5"])

    assert {{:error, "ERR value is not a valid float"}, conn} =
             Client.command(conn, ["INCRBYFLOAT", "balance", "abc"])

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "word", "hello"])

    assert {{:error, "ERR value is not a valid float"}, _conn} =
             Client.command(conn, ["INCRBYFLOAT", "word", "1"])
  end

  test "HSETNX and HMGET", %{conn: conn} do
    assert {1, conn} = Client.command(conn, ["HSETNX", "profile", "name", "ann"])
    assert {0, conn} = Client.command(conn, ["HSETNX", "profile", "name", "ben"])
    assert {"ann", conn} = Client.command(conn, ["HGET", "profile", "name"])
    assert {1, conn} = Client.command(conn, ["HSETNX", "profile", "city", "gdansk"])

    {values, conn} = Client.command(conn, ["HMGET", "profile", "name", "ghost", "city"])
    assert values == ["ann", nil, "gdansk"]

    {absent, conn} = Client.command(conn, ["HMGET", "nohash", "a", "b"])
    assert absent == [nil, nil]

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "plain", "v"])

    assert {{:error, "WRONGTYPE" <> _}, _conn} = Client.command(conn, ["HMGET", "plain", "a"])
  end

  test "SMISMEMBER and SINTERCARD", %{conn: conn} do
    assert {3, conn} = Client.command(conn, ["SADD", "left", "a", "b", "c"])
    assert {3, conn} = Client.command(conn, ["SADD", "right", "b", "c", "d"])

    {flags, conn} = Client.command(conn, ["SMISMEMBER", "left", "a", "zz", "c"])
    assert flags == [1, 0, 1]

    {none, conn} = Client.command(conn, ["SMISMEMBER", "noset", "a"])
    assert none == [0]

    assert {2, conn} = Client.command(conn, ["SINTERCARD", "2", "left", "right"])
    assert {2, conn} = Client.command(conn, ["SINTERCARD", "2", "left", "right", "LIMIT", "0"])
    assert {1, conn} = Client.command(conn, ["SINTERCARD", "2", "left", "right", "LIMIT", "1"])

    assert {{:error, "ERR numkeys should be greater than 0"}, conn} =
             Client.command(conn, ["SINTERCARD", "0", "left"])

    assert {{:error, "ERR Number of keys can't be greater than number of args"}, conn} =
             Client.command(conn, ["SINTERCARD", "3", "left"])

    assert {{:error, "ERR LIMIT can't be negative"}, conn} =
             Client.command(conn, ["SINTERCARD", "1", "left", "LIMIT", "-1"])

    assert {{:error, "ERR syntax error"}, _conn} =
             Client.command(conn, ["SINTERCARD", "1", "left", "BOUND", "1"])
  end

  test "SPOP replies with a bulk without count and an array with count", %{conn: conn} do
    assert {3, conn} = Client.command(conn, ["SADD", "bag", "a", "b", "c"])

    {single, conn} = Client.command(conn, ["SPOP", "bag"])
    assert single in ["a", "b", "c"]

    {batch, conn} = Client.command(conn, ["SPOP", "bag", "5"])
    assert length(batch) == 2
    assert {0, conn} = Client.command(conn, ["EXISTS", "bag"])

    assert {nil, conn} = Client.command(conn, ["SPOP", "bag"])
    assert {[], conn} = Client.command(conn, ["SPOP", "bag", "3"])

    assert {5, conn} = Client.command(conn, ["SADD", "bag", "a", "b", "c", "d", "e"])
    assert {[], conn} = Client.command(conn, ["SPOP", "bag", "0"])
    assert {5, conn} = Client.command(conn, ["SCARD", "bag"])

    assert {{:error, "ERR value is out of range, must be positive"}, _conn} =
             Client.command(conn, ["SPOP", "bag", "-1"])
  end

  test "SRANDMEMBER repeats members for a negative count", %{conn: conn} do
    assert {2, conn} = Client.command(conn, ["SADD", "colors", "red", "green"])

    {single, conn} = Client.command(conn, ["SRANDMEMBER", "colors"])
    assert single in ["red", "green"]

    {repeats, conn} = Client.command(conn, ["SRANDMEMBER", "colors", "-5"])
    assert length(repeats) == 5
    assert Enum.all?(repeats, &(&1 in ["red", "green"]))

    {capped, conn} = Client.command(conn, ["SRANDMEMBER", "colors", "5"])
    assert Enum.sort(capped) == ["green", "red"]

    assert {2, conn} = Client.command(conn, ["SCARD", "colors"])
    assert {nil, conn} = Client.command(conn, ["SRANDMEMBER", "nothing"])
    assert {[], _conn} = Client.command(conn, ["SRANDMEMBER", "nothing", "3"])
  end

  test "SMOVE moves a member and refuses a destination of the wrong type", %{conn: conn} do
    assert {2, conn} = Client.command(conn, ["SADD", "source", "x", "y"])
    assert {1, conn} = Client.command(conn, ["SADD", "target", "z"])

    assert {1, conn} = Client.command(conn, ["SMOVE", "source", "target", "x"])
    assert {0, conn} = Client.command(conn, ["SMOVE", "source", "target", "ghost"])

    {members, conn} = Client.command(conn, ["SMEMBERS", "target"])
    assert Enum.sort(members) == ["x", "z"]

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "notaset", "v"])

    assert {{:error, "WRONGTYPE" <> _}, conn} =
             Client.command(conn, ["SMOVE", "source", "notaset", "y"])

    {left, _conn} = Client.command(conn, ["SMEMBERS", "source"])
    assert left == ["y"]
  end

