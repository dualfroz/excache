defmodule Excache.ZsetTest do
  use Excache.Test.ServerCase, async: false

  test "ZADD, ZSCORE, ZCARD and ZRANK", %{conn: conn} do
    assert {3, conn} =
             Client.command(conn, ["ZADD", "board", "10", "ann", "20", "ben", "5", "cid"])

    assert {0, conn} = Client.command(conn, ["ZADD", "board", "99", "ann"])
    assert {3, conn} = Client.command(conn, ["ZCARD", "board"])

    assert {"99", conn} = Client.command(conn, ["ZSCORE", "board", "ann"])
    assert {nil, conn} = Client.command(conn, ["ZSCORE", "board", "ghost"])

    assert {0, conn} = Client.command(conn, ["ZRANK", "board", "cid"])
    assert {1, conn} = Client.command(conn, ["ZRANK", "board", "ben"])
    assert {2, conn} = Client.command(conn, ["ZRANK", "board", "ann"])
    assert {nil, _conn} = Client.command(conn, ["ZRANK", "board", "ghost"])
  end

  test "equal scores order lexicographically by member", %{conn: conn} do
    assert {3, conn} =
             Client.command(conn, ["ZADD", "tied", "5", "zebra", "5", "apple", "5", "mango"])

    {range, _conn} = Client.command(conn, ["ZRANGE", "tied", "0", "-1"])
    assert range == ["apple", "mango", "zebra"]
  end

  test "ZRANGE by rank with and without WITHSCORES", %{conn: conn} do
    assert {4, conn} =
             Client.command(conn, ["ZADD", "board", "1", "a", "2", "b", "3", "c", "4", "d"])

    {plain, conn} = Client.command(conn, ["ZRANGE", "board", "0", "1"])
    assert plain == ["a", "b"]

    {last_two, conn} = Client.command(conn, ["ZRANGE", "board", "-2", "-1"])
    assert last_two == ["c", "d"]

    {scored, _conn} = Client.command(conn, ["ZRANGE", "board", "0", "-1", "WITHSCORES"])
    assert scored == ["a", "1", "b", "2", "c", "3", "d", "4"]
  end

  test "ZRANGEBYSCORE respects inclusive, exclusive and infinite bounds", %{conn: conn} do
    assert {5, conn} =
             Client.command(conn, [
               "ZADD",
               "prices",
               "10",
               "a",
               "20",
               "b",
               "30",
               "c",
               "40",
               "d",
               "50",
               "e"
             ])

    {inclusive, conn} = Client.command(conn, ["ZRANGEBYSCORE", "prices", "20", "40"])
    assert inclusive == ["b", "c", "d"]

    {exclusive, conn} = Client.command(conn, ["ZRANGEBYSCORE", "prices", "(20", "(40"])
    assert exclusive == ["c"]

    {everything, conn} = Client.command(conn, ["ZRANGEBYSCORE", "prices", "-inf", "+inf"])
    assert everything == ["a", "b", "c", "d", "e"]

    {scored, _conn} = Client.command(conn, ["ZRANGEBYSCORE", "prices", "10", "20", "WITHSCORES"])
    assert scored == ["a", "10", "b", "20"]
  end

  test "ZINCRBY creates, increments and supports floats", %{conn: conn} do
    assert {"5", conn} = Client.command(conn, ["ZINCRBY", "hits", "5", "fresh"])
    assert {"8", conn} = Client.command(conn, ["ZINCRBY", "hits", "3", "fresh"])
    assert {"7.5", _conn} = Client.command(conn, ["ZINCRBY", "hits", "-0.5", "fresh"])
  end

  test "ZREM removes members and deletes the key once empty", %{conn: conn} do
    assert {2, conn} = Client.command(conn, ["ZADD", "temp", "1", "only", "2", "other"])
    assert {1, conn} = Client.command(conn, ["ZREM", "temp", "only"])
    assert {1, conn} = Client.command(conn, ["ZCARD", "temp"])
    assert {1, conn} = Client.command(conn, ["ZREM", "temp", "other"])
    assert {0, conn} = Client.command(conn, ["EXISTS", "temp"])
    assert {0, _conn} = Client.command(conn, ["ZREM", "temp", "other"])
  end

  test "ZADD rejects a non-numeric score", %{conn: conn} do
    assert {{:error, "ERR value is not a valid float"}, _conn} =
             Client.command(conn, ["ZADD", "board", "notanumber", "who"])
  end

  test "WRONGTYPE guard applies to zset commands", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "notazset", "v"])

    assert {{:error, "WRONGTYPE" <> _}, _conn} =
             Client.command(conn, ["ZADD", "notazset", "1", "x"])
  end

  test "EXPIRE and TTL work uniformly on zset keys", %{conn: conn} do
    assert {1, conn} = Client.command(conn, ["ZADD", "expiring", "1", "member"])
    assert {1, conn} = Client.command(conn, ["EXPIRE", "expiring", "100"])
    {ttl, _conn} = Client.command(conn, ["TTL", "expiring"])
    assert ttl > 0 and ttl <= 100
  end

  test "ZADD NX only adds new members and never updates existing scores", %{conn: conn} do
    assert {1, conn} = Client.command(conn, ["ZADD", "nx", "1", "a"])
    assert {0, conn} = Client.command(conn, ["ZADD", "nx", "NX", "99", "a"])
    assert {1, conn} = Client.command(conn, ["ZADD", "nx", "NX", "2", "b"])
    assert {"1", conn} = Client.command(conn, ["ZSCORE", "nx", "a"])
    assert {"2", _conn} = Client.command(conn, ["ZSCORE", "nx", "b"])
  end

  test "ZADD XX only updates existing members and never creates new ones", %{conn: conn} do
    assert {1, conn} = Client.command(conn, ["ZADD", "xx", "1", "a"])
    assert {0, conn} = Client.command(conn, ["ZADD", "xx", "XX", "5", "ghost"])
    assert {0, conn} = Client.command(conn, ["ZADD", "xx", "XX", "50", "a"])
    assert {"50", conn} = Client.command(conn, ["ZSCORE", "xx", "a"])
    assert {nil, _conn} = Client.command(conn, ["ZSCORE", "xx", "ghost"])
  end

  test "ZADD GT and LT only apply score updates in the requested direction", %{conn: conn} do
    assert {1, conn} = Client.command(conn, ["ZADD", "gtlt", "10", "a"])
    assert {0, conn} = Client.command(conn, ["ZADD", "gtlt", "GT", "5", "a"])
    assert {"10", conn} = Client.command(conn, ["ZSCORE", "gtlt", "a"])
    assert {0, conn} = Client.command(conn, ["ZADD", "gtlt", "GT", "20", "a"])
    assert {"20", conn} = Client.command(conn, ["ZSCORE", "gtlt", "a"])
    assert {0, conn} = Client.command(conn, ["ZADD", "gtlt", "LT", "30", "a"])
    assert {"20", conn} = Client.command(conn, ["ZSCORE", "gtlt", "a"])
    assert {0, conn} = Client.command(conn, ["ZADD", "gtlt", "LT", "1", "a"])
    assert {"1", _conn} = Client.command(conn, ["ZSCORE", "gtlt", "a"])
  end

  test "ZADD CH counts changed members instead of only newly added ones", %{conn: conn} do
    assert {1, conn} = Client.command(conn, ["ZADD", "ch", "1", "a"])
    assert {2, conn} = Client.command(conn, ["ZADD", "ch", "CH", "5", "a", "2", "b"])
    assert {0, _conn} = Client.command(conn, ["ZADD", "ch", "5", "a"])
  end

  test "ZADD rejects contradictory flag combinations", %{conn: conn} do
    assert {{:error, "ERR value is not a valid float"}, conn} =
             Client.command(conn, ["ZADD", "bad", "NX", "GT", "1", "a"])

    assert {{:error, "ERR value is not a valid float"}, _conn} =
             Client.command(conn, ["ZADD", "bad", "GT", "LT", "1", "a"])
  end

  test "ZPOPMIN and ZPOPMAX remove and return the edge scores", %{conn: conn} do
    assert {4, conn} =
             Client.command(conn, ["ZADD", "pop", "1", "a", "2", "b", "3", "c", "4", "d"])

    assert {["a", "1"], conn} = Client.command(conn, ["ZPOPMIN", "pop"])
    assert {["d", "4"], conn} = Client.command(conn, ["ZPOPMAX", "pop"])
    assert {["b", "2", "c", "3"], conn} = Client.command(conn, ["ZPOPMIN", "pop", "2"])
    assert {0, conn} = Client.command(conn, ["EXISTS", "pop"])
    assert {[], _conn} = Client.command(conn, ["ZPOPMIN", "pop"])
  end
end
