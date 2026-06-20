defmodule Excache.HashTest do
  use Excache.Test.ServerCase, async: false

  test "hash operations", %{conn: conn} do
    assert {2, conn} = Client.command(conn, ["HSET", "profile", "name", "ann", "age", "30"])
    assert {"ann", conn} = Client.command(conn, ["HGET", "profile", "name"])
    assert {2, conn} = Client.command(conn, ["HLEN", "profile"])

    {keys, conn} = Client.command(conn, ["HKEYS", "profile"])
    assert Enum.sort(keys) == ["age", "name"]

    {all, conn} = Client.command(conn, ["HGETALL", "profile"])
    assert Enum.sort(all) == ["30", "age", "ann", "name"]

    assert {1, conn} = Client.command(conn, ["HDEL", "profile", "age"])
    assert {nil, _conn} = Client.command(conn, ["HGET", "profile", "age"])
  end

  test "HEXISTS reports field presence", %{conn: conn} do
    assert {1, conn} = Client.command(conn, ["HSET", "profile", "name", "ben"])
    assert {1, conn} = Client.command(conn, ["HEXISTS", "profile", "name"])
    assert {0, conn} = Client.command(conn, ["HEXISTS", "profile", "missing"])
    assert {0, _conn} = Client.command(conn, ["HEXISTS", "no-such-key", "name"])
  end

  test "HINCRBY increments a numeric field and creates missing ones", %{conn: conn} do
    assert {5, conn} = Client.command(conn, ["HINCRBY", "counters", "hits", "5"])
    assert {8, conn} = Client.command(conn, ["HINCRBY", "counters", "hits", "3"])
    assert {2, conn} = Client.command(conn, ["HINCRBY", "counters", "misses", "2"])
    assert {-1, conn} = Client.command(conn, ["HINCRBY", "counters", "hits", "-9"])

    assert {1, conn} = Client.command(conn, ["HSET", "wrong", "field", "abc"])

    assert {{:error, "ERR value is not an integer or out of range"}, _conn} =
             Client.command(conn, ["HINCRBY", "wrong", "field", "1"])
  end
end
