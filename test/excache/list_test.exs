defmodule Excache.ListTest do
  use Excache.Test.ServerCase, async: false

  test "list operations", %{conn: conn} do
    assert {3, conn} = Client.command(conn, ["RPUSH", "queue", "a", "b", "c"])
    assert {4, conn} = Client.command(conn, ["LPUSH", "queue", "z"])
    assert {4, conn} = Client.command(conn, ["LLEN", "queue"])

    {range, conn} = Client.command(conn, ["LRANGE", "queue", "0", "-1"])
    assert range == ["z", "a", "b", "c"]

    assert {"z", conn} = Client.command(conn, ["LPOP", "queue"])
    assert {"c", conn} = Client.command(conn, ["RPOP", "queue"])
    assert {"a", conn} = Client.command(conn, ["LINDEX", "queue", "0"])
    assert {nil, _conn} = Client.command(conn, ["LINDEX", "queue", "9"])
  end

  test "LPOP and RPOP with an explicit count", %{conn: conn} do
    assert {5, conn} = Client.command(conn, ["RPUSH", "letters", "a", "b", "c", "d", "e"])
    assert {["a", "b"], conn} = Client.command(conn, ["LPOP", "letters", "2"])
    assert {["e", "d"], conn} = Client.command(conn, ["RPOP", "letters", "2"])
    assert {1, conn} = Client.command(conn, ["LLEN", "letters"])
    assert {nil, _conn} = Client.command(conn, ["LPOP", "missing-key", "3"])
  end

  test "LPUSH on an existing non-list key returns WRONGTYPE", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "notalist", "v"])
    assert {{:error, "WRONGTYPE" <> _}, _conn} = Client.command(conn, ["LPUSH", "notalist", "x"])
  end

  test "popping a list to empty deletes the key", %{conn: conn} do
    assert {1, conn} = Client.command(conn, ["RPUSH", "single", "only"])
    assert {"only", conn} = Client.command(conn, ["LPOP", "single"])
    assert {0, _conn} = Client.command(conn, ["EXISTS", "single"])
  end
end
