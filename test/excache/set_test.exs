defmodule Excache.SetTest do
  use Excache.Test.ServerCase, async: false

  test "set operations", %{conn: conn} do
    assert {3, conn} = Client.command(conn, ["SADD", "tags", "x", "y", "z"])
    assert {0, conn} = Client.command(conn, ["SADD", "tags", "x"])
    assert {3, conn} = Client.command(conn, ["SCARD", "tags"])
    assert {1, conn} = Client.command(conn, ["SISMEMBER", "tags", "y"])
    assert {0, conn} = Client.command(conn, ["SISMEMBER", "tags", "nope"])

    {members, conn} = Client.command(conn, ["SMEMBERS", "tags"])
    assert Enum.sort(members) == ["x", "y", "z"]

    assert {1, _conn} = Client.command(conn, ["SREM", "tags", "y"])
  end

  test "SINTER, SUNION and SDIFF across multiple sets", %{conn: conn} do
    assert {3, conn} = Client.command(conn, ["SADD", "a", "1", "2", "3"])
    assert {3, conn} = Client.command(conn, ["SADD", "b", "2", "3", "4"])

    {inter, conn} = Client.command(conn, ["SINTER", "a", "b"])
    assert Enum.sort(inter) == ["2", "3"]

    {union, conn} = Client.command(conn, ["SUNION", "a", "b"])
    assert Enum.sort(union) == ["1", "2", "3", "4"]

    {diff, conn} = Client.command(conn, ["SDIFF", "a", "b"])
    assert diff == ["1"]

    {diff_missing, conn} = Client.command(conn, ["SDIFF", "a", "missing-set"])
    assert Enum.sort(diff_missing) == ["1", "2", "3"]

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "notaset", "v"])
    assert {{:error, "WRONGTYPE" <> _}, _conn} = Client.command(conn, ["SINTER", "a", "notaset"])
  end

  test "SINTERSTORE, SUNIONSTORE and SDIFFSTORE persist the computed set", %{conn: conn} do
    assert {3, conn} = Client.command(conn, ["SADD", "src-a", "1", "2", "3"])
    assert {3, conn} = Client.command(conn, ["SADD", "src-b", "2", "3", "4"])

    assert {2, conn} = Client.command(conn, ["SINTERSTORE", "dest-inter", "src-a", "src-b"])
    {inter, conn} = Client.command(conn, ["SMEMBERS", "dest-inter"])
    assert Enum.sort(inter) == ["2", "3"]

    assert {4, conn} = Client.command(conn, ["SUNIONSTORE", "dest-union", "src-a", "src-b"])
    {union, conn} = Client.command(conn, ["SMEMBERS", "dest-union"])
    assert Enum.sort(union) == ["1", "2", "3", "4"]

    assert {1, conn} = Client.command(conn, ["SDIFFSTORE", "dest-diff", "src-a", "src-b"])
    assert {["1"], conn} = Client.command(conn, ["SMEMBERS", "dest-diff"])

    assert {0, conn} = Client.command(conn, ["SDIFFSTORE", "dest-empty", "src-a", "src-a"])
    assert {0, _conn} = Client.command(conn, ["EXISTS", "dest-empty"])
  end

  test "SINTERSTORE overwrites any prior value at the destination key", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "dest", "stale"])
    assert {2, conn} = Client.command(conn, ["SADD", "src-a", "x", "y"])
    assert {1, conn} = Client.command(conn, ["SADD", "src-b", "x"])
    assert {1, conn} = Client.command(conn, ["SINTERSTORE", "dest", "src-a", "src-b"])
    assert {["x"], _conn} = Client.command(conn, ["SMEMBERS", "dest"])
  end
end
