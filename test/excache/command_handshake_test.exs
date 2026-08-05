defmodule Excache.CommandHandshakeTest do
  @moduledoc """
  Client libraries ask the server what it can do before they send anything else.
  Until the catalog was wired in, `COMMAND` answered with an empty array over the
  wire - a server that claims to support nothing. These tests pin the answer to
  the real dispatch table, over a real socket.
  """

  use Excache.Test.ServerCase, async: false

  test "COMMAND reports the whole dispatch table over the wire", %{conn: conn} do
    {entries, conn} = Client.command(conn, ["COMMAND"])

    assert is_list(entries)
    assert length(entries) == length(Enum.uniq(Excache.Command.known_commands()))

    names =
      entries
      |> Enum.map(fn [name | _rest] -> String.upcase(name) end)
      |> MapSet.new()

    for command <- ["GET", "SET", "BLPOP", "MEMORY", "CONFIG", "SCAN", "XADD"] do
      assert MapSet.member?(names, command), "#{command} missing from COMMAND"
    end

    {count, _conn} = Client.command(conn, ["COMMAND", "COUNT"])
    assert count == length(entries)
  end

  test "COMMAND INFO carries the arity and key positions a router needs", %{conn: conn} do
    {[["get", arity, flags, firstKey, lastKey, step]], conn} =
      Client.command(conn, ["COMMAND", "INFO", "GET"])

    assert arity == 2
    assert firstKey == 1
    assert lastKey == 1
    assert step == 1
    assert is_list(flags)

    {[entry], _conn} = Client.command(conn, ["COMMAND", "INFO", "nosuchcommand"])
    assert entry == nil
  end

  test "COMMAND GETKEYS extracts the keys a command touches", %{conn: conn} do
    assert {["one", "two"], conn} =
             Client.command(conn, ["COMMAND", "GETKEYS", "MSET", "one", "1", "two", "2"])

    assert {["only"], _conn} = Client.command(conn, ["COMMAND", "GETKEYS", "GET", "only"])
  end
end
