defmodule Excache.TransactionTest do
  use Excache.Test.ServerCase, async: false

  test "MULTI queues commands and EXEC applies them in order", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])
    assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["SET", "a", "1"])
    assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["INCR", "a"])
    assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["GET", "a"])

    assert {[{:simple, "OK"}, 2, "2"], conn} = Client.command(conn, ["EXEC"])
    assert {"2", _conn} = Client.command(conn, ["GET", "a"])
  end

  test "DISCARD drops queued commands without applying them", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])
    assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["SET", "gone", "v"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["DISCARD"])
    assert {nil, _conn} = Client.command(conn, ["GET", "gone"])
  end

  test "EXEC without a prior MULTI is an error", %{conn: conn} do
    assert {{:error, "ERR EXEC without MULTI"}, _conn} = Client.command(conn, ["EXEC"])
  end

  test "DISCARD without a prior MULTI is an error", %{conn: conn} do
    assert {{:error, "ERR DISCARD without MULTI"}, _conn} = Client.command(conn, ["DISCARD"])
  end

  test "nested MULTI is rejected", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])
    assert {{:error, "ERR MULTI calls can not be nested"}, conn} = Client.command(conn, ["MULTI"])
    assert {{:simple, "OK"}, _conn} = Client.command(conn, ["DISCARD"])
  end

  test "an unknown command queued inside MULTI aborts the whole transaction", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])
    assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["SET", "kept", "no"])

    assert {{:error, "ERR unknown command 'NOTACOMMAND'"}, conn} =
             Client.command(conn, ["NOTACOMMAND"])

    assert {{:error, "ERR EXECABORT" <> _}, conn} = Client.command(conn, ["EXEC"])
    assert {nil, _conn} = Client.command(conn, ["GET", "kept"])
  end

  test "WATCH aborts EXEC when the watched key changes before it runs", %{
    port: port,
    conn: conn
  } do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "balance", "100"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["WATCH", "balance"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])
    assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["SET", "balance", "0"])

    interloper = Client.connect(port)
    assert {{:simple, "OK"}, _interloper} = Client.command(interloper, ["SET", "balance", "50"])
    Client.close(interloper)

    assert {nil, conn} = Client.command(conn, ["EXEC"])
    assert {"50", _conn} = Client.command(conn, ["GET", "balance"])
  end

  test "WATCH lets EXEC through when the watched key is untouched", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "balance", "100"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["WATCH", "balance"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])
    assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["SET", "balance", "0"])
    assert {[{:simple, "OK"}], conn} = Client.command(conn, ["EXEC"])
    assert {"0", _conn} = Client.command(conn, ["GET", "balance"])
  end

  test "UNWATCH clears watches so a later EXEC is unaffected", %{port: port, conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "balance", "100"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["WATCH", "balance"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["UNWATCH"])

    interloper = Client.connect(port)
    assert {{:simple, "OK"}, _interloper} = Client.command(interloper, ["SET", "balance", "50"])
    Client.close(interloper)

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])
    assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["SET", "balance", "0"])
    assert {[{:simple, "OK"}], conn} = Client.command(conn, ["EXEC"])
    assert {"0", _conn} = Client.command(conn, ["GET", "balance"])
  end

  test "WATCH is rejected inside a MULTI block", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])

    assert {{:error, "ERR WATCH inside MULTI is not allowed"}, conn} =
             Client.command(conn, ["WATCH", "key"])

    assert {[], _conn} = Client.command(conn, ["EXEC"])
  end

  test "EXEC clears watches even after a successful run", %{port: port, conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "balance", "100"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["WATCH", "balance"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])
    assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["SET", "balance", "0"])
    assert {[{:simple, "OK"}], conn} = Client.command(conn, ["EXEC"])

    interloper = Client.connect(port)
    assert {{:simple, "OK"}, _interloper} = Client.command(interloper, ["SET", "balance", "77"])
    Client.close(interloper)

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])
    assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["SET", "unrelated", "x"])
    assert {[{:simple, "OK"}], _conn} = Client.command(conn, ["EXEC"])
  end
end
