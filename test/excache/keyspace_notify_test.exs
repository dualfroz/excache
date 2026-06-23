defmodule Excache.KeyspaceNotifyTest do
  use Excache.Test.ServerCase, async: false

  test "a SET fires both a keyspace and a keyevent notification", %{port: port, conn: conn} do
    keyspace_sub = Client.connect(port)
    keyevent_sub = Client.connect(port)

    assert {["subscribe", "__keyspace@0__:notified", 1], keyspace_sub} =
             Client.command(keyspace_sub, ["SUBSCRIBE", "__keyspace@0__:notified"])

    assert {["subscribe", "__keyevent@0__:set", 1], keyevent_sub} =
             Client.command(keyevent_sub, ["SUBSCRIBE", "__keyevent@0__:set"])

    assert {{:simple, "OK"}, _conn} = Client.command(conn, ["SET", "notified", "v"])

    assert {["message", "__keyspace@0__:notified", "set"], _keyspace_sub} =
             Client.read(keyspace_sub)

    assert {["message", "__keyevent@0__:set", "notified"], _keyevent_sub} =
             Client.read(keyevent_sub)

    Client.close(keyspace_sub)
    Client.close(keyevent_sub)
  end

  test "DEL fires a del event per removed key", %{port: port, conn: conn} do
    subscriber = Client.connect(port)

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "removable", "v"])

    assert {["subscribe", "__keyevent@0__:del", 1], subscriber} =
             Client.command(subscriber, ["SUBSCRIBE", "__keyevent@0__:del"])

    assert {1, _conn} = Client.command(conn, ["DEL", "removable"])
    assert {["message", "__keyevent@0__:del", "removable"], _subscriber} = Client.read(subscriber)

    Client.close(subscriber)
  end

  test "an expiring key fires an expired event once the sweeper collects it", %{
    port: port,
    conn: conn
  } do
    subscriber = Client.connect(port)

    assert {["subscribe", "__keyevent@0__:expired", 1], subscriber} =
             Client.command(subscriber, ["SUBSCRIBE", "__keyevent@0__:expired"])

    assert {{:simple, "OK"}, _conn} = Client.command(conn, ["SET", "blink", "v", "PX", "40"])

    assert {["message", "__keyevent@0__:expired", "blink"], _subscriber} =
             Client.read(subscriber)

    Client.close(subscriber)
  end

  test "notifications can be turned off", %{port: port, conn: conn} do
    Application.put_env(:excache, :keyspace_notifications, false)

    subscriber = Client.connect(port)

    assert {["subscribe", "__keyevent@0__:set", 1], subscriber} =
             Client.command(subscriber, ["SUBSCRIBE", "__keyevent@0__:set"])

    assert {{:simple, "OK"}, _conn} = Client.command(conn, ["SET", "silent", "v"])

    assert {:error, :timeout} = :gen_tcp.recv(subscriber.socket, 0, 200)

    Application.delete_env(:excache, :keyspace_notifications)
    Client.close(subscriber)
  end
end
