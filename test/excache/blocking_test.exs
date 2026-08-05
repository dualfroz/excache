defmodule Excache.BlockingTest do
  use Excache.Test.ServerCase, async: false

  alias Excache.Blocking

  describe "immediate path" do
    test "BLPOP takes the head when the list already holds data", %{conn: conn} do
      assert {2, conn} = Client.command(conn, ["RPUSH", "jobs", "first", "second"])
      assert {["jobs", "first"], conn} = Client.command(conn, ["BLPOP", "jobs", "0"])
      assert {["second"], conn} = Client.command(conn, ["LRANGE", "jobs", "0", "-1"])
      assert {["jobs", "second"], conn} = Client.command(conn, ["BLPOP", "jobs", "0"])
      assert {0, _conn} = Client.command(conn, ["EXISTS", "jobs"])
    end

    test "BRPOP takes the tail", %{conn: conn} do
      assert {3, conn} = Client.command(conn, ["RPUSH", "tail", "a", "b", "c"])
      assert {["tail", "c"], _conn} = Client.command(conn, ["BRPOP", "tail", "0"])
    end

    test "BLPOP serves the first key given that has data", %{conn: conn} do
      assert {1, conn} = Client.command(conn, ["RPUSH", "second", "later"])
      assert {1, conn} = Client.command(conn, ["RPUSH", "first", "sooner"])

      assert {["first", "sooner"], conn} =
               Client.command(conn, ["BLPOP", "first", "second", "0"])

      assert {["second", "later"], _conn} =
               Client.command(conn, ["BLPOP", "first", "second", "0"])
    end

    test "BLPOP on a key holding another type answers WRONGTYPE without blocking", %{conn: conn} do
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "notalist", "v"])

      assert {{:error, "WRONGTYPE" <> _}, _conn} =
               Client.command(conn, ["BLPOP", "notalist", "0"])
    end

    test "argument errors", %{conn: conn} do
      assert {{:error, "ERR timeout is negative"}, conn} =
               Client.command(conn, ["BLPOP", "k", "-1"])

      assert {{:error, "ERR timeout is not a float or out of range"}, conn} =
               Client.command(conn, ["BLPOP", "k", "later"])

      assert {{:error, "ERR wrong number of arguments for 'blpop' command"}, conn} =
               Client.command(conn, ["BLPOP", "k"])

      assert {{:error, "ERR syntax error"}, _conn} =
               Client.command(conn, ["BLMOVE", "a", "b", "LEFT", "SIDEWAYS", "0"])
    end
  end

  describe "blocking path" do
    test "BLPOP waits until another connection pushes", %{conn: conn, port: port} do
      waiter = blockingTask(port, ["BLPOP", "queue", "0"])
      awaitWaiters("queue", 1)

      assert {1, conn} = Client.command(conn, ["LPUSH", "queue", "payload"])
      assert Task.await(waiter) == ["queue", "payload"]
      assert {0, _conn} = Client.command(conn, ["EXISTS", "queue"])
    end

    test "BRPOP waits on the tail side", %{conn: conn, port: port} do
      waiter = blockingTask(port, ["BRPOP", "queue", "0"])
      awaitWaiters("queue", 1)

      assert {2, _conn} = Client.command(conn, ["RPUSH", "queue", "old", "new"])
      assert Task.await(waiter) == ["queue", "new"]
    end

    test "a waiter registered on several keys wakes on whichever key receives data", %{
      conn: conn,
      port: port
    } do
      waiter = blockingTask(port, ["BLPOP", "left", "right", "0"])
      awaitWaiters("right", 1)

      assert {1, _conn} = Client.command(conn, ["LPUSH", "right", "hello"])
      assert Task.await(waiter) == ["right", "hello"]
    end

    test "a timed out BLPOP answers with a nil array on the wire", %{port: port} do
      socket = rawConnect(port)
      :ok = :gen_tcp.send(socket, encodeCommand(["BLPOP", "never", "0.15"]))

      {elapsed, reply} = :timer.tc(fn -> :gen_tcp.recv(socket, 0, 2000) end)

      assert reply == {:ok, "*-1\r\n"}
      assert elapsed > 100_000
      assert elapsed < 1_500_000

      :gen_tcp.close(socket)
    end

    test "a blocked connection still answers commands sent after it is unblocked", %{
      conn: conn,
      port: port
    } do
      pipelined = Client.connect(port)

      pipelined =
        Client.send_raw(pipelined, [
          encodeCommand(["BLPOP", "pipeq", "0"]),
          encodeCommand(["PING"])
        ])

      awaitWaiters("pipeq", 1)
      assert :gen_tcp.recv(pipelined.socket, 0, 100) == {:error, :timeout}

      assert {1, _conn} = Client.command(conn, ["LPUSH", "pipeq", "v"])

      assert {["pipeq", "v"], pipelined} = Client.read(pipelined)
      assert {{:simple, "PONG"}, pipelined} = Client.read(pipelined)

      Client.close(pipelined)
    end
  end

  describe "delivery accounting" do
    test "three waiters are served in the order they blocked", %{conn: conn, port: port} do
      first = blockingTask(port, ["BLPOP", "fifo", "0"])
      awaitWaiters("fifo", 1)
      second = blockingTask(port, ["BLPOP", "fifo", "0"])
      awaitWaiters("fifo", 2)
      third = blockingTask(port, ["BLPOP", "fifo", "0"])
      awaitWaiters("fifo", 3)

      assert {1, conn} = Client.command(conn, ["LPUSH", "fifo", "a"])
      assert Task.await(first) == ["fifo", "a"]

      assert {1, conn} = Client.command(conn, ["LPUSH", "fifo", "b"])
      assert Task.await(second) == ["fifo", "b"]

      assert {1, _conn} = Client.command(conn, ["LPUSH", "fifo", "c"])
      assert Task.await(third) == ["fifo", "c"]
    end

    test "one pushed element wakes exactly one of two waiters", %{conn: conn, port: port} do
      first = blockingTask(port, ["BLPOP", "single", "0"])
      awaitWaiters("single", 1)
      second = blockingTask(port, ["BLPOP", "single", "0"])
      awaitWaiters("single", 2)

      assert {1, conn} = Client.command(conn, ["LPUSH", "single", "only"])
      assert Task.await(first) == ["single", "only"]

      assert Task.yield(second, 150) == nil
      assert Blocking.waitingCount("single") == 1
      assert {0, conn} = Client.command(conn, ["EXISTS", "single"])

      assert {1, _conn} = Client.command(conn, ["LPUSH", "single", "later"])
      assert Task.await(second) == ["single", "later"]
    end

