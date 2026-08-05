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

    test "a two element push wakes both waiters with one element each", %{
      conn: conn,
      port: port
    } do
      first = blockingTask(port, ["BLPOP", "pair", "0"])
      awaitWaiters("pair", 1)
      second = blockingTask(port, ["BLPOP", "pair", "0"])
      awaitWaiters("pair", 2)

      assert {2, conn} = Client.command(conn, ["LPUSH", "pair", "a", "b"])

      assert Task.await(first) == ["pair", "b"]
      assert Task.await(second) == ["pair", "a"]
      assert {0, _conn} = Client.command(conn, ["EXISTS", "pair"])
    end

    test "a dropped connection leaves the element for the next client", %{
      conn: conn,
      port: port
    } do
      victim = Client.connect(port)
      Client.send_raw(victim, encodeCommand(["BLPOP", "orphan", "0"]))
      awaitWaiters("orphan", 1)

      Client.close(victim)
      awaitUntil(fn -> Blocking.waitingCount("orphan") == 0 end)

      assert {1, conn} = Client.command(conn, ["LPUSH", "orphan", "kept"])
      assert {["kept"], conn} = Client.command(conn, ["LRANGE", "orphan", "0", "-1"])
      assert {["orphan", "kept"], _conn} = Client.command(conn, ["BLPOP", "orphan", "0"])
    end
  end

  describe "isolation" do
    test "a blocked client stalls neither other clients nor the expiry sweeper", %{
      conn: conn,
      port: port
    } do
      subscriber = Client.connect(port)

      assert {["subscribe", "__keyevent@0__:expired", 1], subscriber} =
               Client.command(subscriber, ["SUBSCRIBE", "__keyevent@0__:expired"])

      waiter = blockingTask(port, ["BLPOP", "idle", "1"])
      awaitWaiters("idle", 1)

      assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "other", "v"])
      assert {"v", conn} = Client.command(conn, ["GET", "other"])
      assert {3, conn} = Client.command(conn, ["RPUSH", "unrelated", "a", "b", "c"])
      assert {["a", "b", "c"], conn} = Client.command(conn, ["LRANGE", "unrelated", "0", "-1"])

      assert {{:simple, "OK"}, _conn} =
               Client.command(conn, ["SET", "ephemeral", "v", "PX", "60"])

      assert {["message", "__keyevent@0__:expired", "ephemeral"], _subscriber} =
               Client.read(subscriber)

      assert Task.await(waiter) == nil

      Client.close(subscriber)
    end

    test "blocking commands inside MULTI answer at once instead of waiting", %{port: port} do
      socket = rawConnect(port)

      :ok =
        :gen_tcp.send(socket, [
          encodeCommand(["MULTI"]),
          encodeCommand(["BLPOP", "txn", "0"]),
          encodeCommand(["EXEC"])
        ])

      assert readBytes(socket, byte_size("+OK\r\n+QUEUED\r\n*1\r\n*-1\r\n")) ==
               "+OK\r\n+QUEUED\r\n*1\r\n*-1\r\n"

      :gen_tcp.close(socket)
    end

    test "a queued blocking command still pops available data on EXEC", %{conn: conn} do
      assert {1, conn} = Client.command(conn, ["RPUSH", "txn", "ready"])
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])
      assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["BLPOP", "txn", "0"])
      assert {[["txn", "ready"]], conn} = Client.command(conn, ["EXEC"])
      assert {0, _conn} = Client.command(conn, ["EXISTS", "txn"])
    end

    test "a push made inside a transaction wakes a waiter", %{conn: conn, port: port} do
      waiter = blockingTask(port, ["BLPOP", "txnpush", "0"])
      awaitWaiters("txnpush", 1)

      assert {{:simple, "OK"}, conn} = Client.command(conn, ["MULTI"])
      assert {{:simple, "QUEUED"}, conn} = Client.command(conn, ["LPUSH", "txnpush", "fromtxn"])
      assert {[1], _conn} = Client.command(conn, ["EXEC"])

      assert Task.await(waiter) == ["txnpush", "fromtxn"]
    end
  end

  describe "persistence and notifications" do
    test "a woken BLPOP is logged as LPOP and replays without blocking", %{
      conn: conn,
      port: port
    } do
      aofPath = Application.get_env(:excache, :aof_path)

      waiter = blockingTask(port, ["BLPOP", "aofq", "0"])
      awaitWaiters("aofq", 1)

      assert {1, conn} = Client.command(conn, ["LPUSH", "aofq", "payload"])
      assert Task.await(waiter) == ["aofq", "payload"]

      Excache.Aof.flush()
      log = File.read!(aofPath)

      assert log =~ "LPOP"
      refute log =~ "BLPOP"

      Excache.Store.flushdb()
      Excache.Aof.replay(aofPath)

      assert {0, _conn} = Client.command(conn, ["EXISTS", "aofq"])
    end

    test "a woken BLPOP fires the same keyspace event as LPOP", %{conn: conn, port: port} do
      subscriber = Client.connect(port)

      assert {["subscribe", "__keyevent@0__:lpop", 1], subscriber} =
               Client.command(subscriber, ["SUBSCRIBE", "__keyevent@0__:lpop"])

      waiter = blockingTask(port, ["BLPOP", "notified", "0"])
      awaitWaiters("notified", 1)

      assert {1, _conn} = Client.command(conn, ["LPUSH", "notified", "v"])
      assert Task.await(waiter) == ["notified", "v"]

      assert {["message", "__keyevent@0__:lpop", "notified"], _subscriber} =
               Client.read(subscriber)

      Client.close(subscriber)
    end
  end

  describe "BLMOVE and BRPOPLPUSH" do
    test "BRPOPLPUSH moves an available element straight away", %{conn: conn} do
      assert {2, conn} = Client.command(conn, ["RPUSH", "src", "a", "b"])
      assert {"b", conn} = Client.command(conn, ["BRPOPLPUSH", "src", "dst", "0"])
      assert {["b"], conn} = Client.command(conn, ["LRANGE", "dst", "0", "-1"])
      assert {["a"], _conn} = Client.command(conn, ["LRANGE", "src", "0", "-1"])
    end

    test "BRPOPLPUSH waits for the source and times out with nil", %{conn: conn, port: port} do
      waiter = blockingTask(port, ["BRPOPLPUSH", "src", "dst", "0"])
      awaitWaiters("src", 1)

      assert {1, conn} = Client.command(conn, ["RPUSH", "src", "moved"])
      assert Task.await(waiter) == "moved"
      assert {["moved"], conn} = Client.command(conn, ["LRANGE", "dst", "0", "-1"])

      assert {nil, _conn} = Client.command(conn, ["BRPOPLPUSH", "empty", "dst", "0.1"])
    end

    test "BLMOVE honours both sides while blocked", %{conn: conn, port: port} do
      waiter = blockingTask(port, ["BLMOVE", "src", "dst", "LEFT", "RIGHT", "0"])
      awaitWaiters("src", 1)

      assert {2, conn} = Client.command(conn, ["RPUSH", "src", "head", "tail"])
      assert Task.await(waiter) == "head"

      assert {["tail"], conn} = Client.command(conn, ["LRANGE", "src", "0", "-1"])
      assert {2, conn} = Client.command(conn, ["RPUSH", "dst", "existing"])

      assert {"tail", conn} = Client.command(conn, ["BLMOVE", "src", "dst", "RIGHT", "LEFT", "0"])

      assert {["tail", "head", "existing"], _conn} =
               Client.command(conn, ["LRANGE", "dst", "0", "-1"])
    end

    test "a BLMOVE destination wakes a client blocked on it", %{conn: conn, port: port} do
      consumer = blockingTask(port, ["BLPOP", "stage2", "0"])
      awaitWaiters("stage2", 1)

      mover = blockingTask(port, ["BLMOVE", "stage1", "stage2", "LEFT", "RIGHT", "0"])
      awaitWaiters("stage1", 1)

      assert {1, conn} = Client.command(conn, ["LPUSH", "stage1", "item"])

      assert Task.await(mover) == "item"
      assert Task.await(consumer) == ["stage2", "item"]
      assert {0, _conn} = Client.command(conn, ["EXISTS", "stage2"])
    end

    test "a rename that replaces a waited key with another type reports WRONGTYPE", %{
      conn: conn,
      port: port
    } do
      waiter = blockingTask(port, ["BLPOP", "renamed", "0"])
      awaitWaiters("renamed", 1)

      assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "plain", "v"])
      assert {{:simple, "OK"}, _conn} = Client.command(conn, ["RENAME", "plain", "renamed"])

      assert {:error, "WRONGTYPE" <> _} = Task.await(waiter)
    end
  end

  describe "BLMPOP" do
    test "pops up to COUNT elements from the first non empty key", %{conn: conn} do
      assert {3, conn} = Client.command(conn, ["RPUSH", "mp", "a", "b", "c"])

      assert {["mp", ["a", "b"]], conn} =
               Client.command(conn, ["BLMPOP", "0", "2", "missing", "mp", "LEFT", "COUNT", "2"])

      assert {["mp", ["c"]], _conn} =
               Client.command(conn, ["BLMPOP", "0", "1", "mp", "LEFT"])
    end

    test "blocks across several keys and answers RIGHT side", %{conn: conn, port: port} do
      waiter = blockingTask(port, ["BLMPOP", "0", "2", "mp1", "mp2", "RIGHT", "COUNT", "2"])
      awaitWaiters("mp2", 1)

      assert {3, conn} = Client.command(conn, ["RPUSH", "mp2", "x", "y", "z"])
      assert Task.await(waiter) == ["mp2", ["z", "y"]]
      assert {["x"], _conn} = Client.command(conn, ["LRANGE", "mp2", "0", "-1"])
    end

    test "times out with a nil array", %{conn: conn} do
      assert {nil, _conn} = Client.command(conn, ["BLMPOP", "0.1", "1", "mp", "LEFT"])
    end

    test "argument errors", %{conn: conn} do
      assert {{:error, "ERR numkeys should be greater than 0"}, conn} =
               Client.command(conn, ["BLMPOP", "0", "0", "mp", "LEFT"])

      assert {{:error, "ERR syntax error"}, conn} =
               Client.command(conn, ["BLMPOP", "0", "1", "mp", "SIDEWAYS"])

      assert {{:error, "ERR count should be greater than 0"}, conn} =
               Client.command(conn, ["BLMPOP", "0", "1", "mp", "LEFT", "COUNT", "0"])

      assert {{:error, "ERR syntax error"}, _conn} =
               Client.command(conn, ["BLMPOP", "0", "2", "mp", "LEFT"])
    end
  end

  defp blockingTask(port, args) do
    Task.async(fn ->
      client = Client.connect(port)
      {reply, _client} = Client.command(client, args)
      Client.close(client)
      reply
    end)
  end

  defp awaitWaiters(key, count) do
    awaitUntil(fn -> Blocking.waitingCount(key) == count end)
  end

  defp awaitUntil(check, attempts \\ 400) do
    cond do
      check.() ->
        :ok

      attempts == 0 ->
        flunk("the expected registry state was never reached")

      true ->
        Process.sleep(5)
        awaitUntil(check, attempts - 1)
    end
  end

  defp rawConnect(port) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    socket
  end

  defp readBytes(socket, size) do
    {:ok, bytes} = :gen_tcp.recv(socket, size, 2000)
    bytes
  end

  defp encodeCommand(args) do
    [
      "*",
      Integer.to_string(length(args)),
      "\r\n",
      Enum.map(args, fn arg -> ["$", Integer.to_string(byte_size(arg)), "\r\n", arg, "\r\n"] end)
    ]
  end
end
