defmodule Excache.WritePathTest do
  use Excache.Test.ServerCase, async: false

  alias Excache.Aof
  alias Excache.Blocking
  alias Excache.Command
  alias Excache.Config
  alias Excache.Eviction.Runner
  alias Excache.Eviction.Tracker
  alias Excache.Store

  @payload String.duplicate("x", 1000)
  @oom {:error, "OOM command not allowed when used memory > 'maxmemory'"}

  describe "renaming a key onto itself keeps it" do
    test "the store answers OK and the value survives" do
      assert :ok = Store.set("self:string", "payload", [])
      assert {:ok, "OK"} = Store.rename("self:string", "self:string")
      assert {:ok, "payload"} = Store.get("self:string")
    end

    test "a missing key renamed onto itself is still refused" do
      assert {:error, :nokey} = Store.rename("self:ghost", "self:ghost")
    end

    test "the remaining ttl is untouched" do
      assert :ok = Store.set("self:ttl", "payload", px: 60_000)
      assert {:ok, "OK"} = Store.rename("self:ttl", "self:ttl")

      remaining = Store.ttl("self:ttl", :seconds)
      assert remaining > 0 and remaining <= 60
    end

    test "a list survives the round trip over the wire", %{conn: conn} do
      assert {2, conn} = Client.command(conn, ["RPUSH", "self:list", "a", "b"])
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["RENAME", "self:list", "self:list"])
      assert {["a", "b"], conn} = Client.command(conn, ["LRANGE", "self:list", "0", "-1"])

      conn = replayFromScratch(conn)

      assert {["a", "b"], _conn} = Client.command(conn, ["LRANGE", "self:list", "0", "-1"])
    end

    test "RENAMENX onto itself answers zero and keeps the key", %{conn: conn} do
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "self:nx", "v"])
      assert {0, conn} = Client.command(conn, ["RENAMENX", "self:nx", "self:nx"])
      assert {"v", _conn} = Client.command(conn, ["GET", "self:nx"])
    end
  end

  describe "blocking moves obey the out of memory gate" do
    setup do
      Application.put_env(:excache, :eviction_recheck, 0)
      Config.reset()
      Tracker.clear(Tracker)
      Runner.resetStats()

      on_exit(fn ->
        Config.reset()
        Application.delete_env(:excache, :eviction_recheck)
      end)

      :ok
    end

    test "BRPOPLPUSH is refused once maxmemory is crossed", %{conn: conn} do
      conn = fillOverLimit(conn, "oom:brpoplpush")

      assert {@oom, conn} =
               Client.command(conn, ["BRPOPLPUSH", "oom:brpoplpush", "oom:moved", "0"])

      assert {1, conn} = Client.command(conn, ["LLEN", "oom:brpoplpush"])
      assert {0, _conn} = Client.command(conn, ["EXISTS", "oom:moved"])
    end

    test "BLMOVE is refused once maxmemory is crossed", %{conn: conn} do
      conn = fillOverLimit(conn, "oom:blmove")

      assert {@oom, conn} =
               Client.command(conn, ["BLMOVE", "oom:blmove", "oom:moved", "LEFT", "RIGHT", "0"])

      assert {1, conn} = Client.command(conn, ["LLEN", "oom:blmove"])
      assert {0, _conn} = Client.command(conn, ["EXISTS", "oom:moved"])
    end

    test "lifting the limit lets the same move through", %{conn: conn} do
      conn = fillOverLimit(conn, "oom:lifted")

      assert {@oom, conn} =
               Client.command(conn, ["BLMOVE", "oom:lifted", "oom:dst", "LEFT", "RIGHT", "0"])

      {{:simple, "OK"}, conn} = Client.command(conn, ["CONFIG", "SET", "maxmemory", "0"])

      assert {"value", conn} =
               Client.command(conn, ["BLMOVE", "oom:lifted", "oom:dst", "LEFT", "RIGHT", "0"])

      assert {["value"], _conn} = Client.command(conn, ["LRANGE", "oom:dst", "0", "-1"])
    end
  end

  describe "blocking writes reach the shared log funnel" do
    test "a blocked BLMOVE announces the push it performed", %{conn: conn, port: port} do
      subscriber = subscribeTo(port, "__keyevent@0__:rpush")

      waiter = blockingTask(port, ["BLMOVE", "funnel:src", "funnel:dst", "LEFT", "RIGHT", "0"])
      awaitWaiters("funnel:src", 1)

      assert {1, _conn} = Client.command(conn, ["LPUSH", "funnel:src", "item"])
      assert Task.await(waiter) == "item"

      assert {["message", "__keyevent@0__:rpush", "funnel:dst"], _subscriber} =
               Client.read(subscriber)

      Client.close(subscriber)
    end

    test "an immediately served BLMOVE announces the push as well", %{conn: conn, port: port} do
      assert {1, conn} = Client.command(conn, ["RPUSH", "inline:src", "item"])

      subscriber = subscribeTo(port, "__keyevent@0__:rpush")

      assert {"item", _conn} =
               Client.command(conn, ["BLMOVE", "inline:src", "inline:dst", "LEFT", "RIGHT", "0"])

      assert {["message", "__keyevent@0__:rpush", "inline:dst"], _subscriber} =
               Client.read(subscriber)

      Client.close(subscriber)
    end

    test "a blocked BLMOVE replays into the very same lists", %{conn: conn, port: port} do
      waiter = blockingTask(port, ["BLMOVE", "replay:src", "replay:dst", "LEFT", "RIGHT", "0"])
      awaitWaiters("replay:src", 1)

      assert {2, conn} = Client.command(conn, ["RPUSH", "replay:src", "moved", "stays"])
      assert Task.await(waiter) == "moved"

      {source, conn} = Client.command(conn, ["LRANGE", "replay:src", "0", "-1"])
      {destination, conn} = Client.command(conn, ["LRANGE", "replay:dst", "0", "-1"])

      log = flushedLog()
      refute log =~ "BLMOVE"

      conn = replayFromScratch(conn)

      assert {^source, conn} = Client.command(conn, ["LRANGE", "replay:src", "0", "-1"])
      assert {^destination, _conn} = Client.command(conn, ["LRANGE", "replay:dst", "0", "-1"])
    end

    test "the pop of a woken BLPOP is logged after the push that woke it", %{
      conn: conn,
      port: port
    } do
      waiter = blockingTask(port, ["BLPOP", "order:queue", "0"])
      awaitWaiters("order:queue", 1)

      assert {1, conn} = Client.command(conn, ["LPUSH", "order:queue", "payload"])
      assert Task.await(waiter) == ["order:queue", "payload"]

      log = flushedLog()

      assert offsetOf(log, "LPUSH") < offsetOf(log, "LPOP")

      conn = replayFromScratch(conn)

      assert {0, _conn} = Client.command(conn, ["EXISTS", "order:queue"])
    end
  end

  describe "the wake signal belongs to the command layer" do
    test "a push dispatched straight through the command router wakes a waiter", %{port: port} do
      waiter = blockingTask(port, ["BLPOP", "hook:queue", "2"])
      awaitWaiters("hook:queue", 1)

      Command.execute(["LPUSH", "hook:queue", "direct"], commandState())

      assert Task.await(waiter) == ["hook:queue", "direct"]
    end

    test "a rename dispatched straight through the command router wakes a waiter", %{port: port} do
      assert {:ok, 1} = Store.rpush("hook:source", ["carried"])

      waiter = blockingTask(port, ["BLPOP", "hook:target", "2"])
      awaitWaiters("hook:target", 1)

      Command.execute(["RENAME", "hook:source", "hook:target"], commandState())

      assert Task.await(waiter) == ["hook:target", "carried"]
    end
  end

  defp commandState do
    Map.put(Excache.Command.Transaction.initial_state(), :subscriptions, MapSet.new())
  end

  defp fillOverLimit(conn, listKey) do
    conn =
      Enum.reduce(1..30, conn, fn index, acc ->
        {{:simple, "OK"}, acc} = Client.command(acc, ["SET", "oom:key:#{index}", @payload])
        acc
      end)

    {1, conn} = Client.command(conn, ["RPUSH", listKey, "value"])
    {flat, conn} = Client.command(conn, ["MEMORY", "STATS"])

    allocated =
      flat
      |> Enum.chunk_every(2)
      |> Map.new(fn [name, value] -> {name, value} end)
      |> Map.fetch!("total.allocated")

    {{:simple, "OK"}, conn} =
      Client.command(conn, ["CONFIG", "SET", "maxmemory", Integer.to_string(div(allocated, 2))])

    conn
  end

  defp subscribeTo(port, channel) do
    subscriber = Client.connect(port)
    {["subscribe", ^channel, 1], subscriber} = Client.command(subscriber, ["SUBSCRIBE", channel])
    subscriber
  end

  defp blockingTask(port, args) do
    Task.async(fn ->
      client = Client.connect(port)
      {reply, _client} = Client.command(client, args)
      Client.close(client)
      reply
    end)
  end

  defp awaitWaiters(key, count), do: awaitUntil(fn -> Blocking.waitingCount(key) == count end)

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

  defp offsetOf(log, needle) do
    {offset, _length} = :binary.match(log, needle)
    offset
  end

  defp flushedLog do
    Aof.flush()
    {:ok, log} = File.read(Application.get_env(:excache, :aof_path))
    log
  end

  defp replayFromScratch(conn) do
    Store.flushdb()
    Aof.replay(Application.get_env(:excache, :aof_path))
    conn
  end
end
