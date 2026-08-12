defmodule Excache.StoreOpsRegressionTest do
  use Excache.Test.ServerCase, async: false

  alias Excache.Store

  describe "SMOVE must not drop the member when the destination has the wrong type" do
    test "the member stays in the source and the caller is told about the type clash" do
      source = "regr:smove:src"
      dest = "regr:smove:dst-string"

      assert {:ok, 1} = Store.sadd(source, ["alpha"])
      assert :ok = Store.set(dest, "not-a-set", [])

      assert {:error, :wrongtype} = Store.smove(source, dest, "alpha")
      assert {:ok, ["alpha"]} = Store.smembers(source)
      assert {:ok, "not-a-set"} = Store.get(dest)
    end

    test "the destination type is checked before the source is touched" do
      source = "regr:smove:absent-src"
      dest = "regr:smove:dst-list"

      assert {:ok, 1} = Store.rpush(dest, ["element"])

      assert {:error, :wrongtype} = Store.smove(source, dest, "alpha")
      assert {:ok, ["element"]} = Store.lrange(dest, 0, -1)
    end

    test "a wrong type on the source is reported and the destination is left alone" do
      source = "regr:smove:src-string"
      dest = "regr:smove:dst-set"

      assert :ok = Store.set(source, "not-a-set", [])
      assert {:ok, 1} = Store.sadd(dest, ["beta"])

      assert {:error, :wrongtype} = Store.smove(source, dest, "alpha")
      assert {:ok, ["beta"]} = Store.smembers(dest)
    end

    test "a healthy move across two shards still transfers the member exactly once" do
      source = "regr:smove:move-src"
      dest = keyInOtherShard("regr:smove:move-dst-", source)

      assert {:ok, 2} = Store.sadd(source, ["alpha", "gamma"])
      assert {:ok, 1} = Store.sadd(dest, ["beta"])

      assert {:ok, 1} = Store.smove(source, dest, "alpha")
      assert {:ok, ["gamma"]} = Store.smembers(source)
      assert {:ok, members} = Store.smembers(dest)
      assert Enum.sort(members) == ["alpha", "beta"]
    end

    test "moving a member the source does not hold reports zero and adds nothing" do
      source = "regr:smove:miss-src"
      dest = "regr:smove:miss-dst"

      assert {:ok, 1} = Store.sadd(source, ["alpha"])
      assert {:ok, 1} = Store.sadd(dest, ["beta"])

      assert {:ok, 0} = Store.smove(source, dest, "nothing-like-this")
      assert {:ok, ["alpha"]} = Store.smembers(source)
      assert {:ok, ["beta"]} = Store.smembers(dest)
    end

    test "SMOVE over RESP reports WRONGTYPE and keeps the member", %{conn: conn} do
      assert {1, conn} = Client.command(conn, ["SADD", "regr:smove:resp-src", "alpha"])
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "regr:smove:resp-dst", "s"])

      assert {{:error, "WRONGTYPE" <> _}, conn} =
               Client.command(conn, [
                 "SMOVE",
                 "regr:smove:resp-src",
                 "regr:smove:resp-dst",
                 "alpha"
               ])

      assert {["alpha"], _conn} = Client.command(conn, ["SMEMBERS", "regr:smove:resp-src"])
    end
  end

  describe "RPOPLPUSH must not drop the element when the destination has the wrong type" do
    test "the element stays at the tail of the source and the caller sees the error" do
      source = "regr:rpoplpush:src"
      dest = "regr:rpoplpush:dst-string"

      assert {:ok, 2} = Store.rpush(source, ["head", "tail"])
      assert :ok = Store.set(dest, "not-a-list", [])

      assert {:error, :wrongtype} = Store.rpoplpush(source, dest)
      assert {:ok, ["head", "tail"]} = Store.lrange(source, 0, -1)
      assert {:ok, "not-a-list"} = Store.get(dest)
    end

    test "the destination type is checked before the source is popped" do
      source = "regr:rpoplpush:absent-src"
      dest = "regr:rpoplpush:dst-set"

      assert {:ok, 1} = Store.sadd(dest, ["beta"])

      assert {:error, :wrongtype} = Store.rpoplpush(source, dest)
      assert {:ok, ["beta"]} = Store.smembers(dest)
    end

    test "a healthy rotation across two shards moves the tail to the head" do
      source = "regr:rpoplpush:rot-src"
      dest = keyInOtherShard("regr:rpoplpush:rot-dst-", source)

      assert {:ok, 2} = Store.rpush(source, ["head", "tail"])
      assert {:ok, 1} = Store.rpush(dest, ["existing"])

      assert {:ok, "tail"} = Store.rpoplpush(source, dest)
      assert {:ok, ["head"]} = Store.lrange(source, 0, -1)
      assert {:ok, ["tail", "existing"]} = Store.lrange(dest, 0, -1)
    end

    test "an empty source yields nil and never creates the destination" do
      source = "regr:rpoplpush:empty-src"
      dest = "regr:rpoplpush:empty-dst"

      assert {:ok, nil} = Store.rpoplpush(source, dest)
      assert 0 == Store.exists([dest])
    end

    test "RPOPLPUSH over RESP reports WRONGTYPE and keeps the element", %{conn: conn} do
      assert {1, conn} = Client.command(conn, ["RPUSH", "regr:rpoplpush:resp-src", "tail"])

      assert {{:simple, "OK"}, conn} =
               Client.command(conn, ["SET", "regr:rpoplpush:resp-dst", "s"])

      assert {{:error, "WRONGTYPE" <> _}, conn} =
               Client.command(conn, [
                 "RPOPLPUSH",
                 "regr:rpoplpush:resp-src",
                 "regr:rpoplpush:resp-dst"
               ])

      assert {["tail"], _conn} =
               Client.command(conn, ["LRANGE", "regr:rpoplpush:resp-src", "0", "-1"])
    end
  end

  describe "LPOS with a nil count must not crash the shard" do
    test "a nil count means the first match and the shard survives the call" do
      key = "regr:lpos:nil-count"
      assert {:ok, 4} = Store.rpush(key, ["a", "b", "c", "b"])
      shardBefore = shardPid(key)

      assert {:ok, 1} = Store.lpos(key, "b", 1, nil, 0)

      assert shardPid(key) == shardBefore
      assert Process.alive?(shardBefore)
      assert {:ok, ["a", "b", "c", "b"]} = Store.lrange(key, 0, -1)
    end

    test "a nil count must not take the shard down and wipe its other keys" do
      key = "regr:lpos:blast-radius"
      neighbour = keyInSameShard("regr:lpos:neighbour-", key)

      assert :ok = Store.set(neighbour, "survivor", [])
      assert {:ok, 1} = Store.rpush(key, ["only"])
      shardBefore = shardPid(key)

      assert {:ok, 0} = Store.lpos(key, "only", 1, nil, 0)

      assert shardPid(key) == shardBefore
      assert {:ok, "survivor"} = Store.get(neighbour)
    end

    test "a nil count with no match yields nil rather than an empty list" do
      key = "regr:lpos:no-match"
      assert {:ok, 3} = Store.rpush(key, ["a", "b", "c"])

      assert {:ok, nil} = Store.lpos(key, "zzz", 1, nil, 0)
      assert {:ok, nil} = Store.lpos("regr:lpos:missing-key", "a", 1, nil, 0)
    end

    test "a nil count honours a positive rank" do
      key = "regr:lpos:rank-positive"
      assert {:ok, 4} = Store.rpush(key, ["a", "b", "c", "b"])

      assert {:ok, 1} = Store.lpos(key, "b", 1, nil, 0)
      assert {:ok, 3} = Store.lpos(key, "b", 2, nil, 0)
      assert {:ok, nil} = Store.lpos(key, "b", 3, nil, 0)
    end

    test "a nil count honours a negative rank and searches from the tail" do
      key = "regr:lpos:rank-negative"
      assert {:ok, 4} = Store.rpush(key, ["a", "b", "c", "b"])

      assert {:ok, 3} = Store.lpos(key, "b", -1, nil, 0)
      assert {:ok, 1} = Store.lpos(key, "b", -2, nil, 0)
      assert {:ok, nil} = Store.lpos(key, "b", -3, nil, 0)
    end

    test "a nil count honours maxlen" do
      key = "regr:lpos:maxlen"
      assert {:ok, 4} = Store.rpush(key, ["a", "b", "c", "b"])

      assert {:ok, 1} = Store.lpos(key, "b", 1, nil, 2)
      assert {:ok, nil} = Store.lpos(key, "b", 1, nil, 1)
    end

    test "a zero count still means every match, in both rank directions" do
      key = "regr:lpos:count-zero"
      assert {:ok, 4} = Store.rpush(key, ["a", "b", "c", "b"])

      assert {:ok, [1, 3]} = Store.lpos(key, "b", 1, 0, 0)
      assert {:ok, [3, 1]} = Store.lpos(key, "b", -1, 0, 0)
      assert {:ok, [1]} = Store.lpos(key, "b", 1, 1, 0)
    end

    test "a nil count on a wrong type key reports WRONGTYPE instead of crashing" do
      key = "regr:lpos:wrongtype"
      assert :ok = Store.set(key, "not-a-list", [])
      shardBefore = shardPid(key)

      assert {:error, :wrongtype} = Store.lpos(key, "a", 1, nil, 0)
      assert shardPid(key) == shardBefore
    end

    test "LPOS over RESP is unaffected by the fix", %{conn: conn} do
      assert {4, conn} = Client.command(conn, ["RPUSH", "regr:lpos:resp", "a", "b", "c", "b"])

      assert {1, conn} = Client.command(conn, ["LPOS", "regr:lpos:resp", "b"])
      assert {nil, conn} = Client.command(conn, ["LPOS", "regr:lpos:resp", "zzz"])
      assert {3, conn} = Client.command(conn, ["LPOS", "regr:lpos:resp", "b", "RANK", "-1"])
      assert {[1, 3], _conn} = Client.command(conn, ["LPOS", "regr:lpos:resp", "b", "COUNT", "0"])
    end
  end

  describe "HMGET and SMISMEMBER must surface WRONGTYPE" do
    test "HMGET on a key of the wrong type reports the error instead of nils" do
      key = "regr:hmget:wrongtype"
      assert :ok = Store.set(key, "not-a-hash", [])

      assert {:error, :wrongtype} = Store.hmget(key, ["field-a", "field-b"])
    end

    test "HMGET keeps returning a bare list for hashes and missing keys" do
      key = "regr:hmget:hash"
      assert {:ok, 1} = Store.hset(key, [{"field-a", "value-a"}])

      assert ["value-a", nil] = Store.hmget(key, ["field-a", "field-b"])
      assert [nil, nil] = Store.hmget("regr:hmget:missing", ["field-a", "field-b"])
    end

    test "SMISMEMBER on a key of the wrong type reports the error instead of zeros" do
      key = "regr:smismember:wrongtype"
      assert :ok = Store.set(key, "not-a-set", [])

      assert {:error, :wrongtype} = Store.smismember(key, ["alpha", "beta"])
    end

    test "SMISMEMBER keeps returning a bare list for sets and missing keys" do
      key = "regr:smismember:set"
      assert {:ok, 1} = Store.sadd(key, ["alpha"])

      assert [1, 0] = Store.smismember(key, ["alpha", "beta"])
      assert [0, 0] = Store.smismember("regr:smismember:missing", ["alpha", "beta"])
    end

    test "HMGET and SMISMEMBER over RESP still report WRONGTYPE", %{conn: conn} do
      assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "regr:mixed:string", "v"])

      assert {{:error, "WRONGTYPE" <> _}, conn} =
               Client.command(conn, ["HMGET", "regr:mixed:string", "field-a"])

      assert {{:error, "WRONGTYPE" <> _}, _conn} =
               Client.command(conn, ["SMISMEMBER", "regr:mixed:string", "alpha"])
    end
  end

  describe "SETRANGE with an empty value must not create a key" do
    test "an empty value on a missing key returns zero and creates nothing" do
      key = "regr:setrange:missing"

      assert {:ok, 0} = Store.setrange(key, 5, "")
      assert 0 == Store.exists([key])
      assert "none" == Store.type(key)
    end

    test "an empty value on an existing string returns its length and changes nothing" do
      key = "regr:setrange:existing"
      assert :ok = Store.set(key, "hello", [])

      assert {:ok, 5} = Store.setrange(key, 100, "")
      assert {:ok, "hello"} = Store.get(key)
      assert 1 == Store.exists([key])
    end

    test "an empty value on a key of the wrong type still reports WRONGTYPE" do
      key = "regr:setrange:wrongtype"
      assert {:ok, 1} = Store.rpush(key, ["element"])

      assert {:error, :wrongtype} = Store.setrange(key, 0, "")
      assert {:ok, ["element"]} = Store.lrange(key, 0, -1)
    end

    test "an empty value leaves an existing expiry in place" do
      key = "regr:setrange:ttl"
      assert :ok = Store.set(key, "hello", [])
      assert 1 == Store.expire(key, 60_000)

      assert {:ok, 5} = Store.setrange(key, 0, "")
      assert Store.ttl(key, :seconds) > 0
    end

    test "a non empty value still pads and overwrites as before" do
      key = "regr:setrange:write"

      assert {:ok, 8} = Store.setrange(key, 5, "abc")
      assert {:ok, <<0, 0, 0, 0, 0, "abc">>} = Store.get(key)
      assert {:ok, 8} = Store.setrange(key, 0, "xy")
      assert {:ok, <<"xy", 0, 0, 0, "abc">>} = Store.get(key)
    end

    test "SETRANGE with an empty value over RESP returns the length and creates nothing",
         %{conn: conn} do
      assert {0, conn} = Client.command(conn, ["SETRANGE", "regr:setrange:resp", "5", ""])
      assert {0, conn} = Client.command(conn, ["EXISTS", "regr:setrange:resp"])

      assert {{:simple, "OK"}, conn} =
               Client.command(conn, ["SET", "regr:setrange:resp", "hello"])

      assert {5, _conn} = Client.command(conn, ["SETRANGE", "regr:setrange:resp", "100", ""])
    end
  end

  defp shardIndex(key), do: rem(:erlang.phash2(key), Store.shard_count())

  defp shardPid(key), do: Process.whereis(Store.shard_name(shardIndex(key)))

  defp keyInOtherShard(prefix, reference) do
    findKey(prefix, fn candidate -> shardIndex(candidate) != shardIndex(reference) end)
  end

  defp keyInSameShard(prefix, reference) do
    findKey(prefix, fn candidate -> shardIndex(candidate) == shardIndex(reference) end)
  end

  defp findKey(prefix, accept) do
    Enum.find_value(1..1024, fn suffix ->
      candidate = prefix <> Integer.to_string(suffix)
      if accept.(candidate), do: candidate
    end)
  end
end
