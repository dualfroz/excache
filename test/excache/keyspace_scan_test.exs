defmodule Excache.KeyspaceScanTest do
  use Excache.Test.ServerCase, async: false

  test "SCAN pages through the whole keyspace and returns every key once", %{conn: conn} do
    conn = seed(conn, "key", 1..120)

    {keys, calls, conn} = drain(conn, fn cursor -> ["SCAN", cursor] end)

    assert calls > 1
    assert length(keys) == 120
    assert Enum.sort(keys) == Enum.sort(Enum.map(1..120, &"key:#{&1}"))

    {[cursor, batch], _conn} = Client.command(conn, ["SCAN", "0", "COUNT", "1000"])
    assert cursor == "0"
    assert length(batch) == 120
  end

  test "SCAN filters with MATCH and TYPE", %{conn: conn} do
    conn = seed(conn, "user", 1..30)
    conn = seed(conn, "session", 1..30)

    assert {1, conn} = Client.command(conn, ["RPUSH", "user:list", "a"])

    {users, _calls, conn} = drain(conn, fn cursor -> ["SCAN", cursor, "MATCH", "user:*"] end)
    assert length(users) == 31

    {strings, _calls, conn} =
      drain(conn, fn cursor -> ["SCAN", cursor, "MATCH", "user:*", "TYPE", "string"] end)

    assert length(strings) == 30
    refute "user:list" in strings

    {lists, _calls, conn} = drain(conn, fn cursor -> ["SCAN", cursor, "TYPE", "list"] end)
    assert lists == ["user:list"]

    {none, _calls, _conn} = drain(conn, fn cursor -> ["SCAN", cursor, "MATCH", "nothing:*"] end)
    assert none == []
  end

  test "SCAN returns keys that live through the whole iteration", %{conn: conn} do
    conn = seed(conn, "stable", 1..80)

    {[cursor, first], conn} = Client.command(conn, ["SCAN", "0"])
    refute cursor == "0"

    conn = seed(conn, "noise", 1..60)
    assert {1, conn} = Client.command(conn, ["DEL", "noise:1"])
    assert {1, conn} = Client.command(conn, ["DEL", "noise:2"])

    {seen, _calls, _conn} = drainFrom(conn, fn value -> ["SCAN", value] end, cursor, first)

    stable = Enum.filter(seen, &String.starts_with?(&1, "stable:"))
    assert length(stable) == 80
    assert length(Enum.uniq(stable)) == 80
  end

  test "SCAN validates the cursor", %{conn: conn} do
    assert {{:error, "ERR invalid cursor"}, conn} = Client.command(conn, ["SCAN", "abc"])
    assert {{:error, "ERR invalid cursor"}, conn} = Client.command(conn, ["SCAN", "-1"])

    {[cursor, keys], conn} = Client.command(conn, ["SCAN", "9999"])
    assert cursor == "0"
    assert keys == []

    assert {{:error, "ERR syntax error"}, conn} =
             Client.command(conn, ["SCAN", "0", "COUNT", "0"])

    assert {{:error, "ERR value is not an integer or out of range"}, conn} =
             Client.command(conn, ["SCAN", "0", "COUNT", "many"])

    assert {{:error, "ERR syntax error"}, conn} = Client.command(conn, ["SCAN", "0", "NOVALUES"])
    assert {{:error, "ERR syntax error"}, conn} = Client.command(conn, ["SCAN", "0", "MATCH"])

    assert {{:error, "ERR wrong number of arguments for 'scan' command"}, _conn} =
             Client.command(conn, ["SCAN"])
  end

  test "HSCAN returns field value pairs, MATCH and NOVALUES", %{conn: conn} do
    assert {3, conn} = Client.command(conn, ["HSET", "h", "f1", "v1", "f2", "v2", "other", "v3"])

    {[cursor, flat], conn} = Client.command(conn, ["HSCAN", "h", "0"])
    assert cursor == "0"

    assert flat |> Enum.chunk_every(2) |> Enum.sort() == [
             ["f1", "v1"],
             ["f2", "v2"],
             ["other", "v3"]
           ]

    {[_cursor, fields], conn} = Client.command(conn, ["HSCAN", "h", "0", "NOVALUES"])
    assert Enum.sort(fields) == ["f1", "f2", "other"]

    {[_cursor, matched], conn} = Client.command(conn, ["HSCAN", "h", "0", "MATCH", "f*"])
    assert matched |> Enum.chunk_every(2) |> Enum.sort() == [["f1", "v1"], ["f2", "v2"]]

    {[_cursor, names], conn} =
      Client.command(conn, ["HSCAN", "h", "0", "MATCH", "f?", "NOVALUES"])

    assert Enum.sort(names) == ["f1", "f2"]

    {[emptyCursor, empty], conn} = Client.command(conn, ["HSCAN", "nohash", "0"])
    assert emptyCursor == "0"
    assert empty == []

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "text", "v"])
    assert {{:error, "WRONGTYPE" <> _}, conn} = Client.command(conn, ["HSCAN", "text", "0"])

    assert {{:error, "ERR syntax error"}, conn} =
             Client.command(conn, ["HSCAN", "h", "0", "TYPE", "hash"])

    assert {{:error, "ERR invalid cursor"}, _conn} = Client.command(conn, ["HSCAN", "h", "later"])
  end

  test "HSCAN pages through a large hash exactly once per field", %{conn: conn} do
    fields = Enum.flat_map(1..300, fn index -> ["field:#{index}", "value:#{index}"] end)
    assert {300, conn} = Client.command(conn, ["HSET", "big"] ++ fields)

    {flat, calls, conn} = drain(conn, fn cursor -> ["HSCAN", "big", cursor] end)
    pairs = Enum.chunk_every(flat, 2)

    assert calls > 1
    assert length(pairs) == 300
    assert length(Enum.uniq(pairs)) == 300
    assert ["field:7", "value:7"] in pairs

    {sparse, sparseCalls, _conn} =
      drain(conn, fn cursor -> ["HSCAN", "big", cursor, "COUNT", "500"] end)

    assert sparseCalls == 1
    assert length(sparse) == 600
  end

  test "SSCAN pages a set and keeps members that live through the iteration", %{conn: conn} do
    members = Enum.map(1..200, &"member:#{&1}")
    assert {200, conn} = Client.command(conn, ["SADD", "bag"] ++ members)

    {[cursor, first], conn} = Client.command(conn, ["SSCAN", "bag", "0"])
    refute cursor == "0"

    assert {50, conn} = Client.command(conn, ["SADD", "bag"] ++ Enum.map(1..50, &"extra:#{&1}"))
    assert {25, conn} = Client.command(conn, ["SREM", "bag"] ++ Enum.map(1..25, &"extra:#{&1}"))

    {seen, _calls, conn} = drainFrom(conn, fn value -> ["SSCAN", "bag", value] end, cursor, first)

    stable = Enum.filter(seen, &String.starts_with?(&1, "member:"))
    assert length(stable) == 200
    assert length(Enum.uniq(stable)) == 200

    {[_cursor, matched], conn} =
      Client.command(conn, ["SSCAN", "bag", "0", "MATCH", "member:1", "COUNT", "1000"])

    assert matched == ["member:1"]

    assert {{:error, "ERR syntax error"}, _conn} =
             Client.command(conn, ["SSCAN", "bag", "0", "NOVALUES"])
  end

  test "ZSCAN returns members with their scores", %{conn: conn} do
    assert {3, conn} = Client.command(conn, ["ZADD", "board", "1", "a", "2.5", "b", "-3", "c"])

    {[cursor, flat], conn} = Client.command(conn, ["ZSCAN", "board", "0"])
    assert cursor == "0"

    assert flat |> Enum.chunk_every(2) |> Enum.sort() == [["a", "1"], ["b", "2.5"], ["c", "-3"]]

    {[_cursor, matched], conn} = Client.command(conn, ["ZSCAN", "board", "0", "MATCH", "a"])
    assert matched == ["a", "1"]

    {[emptyCursor, empty], conn} = Client.command(conn, ["ZSCAN", "nozset", "0"])
    assert emptyCursor == "0"
    assert empty == []

    assert {1, conn} = Client.command(conn, ["RPUSH", "listkey", "x"])
    assert {{:error, "WRONGTYPE" <> _}, _conn} = Client.command(conn, ["ZSCAN", "listkey", "0"])
  end

  test "ZSCAN pages a large sorted set exactly once per member", %{conn: conn} do
    entries = Enum.flat_map(1..250, fn index -> [Integer.to_string(index), "member:#{index}"] end)
    assert {250, conn} = Client.command(conn, ["ZADD", "ranking"] ++ entries)

    {flat, calls, _conn} = drain(conn, fn cursor -> ["ZSCAN", "ranking", cursor] end)
    pairs = Enum.chunk_every(flat, 2)

    assert calls > 1
    assert length(pairs) == 250
    assert length(Enum.uniq(pairs)) == 250
    assert ["member:42", "42"] in pairs
  end

  defp seed(conn, prefix, range) do
    Enum.reduce(range, conn, fn index, current ->
      {{:simple, "OK"}, next} = Client.command(current, ["SET", "#{prefix}:#{index}", "v"])
      next
    end)
  end

  defp drain(conn, buildArgs), do: drainFrom(conn, buildArgs, "0", [])

  defp drainFrom(conn, buildArgs, cursor, acc), do: drainFrom(conn, buildArgs, cursor, acc, 0)

  defp drainFrom(_conn, _buildArgs, _cursor, _acc, calls) when calls > 500,
    do: flunk("scan never reached cursor 0")

  defp drainFrom(conn, buildArgs, cursor, acc, calls) do
    {[next, batch], conn} = Client.command(conn, buildArgs.(cursor))
    collected = acc ++ batch

    if next == "0" do
      {collected, calls + 1, conn}
    else
      drainFrom(conn, buildArgs, next, collected, calls + 1)
    end
  end
end
