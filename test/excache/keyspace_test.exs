defmodule Excache.KeyspaceTest do
  use Excache.Test.ServerCase, async: false

  test "STRLEN, GETRANGE and SETRANGE follow Redis offsets", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "greeting", "Hello World"])
    assert {11, conn} = Client.command(conn, ["STRLEN", "greeting"])
    assert {0, conn} = Client.command(conn, ["STRLEN", "absent"])

    assert {"Hello", conn} = Client.command(conn, ["GETRANGE", "greeting", "0", "4"])
    assert {"World", conn} = Client.command(conn, ["GETRANGE", "greeting", "-5", "-1"])
    assert {"", conn} = Client.command(conn, ["GETRANGE", "greeting", "10", "4"])
    assert {"", conn} = Client.command(conn, ["GETRANGE", "absent", "0", "-1"])

    assert {11, conn} = Client.command(conn, ["SETRANGE", "greeting", "6", "Redis"])
    assert {"Hello Redis", conn} = Client.command(conn, ["GETRANGE", "greeting", "0", "-1"])

    assert {8, conn} = Client.command(conn, ["SETRANGE", "padded", "5", "abc"])
    {padded, conn} = Client.command(conn, ["GET", "padded"])
    assert padded == :binary.copy(<<0>>, 5) <> "abc"

    assert {0, conn} = Client.command(conn, ["SETRANGE", "untouched", "0", ""])
    assert {0, conn} = Client.command(conn, ["EXISTS", "untouched"])

    assert {{:error, "ERR offset is out of range"}, _conn} =
             Client.command(conn, ["SETRANGE", "greeting", "-1", "x"])
  end

  test "SETNX and GETDEL", %{conn: conn} do
    assert {1, conn} = Client.command(conn, ["SETNX", "slot", "first"])
    assert {0, conn} = Client.command(conn, ["SETNX", "slot", "second"])
    assert {"first", conn} = Client.command(conn, ["GET", "slot"])

    assert {"first", conn} = Client.command(conn, ["GETDEL", "slot"])
    assert {0, conn} = Client.command(conn, ["EXISTS", "slot"])
    assert {nil, conn} = Client.command(conn, ["GETDEL", "slot"])

    assert {1, conn} = Client.command(conn, ["RPUSH", "queue", "x"])

    assert {{:error, "WRONGTYPE" <> _}, _conn} = Client.command(conn, ["GETDEL", "queue"])
  end

  test "INCRBYFLOAT accumulates and rejects non numeric values", %{conn: conn} do
    assert {"10.5", conn} = Client.command(conn, ["INCRBYFLOAT", "balance", "10.5"])
    assert {"10.6", conn} = Client.command(conn, ["INCRBYFLOAT", "balance", "0.1"])
    assert {"8", conn} = Client.command(conn, ["INCRBYFLOAT", "balance", "-2.6"])
    assert {"5", conn} = Client.command(conn, ["INCRBYFLOAT", "counter", "5"])

    assert {{:error, "ERR value is not a valid float"}, conn} =
             Client.command(conn, ["INCRBYFLOAT", "balance", "abc"])

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "word", "hello"])

    assert {{:error, "ERR value is not a valid float"}, _conn} =
             Client.command(conn, ["INCRBYFLOAT", "word", "1"])
  end

  test "HSETNX and HMGET", %{conn: conn} do
    assert {1, conn} = Client.command(conn, ["HSETNX", "profile", "name", "ann"])
    assert {0, conn} = Client.command(conn, ["HSETNX", "profile", "name", "ben"])
    assert {"ann", conn} = Client.command(conn, ["HGET", "profile", "name"])
    assert {1, conn} = Client.command(conn, ["HSETNX", "profile", "city", "gdansk"])

    {values, conn} = Client.command(conn, ["HMGET", "profile", "name", "ghost", "city"])
    assert values == ["ann", nil, "gdansk"]

    {absent, conn} = Client.command(conn, ["HMGET", "nohash", "a", "b"])
    assert absent == [nil, nil]

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "plain", "v"])

    assert {{:error, "WRONGTYPE" <> _}, _conn} = Client.command(conn, ["HMGET", "plain", "a"])
  end

  test "SMISMEMBER and SINTERCARD", %{conn: conn} do
    assert {3, conn} = Client.command(conn, ["SADD", "left", "a", "b", "c"])
    assert {3, conn} = Client.command(conn, ["SADD", "right", "b", "c", "d"])

    {flags, conn} = Client.command(conn, ["SMISMEMBER", "left", "a", "zz", "c"])
    assert flags == [1, 0, 1]

    {none, conn} = Client.command(conn, ["SMISMEMBER", "noset", "a"])
    assert none == [0]

    assert {2, conn} = Client.command(conn, ["SINTERCARD", "2", "left", "right"])
    assert {2, conn} = Client.command(conn, ["SINTERCARD", "2", "left", "right", "LIMIT", "0"])
    assert {1, conn} = Client.command(conn, ["SINTERCARD", "2", "left", "right", "LIMIT", "1"])

    assert {{:error, "ERR numkeys should be greater than 0"}, conn} =
             Client.command(conn, ["SINTERCARD", "0", "left"])

    assert {{:error, "ERR Number of keys can't be greater than number of args"}, conn} =
             Client.command(conn, ["SINTERCARD", "3", "left"])

    assert {{:error, "ERR LIMIT can't be negative"}, conn} =
             Client.command(conn, ["SINTERCARD", "1", "left", "LIMIT", "-1"])

    assert {{:error, "ERR syntax error"}, _conn} =
             Client.command(conn, ["SINTERCARD", "1", "left", "BOUND", "1"])
  end

  test "SPOP replies with a bulk without count and an array with count", %{conn: conn} do
    assert {3, conn} = Client.command(conn, ["SADD", "bag", "a", "b", "c"])

    {single, conn} = Client.command(conn, ["SPOP", "bag"])
    assert single in ["a", "b", "c"]

    {batch, conn} = Client.command(conn, ["SPOP", "bag", "5"])
    assert length(batch) == 2
    assert {0, conn} = Client.command(conn, ["EXISTS", "bag"])

    assert {nil, conn} = Client.command(conn, ["SPOP", "bag"])
    assert {[], conn} = Client.command(conn, ["SPOP", "bag", "3"])

    assert {5, conn} = Client.command(conn, ["SADD", "bag", "a", "b", "c", "d", "e"])
    assert {[], conn} = Client.command(conn, ["SPOP", "bag", "0"])
    assert {5, conn} = Client.command(conn, ["SCARD", "bag"])

    assert {{:error, "ERR value is out of range, must be positive"}, _conn} =
             Client.command(conn, ["SPOP", "bag", "-1"])
  end

  test "SRANDMEMBER repeats members for a negative count", %{conn: conn} do
    assert {2, conn} = Client.command(conn, ["SADD", "colors", "red", "green"])

    {single, conn} = Client.command(conn, ["SRANDMEMBER", "colors"])
    assert single in ["red", "green"]

    {repeats, conn} = Client.command(conn, ["SRANDMEMBER", "colors", "-5"])
    assert length(repeats) == 5
    assert Enum.all?(repeats, &(&1 in ["red", "green"]))

    {capped, conn} = Client.command(conn, ["SRANDMEMBER", "colors", "5"])
    assert Enum.sort(capped) == ["green", "red"]

    assert {2, conn} = Client.command(conn, ["SCARD", "colors"])
    assert {nil, conn} = Client.command(conn, ["SRANDMEMBER", "nothing"])
    assert {[], _conn} = Client.command(conn, ["SRANDMEMBER", "nothing", "3"])
  end

  test "SMOVE moves a member and refuses a destination of the wrong type", %{conn: conn} do
    assert {2, conn} = Client.command(conn, ["SADD", "source", "x", "y"])
    assert {1, conn} = Client.command(conn, ["SADD", "target", "z"])

    assert {1, conn} = Client.command(conn, ["SMOVE", "source", "target", "x"])
    assert {0, conn} = Client.command(conn, ["SMOVE", "source", "target", "ghost"])

    {members, conn} = Client.command(conn, ["SMEMBERS", "target"])
    assert Enum.sort(members) == ["x", "z"]

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "notaset", "v"])

    assert {{:error, "WRONGTYPE" <> _}, conn} =
             Client.command(conn, ["SMOVE", "source", "notaset", "y"])

    {left, _conn} = Client.command(conn, ["SMEMBERS", "source"])
    assert left == ["y"]
  end

  test "LREM counts from the tail for a negative count", %{conn: conn} do
    assert {5, conn} = Client.command(conn, ["RPUSH", "log", "a", "b", "a", "c", "a"])
    assert {2, conn} = Client.command(conn, ["LREM", "log", "-2", "a"])

    {rest, conn} = Client.command(conn, ["LRANGE", "log", "0", "-1"])
    assert rest == ["a", "b", "c"]

    assert {1, conn} = Client.command(conn, ["LREM", "log", "0", "a"])
    assert {0, conn} = Client.command(conn, ["LREM", "log", "3", "zz"])
    assert {0, conn} = Client.command(conn, ["LREM", "absent", "0", "a"])

    assert {{:error, "ERR value is not an integer or out of range"}, _conn} =
             Client.command(conn, ["LREM", "log", "many", "a"])
  end

  test "LSET reports missing keys and out of range indexes", %{conn: conn} do
    assert {{:error, "ERR no such key"}, conn} =
             Client.command(conn, ["LSET", "nolist", "0", "v"])

    assert {3, conn} = Client.command(conn, ["RPUSH", "items", "a", "b", "c"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["LSET", "items", "-1", "z"])

    {items, conn} = Client.command(conn, ["LRANGE", "items", "0", "-1"])
    assert items == ["a", "b", "z"]

    assert {{:error, "ERR index out of range"}, conn} =
             Client.command(conn, ["LSET", "items", "9", "v"])

    assert {{:error, "ERR index out of range"}, _conn} =
             Client.command(conn, ["LSET", "items", "-9", "v"])
  end

  test "LTRIM keeps an inclusive window and drops an empty list", %{conn: conn} do
    assert {5, conn} = Client.command(conn, ["RPUSH", "queue", "a", "b", "c", "d", "e"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["LTRIM", "queue", "1", "-2"])

    {kept, conn} = Client.command(conn, ["LRANGE", "queue", "0", "-1"])
    assert kept == ["b", "c", "d"]

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["LTRIM", "queue", "5", "10"])
    assert {0, conn} = Client.command(conn, ["EXISTS", "queue"])
    assert {{:simple, "OK"}, _conn} = Client.command(conn, ["LTRIM", "absent", "0", "-1"])
  end

  test "LINSERT places an element around a pivot", %{conn: conn} do
    assert {2, conn} = Client.command(conn, ["RPUSH", "plan", "one", "three"])
    assert {3, conn} = Client.command(conn, ["LINSERT", "plan", "BEFORE", "three", "two"])
    assert {4, conn} = Client.command(conn, ["LINSERT", "plan", "AFTER", "three", "four"])

    {plan, conn} = Client.command(conn, ["LRANGE", "plan", "0", "-1"])
    assert plan == ["one", "two", "three", "four"]

    assert {-1, conn} = Client.command(conn, ["LINSERT", "plan", "BEFORE", "ghost", "x"])
    assert {0, conn} = Client.command(conn, ["LINSERT", "absent", "BEFORE", "a", "b"])

    assert {{:error, "ERR syntax error"}, _conn} =
             Client.command(conn, ["LINSERT", "plan", "SIDEWAYS", "one", "x"])
  end

  test "RPOPLPUSH moves, rotates and protects the destination", %{conn: conn} do
    assert {3, conn} = Client.command(conn, ["RPUSH", "source", "a", "b", "c"])
    assert {"c", conn} = Client.command(conn, ["RPOPLPUSH", "source", "target"])

    {target, conn} = Client.command(conn, ["LRANGE", "target", "0", "-1"])
    assert target == ["c"]

    assert {"b", conn} = Client.command(conn, ["RPOPLPUSH", "source", "source"])
    {rotated, conn} = Client.command(conn, ["LRANGE", "source", "0", "-1"])
    assert rotated == ["b", "a"]

    assert {nil, conn} = Client.command(conn, ["RPOPLPUSH", "absent", "target"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "text", "v"])

    assert {{:error, "WRONGTYPE" <> _}, conn} =
             Client.command(conn, ["RPOPLPUSH", "source", "text"])

    {intact, _conn} = Client.command(conn, ["LRANGE", "source", "0", "-1"])
    assert intact == ["b", "a"]
  end

  test "LPOS honours RANK, COUNT and MAXLEN", %{conn: conn} do
    assert {8, conn} =
             Client.command(conn, ["RPUSH", "mylist", "a", "b", "c", "1", "2", "3", "c", "c"])

    assert {2, conn} = Client.command(conn, ["LPOS", "mylist", "c"])
    assert {6, conn} = Client.command(conn, ["LPOS", "mylist", "c", "RANK", "2"])
    assert {7, conn} = Client.command(conn, ["LPOS", "mylist", "c", "RANK", "-1"])
    assert {6, conn} = Client.command(conn, ["LPOS", "mylist", "c", "RANK", "-2"])

    {all, conn} = Client.command(conn, ["LPOS", "mylist", "c", "COUNT", "0"])
    assert all == [2, 6, 7]

    {two, conn} = Client.command(conn, ["LPOS", "mylist", "c", "COUNT", "2"])
    assert two == [2, 6]

    {backwards, conn} = Client.command(conn, ["LPOS", "mylist", "c", "RANK", "-1", "COUNT", "0"])
    assert backwards == [7, 6, 2]

    assert {nil, conn} = Client.command(conn, ["LPOS", "mylist", "ghost"])
    {empty, conn} = Client.command(conn, ["LPOS", "mylist", "ghost", "COUNT", "0"])
    assert empty == []

    assert {nil, conn} = Client.command(conn, ["LPOS", "mylist", "c", "MAXLEN", "2"])
    assert {2, conn} = Client.command(conn, ["LPOS", "mylist", "c", "MAXLEN", "3"])
    assert {nil, conn} = Client.command(conn, ["LPOS", "absent", "c"])

    assert {{:error, "ERR RANK can't be zero." <> _}, conn} =
             Client.command(conn, ["LPOS", "mylist", "c", "RANK", "0"])

    assert {{:error, "ERR COUNT can't be negative"}, conn} =
             Client.command(conn, ["LPOS", "mylist", "c", "COUNT", "-1"])

    assert {{:error, "ERR MAXLEN can't be negative"}, conn} =
             Client.command(conn, ["LPOS", "mylist", "c", "MAXLEN", "-1"])

    assert {{:error, "ERR syntax error"}, _conn} =
             Client.command(conn, ["LPOS", "mylist", "c", "NEAR", "1"])
  end

  test "ZCOUNT handles exclusive and infinite bounds", %{conn: conn} do
    assert {4, conn} =
             Client.command(conn, ["ZADD", "prices", "10", "a", "20", "b", "30", "c", "40", "d"])

    assert {4, conn} = Client.command(conn, ["ZCOUNT", "prices", "-inf", "+inf"])
    assert {3, conn} = Client.command(conn, ["ZCOUNT", "prices", "20", "40"])
    assert {2, conn} = Client.command(conn, ["ZCOUNT", "prices", "(20", "40"])
    assert {1, conn} = Client.command(conn, ["ZCOUNT", "prices", "(20", "(40"])
    assert {2, conn} = Client.command(conn, ["ZCOUNT", "prices", "-inf", "20"])
    assert {0, conn} = Client.command(conn, ["ZCOUNT", "absent", "-inf", "+inf"])

    assert {{:error, "ERR min or max is not a float"}, _conn} =
             Client.command(conn, ["ZCOUNT", "prices", "abc", "10"])
  end

  test "ZREVRANGE, ZREVRANK and ZMSCORE", %{conn: conn} do
    assert {3, conn} = Client.command(conn, ["ZADD", "board", "1", "a", "2", "b", "3.5", "c"])

    {desc, conn} = Client.command(conn, ["ZREVRANGE", "board", "0", "-1"])
    assert desc == ["c", "b", "a"]

    {top, conn} = Client.command(conn, ["ZREVRANGE", "board", "0", "0"])
    assert top == ["c"]

    {tail, conn} = Client.command(conn, ["ZREVRANGE", "board", "-2", "-1"])
    assert tail == ["b", "a"]

    {scored, conn} = Client.command(conn, ["ZREVRANGE", "board", "0", "-1", "WITHSCORES"])
    assert scored == ["c", "3.5", "b", "2", "a", "1"]

    assert {{:error, "ERR syntax error"}, conn} =
             Client.command(conn, ["ZREVRANGE", "board", "0", "-1", "WITHVALUES"])

    assert {0, conn} = Client.command(conn, ["ZREVRANK", "board", "c"])
    assert {2, conn} = Client.command(conn, ["ZREVRANK", "board", "a"])
    assert {nil, conn} = Client.command(conn, ["ZREVRANK", "board", "ghost"])

    {scores, conn} = Client.command(conn, ["ZMSCORE", "board", "a", "ghost", "c"])
    assert scores == ["1", nil, "3.5"]

    {absent, _conn} = Client.command(conn, ["ZMSCORE", "nozset", "a"])
    assert absent == [nil]
  end

  test "RENAME and RENAMENX", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "old", "payload"])
    assert {1, conn} = Client.command(conn, ["EXPIRE", "old", "100"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["RENAME", "old", "fresh"])
    assert {"payload", conn} = Client.command(conn, ["GET", "fresh"])
    assert {0, conn} = Client.command(conn, ["EXISTS", "old"])

    {ttl, conn} = Client.command(conn, ["TTL", "fresh"])
    assert ttl > 0

    assert {{:error, "ERR no such key"}, conn} = Client.command(conn, ["RENAME", "old", "other"])

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "taken", "t"])
    assert {0, conn} = Client.command(conn, ["RENAMENX", "fresh", "taken"])
    assert {"payload", conn} = Client.command(conn, ["GET", "fresh"])
    assert {1, conn} = Client.command(conn, ["RENAMENX", "fresh", "free"])
    assert {"payload", conn} = Client.command(conn, ["GET", "free"])

    assert {{:error, "ERR no such key"}, _conn} = Client.command(conn, ["RENAMENX", "ghost", "x"])
  end

  test "COPY with REPLACE and DB", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "origin", "payload"])
    assert {1, conn} = Client.command(conn, ["COPY", "origin", "clone"])
    assert {"payload", conn} = Client.command(conn, ["GET", "clone"])
    assert {0, conn} = Client.command(conn, ["COPY", "origin", "clone"])

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "origin", "changed"])
    assert {1, conn} = Client.command(conn, ["COPY", "origin", "clone", "REPLACE"])
    assert {"changed", conn} = Client.command(conn, ["GET", "clone"])

    assert {1, conn} = Client.command(conn, ["COPY", "origin", "sameDb", "DB", "0"])
    assert {"changed", conn} = Client.command(conn, ["GET", "sameDb"])

    assert {{:error, "ERR DB index is out of range"}, conn} =
             Client.command(conn, ["COPY", "origin", "elsewhere", "DB", "3"])

    assert {{:error, "ERR source and destination objects are the same"}, conn} =
             Client.command(conn, ["COPY", "origin", "origin"])

    assert {0, conn} = Client.command(conn, ["COPY", "ghost", "anywhere"])

    assert {{:error, "ERR syntax error"}, _conn} =
             Client.command(conn, ["COPY", "origin", "clone", "FORCE"])
  end

  test "TOUCH and UNLINK count existing keys", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["MSET", "t1", "1", "t2", "2"])
    assert {2, conn} = Client.command(conn, ["TOUCH", "t1", "t2", "t3"])
    assert {2, conn} = Client.command(conn, ["UNLINK", "t1", "t2", "t3"])
    assert {0, conn} = Client.command(conn, ["EXISTS", "t1"])
    assert {0, _conn} = Client.command(conn, ["TOUCH", "t1"])
  end

  test "EXPIREAT, EXPIRETIME and PEXPIRETIME", %{conn: conn} do
    future = System.system_time(:second) + 100

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "session", "v"])
    assert {-1, conn} = Client.command(conn, ["EXPIRETIME", "session"])
    assert {-1, conn} = Client.command(conn, ["PEXPIRETIME", "session"])
    assert {-2, conn} = Client.command(conn, ["EXPIRETIME", "ghost"])
    assert {-2, conn} = Client.command(conn, ["PEXPIRETIME", "ghost"])

    assert {0, conn} = Client.command(conn, ["EXPIREAT", "ghost", Integer.to_string(future)])
    assert {1, conn} = Client.command(conn, ["EXPIREAT", "session", Integer.to_string(future)])
    assert {^future, conn} = Client.command(conn, ["EXPIRETIME", "session"])

    {millis, conn} = Client.command(conn, ["PEXPIRETIME", "session"])
    assert millis == future * 1000

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "doomed", "v"])
    assert {1, conn} = Client.command(conn, ["EXPIREAT", "doomed", "1000"])
    assert {0, conn} = Client.command(conn, ["EXISTS", "doomed"])

    assert {{:error, "ERR value is not an integer or out of range"}, _conn} =
             Client.command(conn, ["EXPIREAT", "session", "soon"])
  end

  test "EXPIREAT respects NX, XX, GT and LT", %{conn: conn} do
    base = System.system_time(:second) + 100

    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "lease", "v"])
    assert {0, conn} = Client.command(conn, ["EXPIREAT", "lease", Integer.to_string(base), "XX"])
    assert {0, conn} = Client.command(conn, ["EXPIREAT", "lease", Integer.to_string(base), "GT"])
    assert {1, conn} = Client.command(conn, ["EXPIREAT", "lease", Integer.to_string(base), "NX"])
    assert {^base, conn} = Client.command(conn, ["EXPIRETIME", "lease"])

    assert {0, conn} =
             Client.command(conn, ["EXPIREAT", "lease", Integer.to_string(base + 50), "NX"])

    later = base + 50
    assert {1, conn} = Client.command(conn, ["EXPIREAT", "lease", Integer.to_string(later), "GT"])
    assert {^later, conn} = Client.command(conn, ["EXPIRETIME", "lease"])

    assert {0, conn} = Client.command(conn, ["EXPIREAT", "lease", Integer.to_string(base), "GT"])
    assert {1, conn} = Client.command(conn, ["EXPIREAT", "lease", Integer.to_string(base), "LT"])
    assert {^base, conn} = Client.command(conn, ["EXPIRETIME", "lease"])
    assert {1, conn} = Client.command(conn, ["EXPIREAT", "lease", Integer.to_string(later), "XX"])

    assert {{:error, "ERR NX and XX, GT or LT options at the same time are not compatible"}, conn} =
             Client.command(conn, ["EXPIREAT", "lease", Integer.to_string(base), "NX", "XX"])

    assert {{:error, "ERR Unsupported option SOON"}, _conn} =
             Client.command(conn, ["EXPIREAT", "lease", Integer.to_string(base), "soon"])
  end

  test "OBJECT reports encoding, refcount, idletime and help", %{conn: conn} do
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "number", "12345"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "shared", "42"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "padded", "007"])
    assert {{:simple, "OK"}, conn} = Client.command(conn, ["SET", "short", "hello"])

    assert {{:simple, "OK"}, conn} =
             Client.command(conn, ["SET", "long", String.duplicate("x", 60)])

    assert {"int", conn} = Client.command(conn, ["OBJECT", "ENCODING", "number"])
    assert {"embstr", conn} = Client.command(conn, ["OBJECT", "ENCODING", "padded"])
    assert {"embstr", conn} = Client.command(conn, ["OBJECT", "ENCODING", "short"])
    assert {"raw", conn} = Client.command(conn, ["OBJECT", "ENCODING", "long"])

    assert {1, conn} = Client.command(conn, ["RPUSH", "smallList", "a"])
    assert {"listpack", conn} = Client.command(conn, ["OBJECT", "ENCODING", "smallList"])

    bigList = ["RPUSH", "bigList"] ++ Enum.map(1..200, &Integer.to_string/1)
    assert {200, conn} = Client.command(conn, bigList)
    assert {"quicklist", conn} = Client.command(conn, ["OBJECT", "ENCODING", "bigList"])

    assert {2, conn} = Client.command(conn, ["SADD", "ints", "1", "2"])
    assert {"intset", conn} = Client.command(conn, ["OBJECT", "ENCODING", "ints"])

    assert {2, conn} = Client.command(conn, ["SADD", "words", "alpha", "beta"])
    assert {"listpack", conn} = Client.command(conn, ["OBJECT", "ENCODING", "words"])

    assert {1, conn} = Client.command(conn, ["HSET", "hash", "field", "value"])
    assert {"listpack", conn} = Client.command(conn, ["OBJECT", "ENCODING", "hash"])

    assert {1, conn} = Client.command(conn, ["ZADD", "zset", "1", "a"])
    assert {"listpack", conn} = Client.command(conn, ["OBJECT", "ENCODING", "zset"])

    assert {1, conn} = Client.command(conn, ["OBJECT", "REFCOUNT", "short"])
    assert {2_147_483_647, conn} = Client.command(conn, ["OBJECT", "REFCOUNT", "shared"])
    assert {0, conn} = Client.command(conn, ["OBJECT", "IDLETIME", "short"])

    assert {{:error, "ERR no such key"}, conn} =
             Client.command(conn, ["OBJECT", "ENCODING", "ghost"])

    assert {{:error, "ERR no such key"}, conn} =
             Client.command(conn, ["OBJECT", "REFCOUNT", "ghost"])

    {help, conn} = Client.command(conn, ["OBJECT", "HELP"])
    assert Enum.any?(help, &match?({:simple, "ENCODING <key>"}, &1))

    assert {{:error, "ERR Unknown subcommand" <> _}, _conn} =
             Client.command(conn, ["OBJECT", "SIZE", "short"])
  end

