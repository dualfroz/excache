defmodule Excache.Command.Introspection.Catalog do
  @moduledoc false

  # {arity, flags, firstKey, lastKey, step, group}
  # Arity counts the command name, negative means "at least this many".
  # firstKey/lastKey/step are 1 based positions in the full argument vector,
  # a negative lastKey counts back from the end, zeroes mean "takes no keys".

  @commands %{
    "PING" => {-1, [:fast], 0, 0, 0, :connection},
    "ECHO" => {2, [:fast], 0, 0, 0, :connection},
    "QUIT" => {-1, [:noscript, :loading, :stale, :fast], 0, 0, 0, :connection},
    "SELECT" => {2, [:loading, :stale, :fast], 0, 0, 0, :connection},
    "CLIENT" => {-2, [:admin, :noscript, :loading, :stale], 0, 0, 0, :connection},
    "COMMAND" => {-1, [:loading, :stale], 0, 0, 0, :server},
    "INFO" => {-1, [:loading, :stale], 0, 0, 0, :server},
    "CONFIG" => {-2, [:admin, :noscript, :loading, :stale], 0, 0, 0, :server},
    "MEMORY" => {-2, [:readonly], 0, 0, 0, :server},
    "DBSIZE" => {1, [:readonly, :fast], 0, 0, 0, :server},
    "FLUSHDB" => {-1, [:write], 0, 0, 0, :server},
    "SAVE" => {1, [:admin, :noscript], 0, 0, 0, :server},
    "BGSAVE" => {-1, [:admin, :noscript], 0, 0, 0, :server},
    "GET" => {2, [:readonly, :fast], 1, 1, 1, :string},
    "SET" => {-3, [:write, :denyoom], 1, 1, 1, :string},
    "SETNX" => {3, [:write, :denyoom, :fast], 1, 1, 1, :string},
    "GETSET" => {3, [:write, :denyoom, :fast], 1, 1, 1, :string},
    "GETDEL" => {2, [:write, :fast], 1, 1, 1, :string},
    "APPEND" => {3, [:write, :denyoom, :fast], 1, 1, 1, :string},
    "STRLEN" => {2, [:readonly, :fast], 1, 1, 1, :string},
    "SETRANGE" => {4, [:write, :denyoom], 1, 1, 1, :string},
    "GETRANGE" => {4, [:readonly], 1, 1, 1, :string},
    "INCR" => {2, [:write, :denyoom, :fast], 1, 1, 1, :string},
    "DECR" => {2, [:write, :denyoom, :fast], 1, 1, 1, :string},
    "INCRBY" => {3, [:write, :denyoom, :fast], 1, 1, 1, :string},
    "DECRBY" => {3, [:write, :denyoom, :fast], 1, 1, 1, :string},
    "INCRBYFLOAT" => {3, [:write, :denyoom, :fast], 1, 1, 1, :string},
    "MSET" => {-3, [:write, :denyoom], 1, -1, 2, :string},
    "MGET" => {-2, [:readonly, :fast], 1, -1, 1, :string},
    "DEL" => {-2, [:write], 1, -1, 1, :generic},
    "UNLINK" => {-2, [:write, :fast], 1, -1, 1, :generic},
    "EXISTS" => {-2, [:readonly, :fast], 1, -1, 1, :generic},
    "TOUCH" => {-2, [:readonly, :fast], 1, -1, 1, :generic},
    "KEYS" => {2, [:readonly], 0, 0, 0, :generic},
    "SCAN" => {-2, [:readonly], 0, 0, 0, :generic},
    "TYPE" => {2, [:readonly, :fast], 1, 1, 1, :generic},
    "OBJECT" => {-2, [:readonly], 0, 0, 0, :generic},
    "RENAME" => {3, [:write], 1, 2, 1, :generic},
    "RENAMENX" => {3, [:write, :fast], 1, 2, 1, :generic},
    "COPY" => {-3, [:write, :denyoom], 1, 2, 1, :generic},
    "EXPIRE" => {-3, [:write, :fast], 1, 1, 1, :generic},
    "PEXPIRE" => {-3, [:write, :fast], 1, 1, 1, :generic},
    "EXPIREAT" => {-3, [:write, :fast], 1, 1, 1, :generic},
    "EXPIRETIME" => {2, [:readonly, :fast], 1, 1, 1, :generic},
    "PEXPIRETIME" => {2, [:readonly, :fast], 1, 1, 1, :generic},
    "TTL" => {2, [:readonly, :fast], 1, 1, 1, :generic},
    "PTTL" => {2, [:readonly, :fast], 1, 1, 1, :generic},
    "PERSIST" => {2, [:write, :fast], 1, 1, 1, :generic},
    "LPUSH" => {-3, [:write, :denyoom, :fast], 1, 1, 1, :list},
    "RPUSH" => {-3, [:write, :denyoom, :fast], 1, 1, 1, :list},
    "LPOP" => {-2, [:write, :fast], 1, 1, 1, :list},
    "RPOP" => {-2, [:write, :fast], 1, 1, 1, :list},
    "LRANGE" => {4, [:readonly], 1, 1, 1, :list},
    "LLEN" => {2, [:readonly, :fast], 1, 1, 1, :list},
    "LINDEX" => {3, [:readonly], 1, 1, 1, :list},
    "LSET" => {4, [:write, :denyoom], 1, 1, 1, :list},
    "LREM" => {4, [:write], 1, 1, 1, :list},
    "LTRIM" => {4, [:write], 1, 1, 1, :list},
    "LINSERT" => {5, [:write, :denyoom], 1, 1, 1, :list},
    "LPOS" => {-3, [:readonly], 1, 1, 1, :list},
    "RPOPLPUSH" => {3, [:write, :denyoom], 1, 2, 1, :list},
    "BLPOP" => {-3, [:write, :noscript, :blocking], 1, -2, 1, :list},
    "BRPOP" => {-3, [:write, :noscript, :blocking], 1, -2, 1, :list},
    "BLMOVE" => {6, [:write, :denyoom, :noscript, :blocking], 1, 2, 1, :list},
    "BRPOPLPUSH" => {4, [:write, :denyoom, :noscript, :blocking], 1, 2, 1, :list},
    "BLMPOP" => {-5, [:write, :noscript, :blocking, :movablekeys], 0, 0, 0, :list},
    "HSET" => {-4, [:write, :denyoom, :fast], 1, 1, 1, :hash},
    "HSETNX" => {4, [:write, :denyoom, :fast], 1, 1, 1, :hash},
    "HGET" => {3, [:readonly, :fast], 1, 1, 1, :hash},
    "HMGET" => {-3, [:readonly, :fast], 1, 1, 1, :hash},
    "HDEL" => {-3, [:write, :fast], 1, 1, 1, :hash},
    "HGETALL" => {2, [:readonly], 1, 1, 1, :hash},
    "HKEYS" => {2, [:readonly], 1, 1, 1, :hash},
    "HVALS" => {2, [:readonly], 1, 1, 1, :hash},
    "HLEN" => {2, [:readonly, :fast], 1, 1, 1, :hash},
    "HEXISTS" => {3, [:readonly, :fast], 1, 1, 1, :hash},
    "HINCRBY" => {4, [:write, :denyoom, :fast], 1, 1, 1, :hash},
    "HSCAN" => {-3, [:readonly], 1, 1, 1, :hash},
    "SADD" => {-3, [:write, :denyoom, :fast], 1, 1, 1, :set},
    "SREM" => {-3, [:write, :fast], 1, 1, 1, :set},
    "SMEMBERS" => {2, [:readonly], 1, 1, 1, :set},
    "SISMEMBER" => {3, [:readonly, :fast], 1, 1, 1, :set},
    "SMISMEMBER" => {-3, [:readonly, :fast], 1, 1, 1, :set},
    "SCARD" => {2, [:readonly, :fast], 1, 1, 1, :set},
    "SPOP" => {-2, [:write, :fast], 1, 1, 1, :set},
    "SRANDMEMBER" => {-2, [:readonly], 1, 1, 1, :set},
    "SMOVE" => {4, [:write, :fast], 1, 2, 1, :set},
    "SINTER" => {-2, [:readonly], 1, -1, 1, :set},
    "SUNION" => {-2, [:readonly], 1, -1, 1, :set},
    "SDIFF" => {-2, [:readonly], 1, -1, 1, :set},
    "SINTERCARD" => {-3, [:readonly, :movablekeys], 0, 0, 0, :set},
    "SINTERSTORE" => {-3, [:write, :denyoom], 1, -1, 1, :set},
    "SUNIONSTORE" => {-3, [:write, :denyoom], 1, -1, 1, :set},
    "SDIFFSTORE" => {-3, [:write, :denyoom], 1, -1, 1, :set},
    "SSCAN" => {-3, [:readonly], 1, 1, 1, :set},
    "ZADD" => {-4, [:write, :denyoom, :fast], 1, 1, 1, :zset},
    "ZINCRBY" => {4, [:write, :denyoom, :fast], 1, 1, 1, :zset},
    "ZREM" => {-3, [:write, :fast], 1, 1, 1, :zset},
    "ZSCORE" => {3, [:readonly, :fast], 1, 1, 1, :zset},
    "ZMSCORE" => {-3, [:readonly, :fast], 1, 1, 1, :zset},
    "ZRANK" => {3, [:readonly, :fast], 1, 1, 1, :zset},
    "ZREVRANK" => {3, [:readonly, :fast], 1, 1, 1, :zset},
    "ZCARD" => {2, [:readonly, :fast], 1, 1, 1, :zset},
    "ZCOUNT" => {4, [:readonly, :fast], 1, 1, 1, :zset},
    "ZRANGE" => {-4, [:readonly], 1, 1, 1, :zset},
    "ZREVRANGE" => {-4, [:readonly], 1, 1, 1, :zset},
    "ZRANGEBYSCORE" => {-4, [:readonly], 1, 1, 1, :zset},
    "ZPOPMIN" => {-2, [:write, :fast], 1, 1, 1, :zset},
    "ZPOPMAX" => {-2, [:write, :fast], 1, 1, 1, :zset},
    "ZSCAN" => {-3, [:readonly], 1, 1, 1, :zset},
    "XADD" => {-5, [:write, :denyoom, :fast], 1, 1, 1, :stream},
    "XLEN" => {2, [:readonly, :fast], 1, 1, 1, :stream},
    "XRANGE" => {-4, [:readonly], 1, 1, 1, :stream},
    "XREVRANGE" => {-4, [:readonly], 1, 1, 1, :stream},
    "XDEL" => {-3, [:write, :fast], 1, 1, 1, :stream},
    "XTRIM" => {-4, [:write], 1, 1, 1, :stream},
    "XSETID" => {-3, [:write, :denyoom, :fast], 1, 1, 1, :stream},
    "XACK" => {-4, [:write, :fast], 1, 1, 1, :stream},
    "XPENDING" => {-3, [:readonly], 1, 1, 1, :stream},
    "XCLAIM" => {-6, [:write, :fast], 1, 1, 1, :stream},
    "XAUTOCLAIM" => {-7, [:write, :fast], 1, 1, 1, :stream},
    "XREAD" => {-4, [:readonly, :blocking, :movablekeys], 0, 0, 0, :stream},
    "XREADGROUP" => {-7, [:write, :blocking, :movablekeys], 0, 0, 0, :stream},
    "XINFO" => {-2, [:readonly], 0, 0, 0, :stream},
    "XGROUP" => {-2, [:write, :denyoom], 0, 0, 0, :stream},
    "SETBIT" => {4, [:write, :denyoom], 1, 1, 1, :bitmap},
    "GETBIT" => {3, [:readonly, :fast], 1, 1, 1, :bitmap},
    "BITCOUNT" => {-2, [:readonly], 1, 1, 1, :bitmap},
    "BITPOS" => {-3, [:readonly], 1, 1, 1, :bitmap},
    "BITFIELD" => {-2, [:write, :denyoom], 1, 1, 1, :bitmap},
    "BITOP" => {-4, [:write, :denyoom], 2, -1, 1, :bitmap},
    "PFADD" => {-2, [:write, :denyoom, :fast], 1, 1, 1, :hyperloglog},
    "PFCOUNT" => {-2, [:readonly], 1, -1, 1, :hyperloglog},
    "PFMERGE" => {-2, [:write, :denyoom], 1, -1, 1, :hyperloglog},
    "SUBSCRIBE" => {-2, [:pubsub, :noscript, :loading, :stale, :fast], 0, 0, 0, :pubsub},
    "UNSUBSCRIBE" => {-1, [:pubsub, :noscript, :loading, :stale, :fast], 0, 0, 0, :pubsub},
    "PUBLISH" => {3, [:pubsub, :loading, :stale, :fast], 0, 0, 0, :pubsub},
    "MULTI" => {1, [:noscript, :loading, :stale, :fast], 0, 0, 0, :transactions},
    "EXEC" => {1, [:noscript, :loading, :stale], 0, 0, 0, :transactions},
    "DISCARD" => {1, [:noscript, :loading, :stale, :fast], 0, 0, 0, :transactions},
    "WATCH" => {-2, [:noscript, :loading, :stale, :fast], 1, -1, 1, :transactions},
    "UNWATCH" => {1, [:noscript, :loading, :stale, :fast], 0, 0, 0, :transactions}
  }

  @keyRules %{
    "XREAD" => :streams,
    "XREADGROUP" => :streams,
    "SINTERCARD" => {:numkeys, 1},
    "BLMPOP" => {:numkeys, 2},
    "OBJECT" => {:subcommand, ~w(ENCODING REFCOUNT FREQ IDLETIME)},
    "MEMORY" => {:subcommand, ~w(USAGE)},
    "XINFO" => {:subcommand, ~w(STREAM GROUPS CONSUMERS)},
    "XGROUP" => {:subcommand, ~w(CREATE SETID DESTROY CREATECONSUMER DELCONSUMER)}
  }

  def tableNames, do: Map.keys(@commands)

  def names, do: Excache.Command.known_commands() |> Enum.uniq() |> Enum.sort()

  def known?(name), do: Map.has_key?(@commands, name)

  def count, do: length(names())

  def infoTerm(name) do
    case Map.fetch(@commands, name) do
      {:ok, {arity, flags, firstKey, lastKey, step, _group}} ->
        {:array,
         [
           {:bulk, String.downcase(name)},
           {:integer, arity},
           {:array, Enum.map(flags, fn flag -> {:simple, Atom.to_string(flag)} end)},
           {:integer, firstKey},
           {:integer, lastKey},
           {:integer, step}
         ]}

      :error ->
        {:array, nil}
    end
  end

  def docsTerm(name) do
    {arity, _flags, _firstKey, _lastKey, _step, group} = Map.fetch!(@commands, name)

    {:array,
     [
       {:bulk, "group"},
       {:bulk, Atom.to_string(group)},
       {:bulk, "arity"},
       {:integer, arity}
     ]}
  end

  def arityOk?(name, argc) do
    {arity, _flags, _firstKey, _lastKey, _step, _group} = Map.fetch!(@commands, name)
    if arity < 0, do: argc >= -arity, else: argc == arity
  end

  def keysFor(name, args) do
    case Map.get(@keyRules, name) do
      nil -> rangeKeys(name, args)
      rule -> ruleKeys(rule, args)
    end
  end

  defp rangeKeys(name, args) do
    {_arity, _flags, firstKey, lastKey, step, _group} = Map.fetch!(@commands, name)

    if firstKey == 0 do
      {:ok, []}
    else
      count = length(args)
      from = firstKey - 1
      to = if lastKey < 0, do: count + lastKey, else: lastKey - 1

      if to < from do
        {:ok, []}
      else
        {:ok, args |> Enum.slice(from..to) |> Enum.take_every(step)}
      end
    end
  end

  defp ruleKeys(:streams, args) do
    case Enum.find_index(args, fn arg -> String.upcase(arg) == "STREAMS" end) do
      nil ->
        :error

      index ->
        tail = Enum.drop(args, index + 1)
        half = div(length(tail), 2)
        if half == 0 or rem(length(tail), 2) == 1, do: :error, else: {:ok, Enum.take(tail, half)}
    end
  end

  defp ruleKeys({:numkeys, position}, args) do
    with {numkeys, ""} when numkeys > 0 <- Integer.parse(Enum.at(args, position - 1, "")),
         keys when length(keys) == numkeys <- args |> Enum.drop(position) |> Enum.take(numkeys) do
      {:ok, keys}
    else
      _invalid -> :error
    end
  end

  defp ruleKeys({:subcommand, takesKey}, [subcommand, key | _rest]) do
    if String.upcase(subcommand) in takesKey, do: {:ok, [key]}, else: {:ok, []}
  end

  defp ruleKeys({:subcommand, _takesKey}, _args), do: {:ok, []}
end
