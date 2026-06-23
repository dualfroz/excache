defmodule Excache.Command do
  @moduledoc false

  alias Excache.{Blocking, PubSub, Store, Persistence, Aof, Notify}
  alias Excache.Command.{Reply, Transaction}
  alias Excache.Eviction.Runner

  import Reply,
    only: [
      reply: 2,
      with_int: 3,
      with_two_ints: 4,
      map_get: 1,
      map_int: 1,
      map_score: 1,
      map_list: 1,
      map_pop: 2,
      zrange_reply: 2,
      bulks: 1,
      bulks_nullable: 1,
      pairs: 1,
      parse_set_opts: 2,
      parse_zadd_args: 1,
      parse_bound: 1
    ]

  @known ~w(
    PING ECHO COMMAND DBSIZE FLUSHDB SELECT QUIT
    GET SET DEL EXISTS KEYS TYPE INCR DECR INCRBY DECRBY APPEND GETSET MSET MGET
    EXPIRE PEXPIRE TTL PTTL PERSIST
    LPUSH RPUSH LPOP RPOP LRANGE LLEN LINDEX
    HSET HGET HDEL HGETALL HKEYS HVALS HLEN HEXISTS HINCRBY
    SADD SREM SMEMBERS SISMEMBER SCARD SINTER SUNION SDIFF
    SINTERSTORE SUNIONSTORE SDIFFSTORE
    ZADD ZSCORE ZRANK ZINCRBY ZREM ZCARD ZRANGE ZRANGEBYSCORE ZPOPMIN ZPOPMAX
    MULTI EXEC DISCARD WATCH UNWATCH
    SUBSCRIBE UNSUBSCRIBE PUBLISH
    SAVE BGSAVE
  )

  @write_commands ~w(
    SET DEL EXPIRE PEXPIRE PERSIST INCR DECR INCRBY DECRBY APPEND GETSET MSET FLUSHDB
    LPUSH RPUSH LPOP RPOP
    HSET HDEL HINCRBY
    SADD SREM SINTERSTORE SUNIONSTORE SDIFFSTORE
    ZADD ZREM ZINCRBY ZPOPMIN ZPOPMAX
  )

  @oomDenied ~w(
    SET SETNX SETRANGE APPEND GETSET MSET INCR INCRBY DECR DECRBY INCRBYFLOAT
    LPUSH RPUSH LINSERT LSET RPOPLPUSH BLMOVE BRPOPLPUSH
    HSET HSETNX HINCRBY
    SADD SMOVE SINTERSTORE SUNIONSTORE SDIFFSTORE
    ZADD ZINCRBY
    XADD XSETID XGROUP XREADGROUP XCLAIM XAUTOCLAIM
    SETBIT BITOP BITFIELD PFADD PFMERGE
    COPY RENAME RENAMENX
  )

  @oom {:error, "OOM command not allowed when used memory > 'maxmemory'"}

  def known_commands, do: @known ++ Excache.Command.Extended.command_names()

  @extended_write Excache.Command.Extended.write_command_names()

  @blockingNames Excache.Command.Blocking.commandNames()

  def execute([], state), do: {[], state}

  def execute([name | args], state) do
    Transaction.execute(String.upcase(name), args, state, &dispatch_and_log/3)
  end

  def replay_dispatch(command, args) do
    dispatch(command, args, replay_state())
    :ok
  end

  def dispatch_and_log(command, args, state) do
    if oomDenied?(command) and Runner.admit() == :oom do
      {[@oom], state}
    else
      logged(command, args, state)
    end
  end

  defp logged(command, args, state) do
    {replies, updated} = dispatch(command, args, state)
    succeeded = not Enum.any?(replies, &match?({:error, _}, &1))

    if succeeded and oomDenied?(command), do: Runner.account(args)

    entries = if succeeded, do: aofEntries(command, args, replies), else: []
    logWrites(entries)
    logWrites(servedWrites(command, wakeKeys(entries)))

    {replies, updated}
  end

  defp logWrites(entries) do
    Enum.each(entries, fn {name, args} ->
      if writeCommand?(name) do
        Aof.append(name, args)
        Notify.for_command(name, args)
      end
    end)
  end

  defp servedWrites(command, keys) do
    if command in @blockingNames, do: Blocking.takeWrites(), else: Blocking.signal(keys)
  end

  defp wakeKeys(entries), do: Enum.flat_map(entries, &listTargets/1)

  defp listTargets({"LPUSH", [key | _values]}), do: [key]
  defp listTargets({"RPUSH", [key | _values]}), do: [key]
  defp listTargets({"LINSERT", [key | _rest]}), do: [key]
  defp listTargets({"RPOPLPUSH", [_source, destination]}), do: [destination]
  defp listTargets({"RENAME", [_source, destination]}), do: [destination]
  defp listTargets({"RENAMENX", [_source, destination]}), do: [destination]
  defp listTargets({"COPY", [_source, destination | _options]}), do: [destination]
  defp listTargets(_entry), do: []

  defp writeCommand?(command), do: command in @write_commands or command in @extended_write

  defp oomDenied?(command), do: command in @oomDenied

  defp aofEntries("XADD", args, [{:bulk, id}]) when is_binary(id),
    do: [{"XADD", Excache.Command.Streams.rewriteAddId(args, id)}]

  defp aofEntries("SPOP", [key | _rest] = args, replies) do
    case poppedMembers(replies) do
      [] -> [{"SPOP", args}]
      members -> [{"SREM", [key | members]}]
    end
  end

  defp aofEntries(command, args, _replies), do: [{command, args}]

  defp poppedMembers([{:bulk, member}]) when is_binary(member), do: [member]

  defp poppedMembers([{:array, entries}]),
    do: for({:bulk, member} <- entries, is_binary(member), do: member)

  defp poppedMembers(_replies), do: []

  defp dispatch("PING", [], state), do: {[{:simple, "PONG"}], state}
  defp dispatch("PING", [message], state), do: {[{:bulk, message}], state}
  defp dispatch("ECHO", [message], state), do: {[{:bulk, message}], state}
  defp dispatch("SELECT", [_db], state), do: {[{:simple, "OK"}], state}
  defp dispatch("QUIT", [], state), do: {[{:simple, "OK"}], state}
  defp dispatch("DBSIZE", [], state), do: {[{:integer, Store.dbsize()}], state}

  defp dispatch("FLUSHDB", _args, state) do
    Store.flushdb()
    {[{:simple, "OK"}], state}
  end

  defp dispatch("GET", [key], state), do: reply(map_get(Store.get(key)), state)

  defp dispatch("SET", [key, value | rest], state) do
    case parse_set_opts(rest, []) do
      {:ok, opts} ->
        reply(
          case Store.set(key, value, opts) do
            :ok -> {:simple, "OK"}
            nil -> {:bulk, nil}
          end,
          state
        )

      :error ->
        {[Reply.syntax()], state}
    end
  end

  defp dispatch("DEL", [_ | _] = keys, state), do: {[{:integer, Store.del(keys)}], state}
  defp dispatch("EXISTS", [_ | _] = keys, state), do: {[{:integer, Store.exists(keys)}], state}
  defp dispatch("KEYS", [pattern], state), do: {[{:array, bulks(Store.keys(pattern))}], state}
  defp dispatch("TYPE", [key], state), do: {[{:simple, Store.type(key)}], state}

  defp dispatch("INCR", [key], state), do: reply(map_int(Store.incrby(key, 1)), state)
  defp dispatch("DECR", [key], state), do: reply(map_int(Store.incrby(key, -1)), state)

  defp dispatch("INCRBY", [key, delta], state) do
    with_int(delta, state, fn amount -> map_int(Store.incrby(key, amount)) end)
  end

  defp dispatch("DECRBY", [key, delta], state) do
    with_int(delta, state, fn amount -> map_int(Store.incrby(key, -amount)) end)
  end

  defp dispatch("APPEND", [key, value], state),
    do: reply(map_int(Store.append(key, value)), state)

  defp dispatch("GETSET", [key, value], state),
    do: reply(map_get(Store.getset(key, value)), state)

  defp dispatch("MSET", args, state) when args != [] do
    if rem(length(args), 2) == 0 do
      Store.mset(pairs(args))
      {[{:simple, "OK"}], state}
    else
      {[{:error, "ERR wrong number of arguments for 'mset' command"}], state}
    end
  end

  defp dispatch("MGET", [_ | _] = keys, state) do
    {[{:array, bulks_nullable(Store.mget(keys))}], state}
  end

  defp dispatch("EXPIRE", [key, seconds], state) do
    with_int(seconds, state, fn secs -> {:integer, Store.expire(key, secs * 1000)} end)
  end

  defp dispatch("PEXPIRE", [key, ms], state) do
    with_int(ms, state, fn millis -> {:integer, Store.expire(key, millis)} end)
  end

  defp dispatch("TTL", [key], state), do: {[{:integer, Store.ttl(key, :seconds)}], state}
  defp dispatch("PTTL", [key], state), do: {[{:integer, Store.ttl(key, :milliseconds)}], state}
  defp dispatch("PERSIST", [key], state), do: {[{:integer, Store.persist(key)}], state}

  defp dispatch("LPUSH", [key | [_ | _] = values], state),
    do: reply(map_int(Store.lpush(key, values)), state)

  defp dispatch("RPUSH", [key | [_ | _] = values], state),
    do: reply(map_int(Store.rpush(key, values)), state)

  defp dispatch("LPOP", [key], state), do: reply(map_pop(Store.lpop(key, nil), nil), state)

  defp dispatch("LPOP", [key, count], state) do
    with_int(count, state, fn n -> map_pop(Store.lpop(key, n), n) end)
  end

  defp dispatch("RPOP", [key], state), do: reply(map_pop(Store.rpop(key, nil), nil), state)

  defp dispatch("RPOP", [key, count], state) do
    with_int(count, state, fn n -> map_pop(Store.rpop(key, n), n) end)
  end

  defp dispatch("LRANGE", [key, start, stop], state) do
    with_two_ints(start, stop, state, fn lo, hi -> map_list(Store.lrange(key, lo, hi)) end)
  end

  defp dispatch("LLEN", [key], state), do: reply(map_int(Store.llen(key)), state)

  defp dispatch("LINDEX", [key, index], state) do
    with_int(index, state, fn i -> map_get(Store.lindex(key, i)) end)
  end

  defp dispatch("HSET", [key | [_, _ | _] = rest], state) do
    if rem(length(rest), 2) == 0 do
      reply(map_int(Store.hset(key, pairs(rest))), state)
    else
      {[{:error, "ERR wrong number of arguments for 'hset' command"}], state}
    end
  end

  defp dispatch("HGET", [key, field], state), do: reply(map_get(Store.hget(key, field)), state)

  defp dispatch("HDEL", [key | [_ | _] = fields], state),
    do: reply(map_int(Store.hdel(key, fields)), state)

  defp dispatch("HGETALL", [key], state), do: reply(map_list(Store.hgetall(key)), state)
  defp dispatch("HKEYS", [key], state), do: reply(map_list(Store.hkeys(key)), state)
  defp dispatch("HVALS", [key], state), do: reply(map_list(Store.hvals(key)), state)
  defp dispatch("HLEN", [key], state), do: reply(map_int(Store.hlen(key)), state)

  defp dispatch("HEXISTS", [key, field], state),
    do: reply(map_int(Store.hexists(key, field)), state)

  defp dispatch("HINCRBY", [key, field, delta], state) do
    with_int(delta, state, fn amount -> map_int(Store.hincrby(key, field, amount)) end)
  end

  defp dispatch("SADD", [key | [_ | _] = members], state),
    do: reply(map_int(Store.sadd(key, members)), state)

  defp dispatch("SREM", [key | [_ | _] = members], state),
    do: reply(map_int(Store.srem(key, members)), state)

  defp dispatch("SMEMBERS", [key], state), do: reply(map_list(Store.smembers(key)), state)

  defp dispatch("SISMEMBER", [key, member], state),
    do: reply(map_int(Store.sismember(key, member)), state)

  defp dispatch("SCARD", [key], state), do: reply(map_int(Store.scard(key)), state)

  defp dispatch("SINTER", [_ | _] = keys, state), do: reply(map_list(Store.sinter(keys)), state)
  defp dispatch("SUNION", [_ | _] = keys, state), do: reply(map_list(Store.sunion(keys)), state)
  defp dispatch("SDIFF", [_ | _] = keys, state), do: reply(map_list(Store.sdiff(keys)), state)

  defp dispatch("SINTERSTORE", [dest | [_ | _] = keys], state),
    do: reply(map_int(Store.sinterstore(dest, keys)), state)

  defp dispatch("SUNIONSTORE", [dest | [_ | _] = keys], state),
    do: reply(map_int(Store.sunionstore(dest, keys)), state)

  defp dispatch("SDIFFSTORE", [dest | [_ | _] = keys], state),
    do: reply(map_int(Store.sdiffstore(dest, keys)), state)

  defp dispatch("ZADD", [key | [_ | _] = rest], state) do
    case parse_zadd_args(rest) do
      {:ok, opts, entries} -> reply(map_int(Store.zadd(key, entries, opts)), state)
      :error -> {[Reply.not_float()], state}
    end
  end

  defp dispatch("ZSCORE", [key, member], state),
    do: reply(map_score(Store.zscore(key, member)), state)

  defp dispatch("ZRANK", [key, member], state),
    do: reply(map_int(Store.zrank(key, member)), state)

  defp dispatch("ZINCRBY", [key, delta, member], state) do
    case Reply.parse_score(delta) do
      {:ok, amount} -> reply(map_score(Store.zincrby(key, amount, member)), state)
      :error -> {[Reply.not_float()], state}
    end
  end

  defp dispatch("ZREM", [key | [_ | _] = members], state),
    do: reply(map_int(Store.zrem(key, members)), state)

  defp dispatch("ZCARD", [key], state), do: reply(map_int(Store.zcard(key)), state)

  defp dispatch("ZRANGE", [key, start, stop], state) do
    with_two_ints(start, stop, state, fn lo, hi ->
      zrange_reply(Store.zrange(key, lo, hi), false)
    end)
  end

  defp dispatch("ZRANGE", [key, start, stop, flag], state) do
    if String.upcase(flag) == "WITHSCORES" do
      with_two_ints(start, stop, state, fn lo, hi ->
        zrange_reply(Store.zrange(key, lo, hi), true)
      end)
    else
      {[Reply.syntax()], state}
    end
  end

  defp dispatch("ZRANGEBYSCORE", [key, min, max], state),
    do: zrangebyscore(key, min, max, false, state)

  defp dispatch("ZRANGEBYSCORE", [key, min, max, flag], state) do
    if String.upcase(flag) == "WITHSCORES" do
      zrangebyscore(key, min, max, true, state)
    else
      {[Reply.syntax()], state}
    end
  end

  defp dispatch("ZPOPMIN", [key], state),
    do: reply(zrange_reply(Store.zpopmin(key, 1), true), state)

  defp dispatch("ZPOPMIN", [key, count], state) do
    with_int(count, state, fn n -> zrange_reply(Store.zpopmin(key, n), true) end)
  end

  defp dispatch("ZPOPMAX", [key], state),
    do: reply(zrange_reply(Store.zpopmax(key, 1), true), state)

  defp dispatch("ZPOPMAX", [key, count], state) do
    with_int(count, state, fn n -> zrange_reply(Store.zpopmax(key, n), true) end)
  end

  defp dispatch("SUBSCRIBE", [_ | _] = channels, state) do
    Enum.reduce(channels, {[], state}, fn channel, {replies, current} ->
      updated =
        if MapSet.member?(current.subscriptions, channel) do
          current
        else
          PubSub.subscribe(channel)
          %{current | subscriptions: MapSet.put(current.subscriptions, channel)}
        end

      confirm =
        {:array,
         [{:bulk, "subscribe"}, {:bulk, channel}, {:integer, MapSet.size(updated.subscriptions)}]}

      {replies ++ [confirm], updated}
    end)
  end

  defp dispatch("UNSUBSCRIBE", [], state) do
    case MapSet.to_list(state.subscriptions) do
      [] ->
        {[{:array, [{:bulk, "unsubscribe"}, {:bulk, nil}, {:integer, 0}]}], state}

      channels ->
        unsubscribe_each(channels, state)
    end
  end

  defp dispatch("UNSUBSCRIBE", channels, state), do: unsubscribe_each(channels, state)

  defp dispatch("PUBLISH", [channel, payload], state) do
    {[{:integer, PubSub.publish(channel, payload)}], state}
  end

  defp dispatch("SAVE", [], state) do
    Persistence.save()
    {[{:simple, "OK"}], state}
  end

  defp dispatch("BGSAVE", [], state) do
    Task.start(fn -> Persistence.save() end)
    {[{:simple, "Background saving started"}], state}
  end

  defp dispatch(name, args, state) do
    case Excache.Command.Extended.dispatch(name, args, state) do
      :unhandled -> unknownOrArity(name, state)
      result -> result
    end
  end

  defp unknownOrArity(name, state) do
    if name in known_commands() do
      {[{:error, "ERR wrong number of arguments for '#{String.downcase(name)}' command"}], state}
    else
      {[{:error, "ERR unknown command '#{name}'"}], state}
    end
  end

  defp unsubscribe_each(channels, state) do
    Enum.reduce(channels, {[], state}, fn channel, {replies, current} ->
      updated =
        if MapSet.member?(current.subscriptions, channel) do
          PubSub.unsubscribe(channel)
          %{current | subscriptions: MapSet.delete(current.subscriptions, channel)}
        else
          current
        end

      confirm =
        {:array,
         [
           {:bulk, "unsubscribe"},
           {:bulk, channel},
           {:integer, MapSet.size(updated.subscriptions)}
         ]}

      {replies ++ [confirm], updated}
    end)
  end

  defp zrangebyscore(key, min, max, withscores, state) do
    with {:ok, lo} <- parse_bound(min), {:ok, hi} <- parse_bound(max) do
      reply(zrange_reply(Store.zrangebyscore(key, lo, hi), withscores), state)
    else
      :error -> {[Reply.not_float()], state}
    end
  end

  defp replay_state, do: Map.put(Transaction.initial_state(), :subscriptions, MapSet.new())
end
