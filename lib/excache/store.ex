defmodule Excache.Store do
  @moduledoc false

  alias Excache.{Bitmap, HyperLogLog}
  alias Excache.Config
  alias Excache.Eviction.Tracker

  def shard_count, do: Application.get_env(:excache, :shard_count, 8)

  def shard_name(index), do: :"Excache.Store.Shard.#{index}"

  defp shard_for(key), do: shard_name(rem(:erlang.phash2(key), shard_count()))

  defp call_key(key, op) do
    trackAccess(key, op)
    GenServer.call(shard_for(key), {:op, op})
  end

  defp trackAccess(_key, {:del, _target}), do: :ok
  defp trackAccess(_key, {:peek, _target}), do: :ok

  defp trackAccess(key, _op) do
    if Config.tracksAccess?() do
      Tracker.touchAsync(Tracker, key, System.system_time(:millisecond))
    end

    :ok
  end

  defp fan_out(op) do
    for index <- 0..(shard_count() - 1) do
      GenServer.call(shard_name(index), {:op, op})
    end
  end

  def get(key), do: call_key(key, {:get, key})
  def set(key, value, opts), do: call_key(key, {:set, key, value, opts})
  def type(key), do: call_key(key, {:type, key})
  def incrby(key, delta), do: call_key(key, {:incrby, key, delta})
  def append(key, value), do: call_key(key, {:append, key, value})
  def getset(key, value), do: call_key(key, {:getset, key, value})

  def del(keys), do: Enum.reduce(keys, 0, fn key, acc -> acc + call_key(key, {:del, key}) end)

  def exists(keys),
    do: Enum.reduce(keys, 0, fn key, acc -> acc + call_key(key, {:exists, key}) end)

  def mset(pairs) do
    Enum.each(pairs, fn {key, value} -> call_key(key, {:set, key, value, []}) end)
    :ok
  end

  def mget(keys) do
    Enum.map(keys, fn key ->
      case call_key(key, {:get, key}) do
        {:ok, value} -> value
        _ -> nil
      end
    end)
  end

  def expire(key, ms), do: call_key(key, {:expire, key, ms})
  def ttl(key, unit), do: call_key(key, {:ttl, key, unit})
  def persist(key), do: call_key(key, {:persist, key})

  def lpush(key, values), do: call_key(key, {:lpush, key, values})
  def rpush(key, values), do: call_key(key, {:rpush, key, values})
  def lpop(key, count), do: call_key(key, {:lpop, key, count})
  def rpop(key, count), do: call_key(key, {:rpop, key, count})
  def lrange(key, start, stop), do: call_key(key, {:lrange, key, start, stop})
  def llen(key), do: call_key(key, {:llen, key})
  def lindex(key, index), do: call_key(key, {:lindex, key, index})

  def hset(key, pairs), do: call_key(key, {:hset, key, pairs})
  def hget(key, field), do: call_key(key, {:hget, key, field})
  def hdel(key, fields), do: call_key(key, {:hdel, key, fields})
  def hgetall(key), do: call_key(key, {:hgetall, key})
  def hkeys(key), do: call_key(key, {:hkeys, key})
  def hvals(key), do: call_key(key, {:hvals, key})
  def hlen(key), do: call_key(key, {:hlen, key})
  def hexists(key, field), do: call_key(key, {:hexists, key, field})
  def hincrby(key, field, delta), do: call_key(key, {:hincrby, key, field, delta})

  def sadd(key, members), do: call_key(key, {:sadd, key, members})
  def srem(key, members), do: call_key(key, {:srem, key, members})
  def smembers(key), do: call_key(key, {:smembers, key})
  def sismember(key, member), do: call_key(key, {:sismember, key, member})
  def scard(key), do: call_key(key, {:scard, key})
  def sinter(keys), do: set_op(keys, &MapSet.intersection/2)
  def sunion(keys), do: set_op(keys, &MapSet.union/2)
  def sdiff(keys), do: set_op(keys, &MapSet.difference/2)
  def sinterstore(dest, keys), do: store_set_op(dest, keys, &MapSet.intersection/2)
  def sunionstore(dest, keys), do: store_set_op(dest, keys, &MapSet.union/2)
  def sdiffstore(dest, keys), do: store_set_op(dest, keys, &MapSet.difference/2)

  def zadd(key, entries, opts \\ []), do: call_key(key, {:zadd, key, entries, opts})
  def zscore(key, member), do: call_key(key, {:zscore, key, member})
  def zrank(key, member), do: call_key(key, {:zrank, key, member})
  def zincrby(key, delta, member), do: call_key(key, {:zincrby, key, delta, member})
  def zrem(key, members), do: call_key(key, {:zrem, key, members})
  def zcard(key), do: call_key(key, {:zcard, key})
  def zrange(key, start, stop), do: call_key(key, {:zrange, key, start, stop})
  def zrangebyscore(key, min, max), do: call_key(key, {:zrangebyscore, key, min, max})
  def zpopmin(key, count), do: call_key(key, {:zpopmin, key, count})
  def zpopmax(key, count), do: call_key(key, {:zpopmax, key, count})

  def xadd(key, idSpec, fields, trim, nomkstream),
    do: call_key(key, {:stream, {:xadd, key, idSpec, fields, trim, nomkstream}})

  def xlen(key), do: call_key(key, {:stream, {:xlen, key}})

  def xrange(key, startBound, endBound, count),
    do: call_key(key, {:stream, {:xrange, key, startBound, endBound, count}})

  def xrevrange(key, endBound, startBound, count),
    do: call_key(key, {:stream, {:xrevrange, key, endBound, startBound, count}})

  def xread(key, afterId, count), do: call_key(key, {:stream, {:xread, key, afterId, count}})
  def xdel(key, ids), do: call_key(key, {:stream, {:xdel, key, ids}})
  def xtrim(key, strategy), do: call_key(key, {:stream, {:xtrim, key, strategy}})
  def xsetid(key, id), do: call_key(key, {:stream, {:xsetid, key, id}})
  def xinfoStream(key), do: call_key(key, {:stream, {:xinfoStream, key}})

  def xgroupCreate(key, name, startSpec, mkstream),
    do: call_key(key, {:stream, {:xgroupCreate, key, name, startSpec, mkstream}})

  def xgroupDestroy(key, name), do: call_key(key, {:stream, {:xgroupDestroy, key, name}})

  def xgroupCreateConsumer(key, name, consumer),
    do: call_key(key, {:stream, {:xgroupCreateConsumer, key, name, consumer}})

  def xgroupDelConsumer(key, name, consumer),
    do: call_key(key, {:stream, {:xgroupDelConsumer, key, name, consumer}})

  def xgroupSetId(key, name, id), do: call_key(key, {:stream, {:xgroupSetId, key, name, id}})

  def xreadgroup(key, name, consumer, mode, count, noack),
    do: call_key(key, {:stream, {:xreadgroup, key, name, consumer, mode, count, noack}})

  def xack(key, name, ids), do: call_key(key, {:stream, {:xack, key, name, ids}})

  def xpendingSummary(key, name), do: call_key(key, {:stream, {:xpendingSummary, key, name}})

  def xpendingRange(key, name, startId, endId, count, consumer, minIdle),
    do:
      call_key(
        key,
        {:stream, {:xpendingRange, key, name, startId, endId, count, consumer, minIdle}}
      )

  def xclaim(key, name, consumer, minIdle, ids, opts),
    do: call_key(key, {:stream, {:xclaim, key, name, consumer, minIdle, ids, opts}})

  def xautoclaim(key, name, consumer, minIdle, startId, count),
    do: call_key(key, {:stream, {:xautoclaim, key, name, consumer, minIdle, startId, count}})

  def setbit(key, offset, value), do: call_key(key, {:binary, {:setbit, key, offset, value}})
  def getbit(key, offset), do: call_key(key, {:binary, {:getbit, key, offset}})
  def bitcount(key, range), do: call_key(key, {:binary, {:bitcount, key, range}})

  def bitpos(key, bit, range, endGiven),
    do: call_key(key, {:binary, {:bitpos, key, bit, range, endGiven}})

  def bitfield(key, operations, writes?),
    do: call_key(key, {:binary, {:bitfield, key, operations, writes?}})

  def strlen(key), do: call_key(key, {:binary, {:strlen, key}})
  def getrange(key, start, stop), do: call_key(key, {:binary, {:getrange, key, start, stop}})
  def setrange(key, offset, value), do: call_key(key, {:binary, {:setrange, key, offset, value}})

  def pfadd(key, elements), do: call_key(key, {:binary, {:pfadd, key, elements}})
  def pfget(key), do: call_key(key, {:binary, {:pfget, key}})
  def pfstore(key, binary), do: call_key(key, {:binary, {:pfstore, key, binary}})

  def bitop(:not, dest, [source]) do
    with {:ok, binary} <- fetchRawString(source) do
      result = Bitmap.combine(:not, [binary || ""])
      storeStringResult(dest, result)
    end
  end

  def bitop(operation, dest, sources) do
    with {:ok, binaries} <- fetchRawStrings(sources) do
      result = Bitmap.combine(operation, Enum.map(binaries, &(&1 || "")))
      storeStringResult(dest, result)
    end
  end

  def pfcount(keys) do
    with {:ok, binaries} <- fetchHllBinaries(keys) do
      present = Enum.reject(binaries, &is_nil/1)
      count = if present == [], do: 0, else: HyperLogLog.countMerged(present)
      {:ok, count}
    end
  end

  def pfmerge(dest, sources) do
    with {:ok, binaries} <- fetchHllBinaries([dest | sources]) do
      present = Enum.reject(binaries, &is_nil/1)
      merged = if present == [], do: HyperLogLog.empty(), else: HyperLogLog.merge(present)
      pfstore(dest, merged)
      {:ok, "OK"}
    end
  end

  defp fetchRawString(key) do
    case call_key(key, {:get, key}) do
      {:ok, value} -> {:ok, value}
      nil -> {:ok, nil}
      {:error, _} = error -> error
    end
  end

  defp fetchRawStrings(keys) do
    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
      case fetchRawString(key) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp fetchHllBinaries(keys) do
    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
      case pfget(key) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp storeStringResult(dest, result) do
    if result == <<>> do
      call_key(dest, {:del, dest})
    else
      call_key(dest, {:set, dest, result, []})
    end

    {:ok, byte_size(result)}
  end

  def getdel(key), do: call_key(key, {:extra, {:getdel, key}})
  def incrbyfloat(key, delta), do: call_key(key, {:extra, {:incrbyfloat, key, delta}})
  def hsetnx(key, field, value), do: call_key(key, {:extra, {:hsetnx, key, field, value}})
  def spop(key, count), do: call_key(key, {:extra, {:spop, key, count}})
  def srandmember(key, count), do: call_key(key, {:extra, {:srandmember, key, count}})
  def lrem(key, count, value), do: call_key(key, {:extra, {:lrem, key, count, value}})
  def lset(key, index, value), do: call_key(key, {:extra, {:lset, key, index, value}})
  def ltrim(key, start, stop), do: call_key(key, {:extra, {:ltrim, key, start, stop}})

  def linsert(key, position, pivot, value),
    do: call_key(key, {:extra, {:linsert, key, position, pivot, value}})

  def zcount(key, minBound, maxBound),
    do: call_key(key, {:extra, {:zcount, key, minBound, maxBound}})

  def zrevrange(key, start, stop), do: call_key(key, {:extra, {:zrevrange, key, start, stop}})
  def zrevrank(key, member), do: call_key(key, {:extra, {:zrevrank, key, member}})
  def zmscore(key, members), do: call_key(key, {:extra, {:zmscore, key, members}})
  def expireat(key, msAbs), do: call_key(key, {:extra, {:expireat, key, msAbs}})
  def expiretime(key, unit), do: call_key(key, {:extra, {:expiretime, key, unit}})

  def lpos(key, element, rank, count, maxlen),
    do: call_key(key, {:extra, {:lpos, key, element, rank, count, maxlen}})

  def setnx(key, value) do
    case set(key, value, nx: true) do
      :ok -> 1
      nil -> 0
    end
  end

  def touch(keys), do: exists(keys)

  def hmget(key, fields) do
    collectPerField(fields, fn field ->
      case call_key(key, {:hget, key, field}) do
        {:ok, value} -> {:ok, value}
        nil -> {:ok, nil}
        error -> error
      end
    end)
  end

  def smismember(key, members) do
    collectPerField(members, fn member ->
      case call_key(key, {:sismember, key, member}) do
        {:ok, present} -> {:ok, present}
        error -> error
      end
    end)
  end

  defp collectPerField(entries, lookup) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      case lookup.(entry) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> Enum.reverse(values)
      error -> error
    end
  end

  def sintercard(keys, limit) do
    with {:ok, members} <- set_op(keys, &MapSet.intersection/2) do
      count = length(members)
      {:ok, if(limit == 0, do: count, else: min(count, limit))}
    end
  end

  def smove(source, dest, member) do
    with :ok <- ensureKind(dest, "set") do
      case call_key(source, {:srem, source, [member]}) do
        {:ok, 0} ->
          {:ok, 0}

        {:ok, _removed} ->
          case call_key(dest, {:sadd, dest, [member]}) do
            {:ok, _added} ->
              {:ok, 1}

            error ->
              call_key(source, {:sadd, source, [member]})
              error
          end

        error ->
          error
      end
    end
  end

  def rpoplpush(source, dest) do
    with :ok <- ensureKind(dest, "list") do
      case call_key(source, {:rpop, source, nil}) do
        {:ok, value} ->
          case call_key(dest, {:lpush, dest, [value]}) do
            {:ok, _length} ->
              {:ok, value}

            error ->
              call_key(source, {:rpush, source, [value]})
              error
          end

        nil ->
          {:ok, nil}

        error ->
          error
      end
    end
  end

  defp ensureKind(key, expected) do
    case call_key(key, {:type, key}) do
      "none" -> :ok
      ^expected -> :ok
      _ -> {:error, :wrongtype}
    end
  end

  defp putEntry(dest, {kind, value, ea}),
    do: call_key(dest, {:extra, {:raw_put, dest, kind, value, ea}})

  def rename(key, key) do
    case call_key(key, {:exists, key}) do
      1 -> {:ok, "OK"}
      0 -> {:error, :nokey}
    end
  end

  def rename(source, dest) do
    case call_key(source, {:peek, source}) do
      :miss ->
        {:error, :nokey}

      entry ->
        if putEntry(dest, entry) == 1 do
          call_key(source, {:del, source})
          {:ok, "OK"}
        else
          {:error, :nokey}
        end
    end
  end

  def renamenx(source, dest) do
    case call_key(source, {:peek, source}) do
      :miss ->
        {:error, :nokey}

      entry ->
        case call_key(dest, {:exists, dest}) do
          1 ->
            {:ok, 0}

          0 ->
            if putEntry(dest, entry) == 1 do
              call_key(source, {:del, source})
              {:ok, 1}
            else
              {:error, :nokey}
            end
        end
    end
  end

  def copy(source, dest, replace) do
    case call_key(source, {:peek, source}) do
      :miss ->
        {:ok, 0}

      entry ->
        if not replace and call_key(dest, {:exists, dest}) == 1 do
          {:ok, 0}
        else
          {:ok, putEntry(dest, entry)}
        end
    end
  end

  def encoding(key) do
    case call_key(key, {:peek, key}) do
      :miss -> nil
      entry -> {:ok, entry}
    end
  end

  def scan(cursor, regex, typeFilter) do
    shard = cursor
    keys = GenServer.call(shard_name(shard), {:op, {:extra, {:scan_shard, regex, typeFilter}}})
    nextCursor = if shard + 1 < shard_count(), do: shard + 1, else: 0
    {nextCursor, keys}
  end

  def watch_snapshot(key), do: call_key(key, {:peek, key})
  def unchanged?(key, snapshot), do: watch_snapshot(key) == snapshot

  def keys(pattern), do: fan_out({:keys, pattern}) |> List.flatten()
  def dbsize, do: fan_out(:dbsize) |> Enum.sum()

  def flushdb do
    fan_out(:flushdb)
    :ok
  end

  def sweep do
    fan_out(:sweep) |> List.flatten()
  end

  def dump do
    for index <- 0..(shard_count() - 1),
        entry <- GenServer.call(shard_name(index), :dump) do
      entry
    end
  end

  def restart do
    for index <- 0..(shard_count() - 1) do
      Supervisor.terminate_child(Excache.Store.Supervisor, {:shard, index})
      Supervisor.restart_child(Excache.Store.Supervisor, {:shard, index})
    end

    :ok
  end

  defp set_op([first | rest], combine) do
    with {:ok, initial} <- fetch_set(first) do
      Enum.reduce_while(rest, {:ok, initial}, fn key, {:ok, acc} ->
        case fetch_set(key) do
          {:ok, set} -> {:cont, {:ok, combine.(acc, set)}}
          error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, set} -> {:ok, MapSet.to_list(set)}
        error -> error
      end
    end
  end

  defp fetch_set(key) do
    case call_key(key, {:smembers, key}) do
      {:ok, members} -> {:ok, MapSet.new(members)}
      error -> error
    end
  end

  defp store_set_op(dest, keys, combine) do
    with {:ok, members} <- set_op(keys, combine) do
      call_key(dest, {:del, dest})

      if members == [] do
        {:ok, 0}
      else
        {:ok, added} = call_key(dest, {:sadd, dest, members})
        {:ok, added}
      end
    end
  end
end
