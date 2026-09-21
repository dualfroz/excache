defmodule Excache.Store.Ops do
  @moduledoc false

  alias Excache.SkipList
  alias Excache.Store.Ops.Helpers

  import Helpers,
    only: [
      fetch: 3,
      collection: 6,
      with_kind: 6,
      pop_list: 3,
      rank_window: 3,
      slice: 3,
      at: 2,
      expired?: 2,
      expiry_from_opts: 2,
      remaining: 2,
      compile_pattern: 1
    ]

  def apply_op(table, now, {:stream, op}),
    do: Excache.Store.Ops.Streams.apply_op(table, now, op)

  def apply_op(table, now, {:binary, op}),
    do: Excache.Store.Ops.Binary.apply_op(table, now, op)

  def apply_op(table, now, {:extra, op}),
    do: Excache.Store.Ops.Extra.apply_op(table, now, op)

  def apply_op(table, now, {:peek, key}), do: fetch(table, key, now)

  def apply_op(table, now, {:get, key}) do
    case fetch(table, key, now) do
      :miss -> nil
      {:string, value, _ea} -> {:ok, value}
      _ -> {:error, :wrongtype}
    end
  end

  def apply_op(table, now, {:set, key, value, opts}) do
    existing = fetch(table, key, now)
    exists = existing != :miss

    cond do
      Keyword.get(opts, :nx, false) and exists ->
        nil

      Keyword.get(opts, :xx, false) and not exists ->
        nil

      true ->
        :ets.insert(table, {key, :string, value, expiry_from_opts(opts, now)})
        :ok
    end
  end

  def apply_op(table, now, {:del, key}) do
    case fetch(table, key, now) do
      :miss ->
        0

      _ ->
        :ets.delete(table, key)
        1
    end
  end

  def apply_op(table, now, {:exists, key}) do
    case fetch(table, key, now) do
      :miss -> 0
      _ -> 1
    end
  end

  def apply_op(table, now, {:type, key}) do
    case fetch(table, key, now) do
      :miss -> "none"
      {kind, _value, _ea} -> Atom.to_string(kind)
    end
  end

  def apply_op(table, now, {:incrby, key, delta}) do
    case fetch(table, key, now) do
      :miss ->
        :ets.insert(table, {key, :string, Integer.to_string(delta), nil})
        {:ok, delta}

      {:string, value, ea} ->
        case Integer.parse(value) do
          {current, ""} ->
            updated = current + delta
            :ets.insert(table, {key, :string, Integer.to_string(updated), ea})
            {:ok, updated}

          _ ->
            {:error, :not_integer}
        end

      _ ->
        {:error, :wrongtype}
    end
  end

  def apply_op(table, now, {:append, key, value}) do
    case fetch(table, key, now) do
      :miss ->
        :ets.insert(table, {key, :string, value, nil})
        {:ok, byte_size(value)}

      {:string, current, ea} ->
        updated = current <> value
        :ets.insert(table, {key, :string, updated, ea})
        {:ok, byte_size(updated)}

      _ ->
        {:error, :wrongtype}
    end
  end

  def apply_op(table, now, {:getset, key, value}) do
    previous =
      case fetch(table, key, now) do
        :miss -> {:ok, nil}
        {:string, current, _ea} -> {:ok, current}
        _ -> {:error, :wrongtype}
      end

    case previous do
      {:ok, prior} ->
        :ets.insert(table, {key, :string, value, nil})
        {:ok, prior}

      error ->
        error
    end
  end

  def apply_op(table, now, {:expire, key, ms}) do
    case fetch(table, key, now) do
      :miss ->
        0

      {kind, value, _ea} ->
        :ets.insert(table, {key, kind, value, now + ms})
        1
    end
  end

  def apply_op(table, now, {:ttl, key, unit}) do
    case fetch(table, key, now) do
      :miss -> -2
      {_kind, _value, nil} -> -1
      {_kind, _value, ea} -> remaining(ea - now, unit)
    end
  end

  def apply_op(table, now, {:persist, key}) do
    case fetch(table, key, now) do
      :miss ->
        0

      {_kind, _value, nil} ->
        0

      {kind, value, _ea} ->
        :ets.insert(table, {key, kind, value, nil})
        1
    end
  end

  def apply_op(table, now, {:lpush, key, values}) do
    collection(table, key, now, :list, [], fn current ->
      updated = Enum.reverse(values) ++ current
      {updated, {:ok, length(updated)}}
    end)
  end

  def apply_op(table, now, {:rpush, key, values}) do
    collection(table, key, now, :list, [], fn current ->
      updated = current ++ values
      {updated, {:ok, length(updated)}}
    end)
  end

  def apply_op(table, now, {:lpop, key, count}) do
    collection(table, key, now, :list, [], fn current -> pop_list(current, :left, count) end)
  end

  def apply_op(table, now, {:rpop, key, count}) do
    collection(table, key, now, :list, [], fn current -> pop_list(current, :right, count) end)
  end

  def apply_op(table, now, {:lrange, key, start, stop}) do
    with_kind(table, key, now, :list, [], fn list -> {:ok, slice(list, start, stop)} end)
  end

  def apply_op(table, now, {:llen, key}) do
    with_kind(table, key, now, :list, [], fn list -> {:ok, length(list)} end)
  end

  def apply_op(table, now, {:lindex, key, index}) do
    with_kind(table, key, now, :list, [], fn list -> at(list, index) end)
  end

  def apply_op(table, now, {:hset, key, pairs}) do
    collection(table, key, now, :hash, %{}, fn current ->
      added = Enum.count(pairs, fn {field, _} -> not Map.has_key?(current, field) end)

      updated =
        Enum.reduce(pairs, current, fn {field, value}, acc -> Map.put(acc, field, value) end)

      {updated, {:ok, added}}
    end)
  end

  def apply_op(table, now, {:hget, key, field}) do
    with_kind(table, key, now, :hash, %{}, fn map ->
      case Map.fetch(map, field) do
        {:ok, value} -> {:ok, value}
        :error -> nil
      end
    end)
  end

  def apply_op(table, now, {:hdel, key, fields}) do
    collection(table, key, now, :hash, %{}, fn current ->
      removed = Enum.count(fields, &Map.has_key?(current, &1))
      {Map.drop(current, fields), {:ok, removed}}
    end)
  end

  def apply_op(table, now, {:hgetall, key}) do
    with_kind(table, key, now, :hash, %{}, fn map ->
      {:ok, Enum.flat_map(map, fn {field, value} -> [field, value] end)}
    end)
  end

  def apply_op(table, now, {:hkeys, key}) do
    with_kind(table, key, now, :hash, %{}, fn map -> {:ok, Map.keys(map)} end)
  end

  def apply_op(table, now, {:hvals, key}) do
    with_kind(table, key, now, :hash, %{}, fn map -> {:ok, Map.values(map)} end)
  end

  def apply_op(table, now, {:hlen, key}) do
    with_kind(table, key, now, :hash, %{}, fn map -> {:ok, map_size(map)} end)
  end

  def apply_op(table, now, {:hexists, key, field}) do
    with_kind(table, key, now, :hash, %{}, fn map ->
      {:ok, if(Map.has_key?(map, field), do: 1, else: 0)}
    end)
  end

  def apply_op(table, now, {:hincrby, key, field, delta}) do
    collection(table, key, now, :hash, %{}, fn current ->
      case Map.fetch(current, field) do
        :error ->
          {Map.put(current, field, Integer.to_string(delta)), {:ok, delta}}

        {:ok, value} ->
          case Integer.parse(value) do
            {existing, ""} ->
              updated = existing + delta
              {Map.put(current, field, Integer.to_string(updated)), {:ok, updated}}

            _ ->
              {current, {:error, :not_integer}}
          end
      end
    end)
  end

  def apply_op(table, now, {:sadd, key, members}) do
    collection(table, key, now, :set, MapSet.new(), fn current ->
      added = Enum.count(members, &(not MapSet.member?(current, &1)))
      updated = Enum.reduce(members, current, &MapSet.put(&2, &1))
      {updated, {:ok, added}}
    end)
  end

  def apply_op(table, now, {:srem, key, members}) do
    collection(table, key, now, :set, MapSet.new(), fn current ->
      removed = Enum.count(members, &MapSet.member?(current, &1))
      updated = Enum.reduce(members, current, &MapSet.delete(&2, &1))
      {updated, {:ok, removed}}
    end)
  end

  def apply_op(table, now, {:smembers, key}) do
    with_kind(table, key, now, :set, MapSet.new(), fn set -> {:ok, MapSet.to_list(set)} end)
  end

  def apply_op(table, now, {:sismember, key, member}) do
    with_kind(table, key, now, :set, MapSet.new(), fn set ->
      {:ok, if(MapSet.member?(set, member), do: 1, else: 0)}
    end)
  end

  def apply_op(table, now, {:scard, key}) do
    with_kind(table, key, now, :set, MapSet.new(), fn set -> {:ok, MapSet.size(set)} end)
  end

  def apply_op(table, now, {:zadd, key, entries, opts}) do
    collection(table, key, now, :zset, SkipList.new(), fn current ->
      {updated, added, changed} =
        Enum.reduce(entries, {current, 0, 0}, fn {member, score}, {acc, added, changed} ->
          zadd_entry(acc, member, score, opts, added, changed)
        end)

      count = if Keyword.get(opts, :ch, false), do: changed, else: added
      {updated, {:ok, count}}
    end)
  end

  def apply_op(table, now, {:zscore, key, member}) do
    with_kind(table, key, now, :zset, SkipList.new(), fn zset ->
      case SkipList.score(zset, member) do
        {:ok, score} -> {:ok, score}
        :error -> nil
      end
    end)
  end

  def apply_op(table, now, {:zrank, key, member}) do
    with_kind(table, key, now, :zset, SkipList.new(), fn zset ->
      case SkipList.rank(zset, member) do
        {:ok, rank} -> {:ok, rank}
        :error -> nil
      end
    end)
  end

  def apply_op(table, now, {:zincrby, key, delta, member}) do
    collection(table, key, now, :zset, SkipList.new(), fn current ->
      previous =
        case SkipList.score(current, member) do
          {:ok, score} -> score
          :error -> 0
        end

      updated_score = previous + delta
      {SkipList.insert(current, member, updated_score), {:ok, updated_score}}
    end)
  end

  def apply_op(table, now, {:zrem, key, members}) do
    collection(table, key, now, :zset, SkipList.new(), fn current ->
      removed = Enum.count(members, &SkipList.member?(current, &1))
      updated = Enum.reduce(members, current, &SkipList.delete(&2, &1))
      {updated, {:ok, removed}}
    end)
  end

  def apply_op(table, now, {:zpopmin, key, count}) do
    collection(table, key, now, :zset, SkipList.new(), fn current ->
      popped = SkipList.range_by_rank(current, 0, count)
      {drop_zset_members(current, popped), {:ok, popped}}
    end)
  end

  def apply_op(table, now, {:zpopmax, key, count}) do
    collection(table, key, now, :zset, SkipList.new(), fn current ->
      lower = max(SkipList.size(current) - count, 0)
      popped = current |> SkipList.range_by_rank(lower, count) |> Enum.reverse()
      {drop_zset_members(current, popped), {:ok, popped}}
    end)
  end

  def apply_op(table, now, {:zcard, key}) do
    with_kind(table, key, now, :zset, SkipList.new(), fn zset -> {:ok, SkipList.size(zset)} end)
  end

  def apply_op(table, now, {:zrange, key, start, stop}) do
    with_kind(table, key, now, :zset, SkipList.new(), fn zset ->
      {lower, count} = rank_window(SkipList.size(zset), start, stop)
      {:ok, SkipList.range_by_rank(zset, lower, count)}
    end)
  end

  def apply_op(table, now, {:zrangebyscore, key, min_bound, max_bound}) do
    with_kind(table, key, now, :zset, SkipList.new(), fn zset ->
      {:ok, SkipList.range_by_score(zset, min_bound, max_bound)}
    end)
  end

  def apply_op(table, now, {:keys, pattern}) do
    regex = compile_pattern(pattern)

    :ets.foldl(
      fn {key, _kind, _value, ea}, acc ->
        if not expired?(ea, now) and Regex.match?(regex, key), do: [key | acc], else: acc
      end,
      [],
      table
    )
  end

  def apply_op(table, now, :dbsize) do
    :ets.foldl(
      fn {_key, _kind, _value, ea}, acc -> if expired?(ea, now), do: acc, else: acc + 1 end,
      0,
      table
    )
  end

  def apply_op(table, _now, :flushdb) do
    :ets.delete_all_objects(table)
    :ok
  end

  def apply_op(table, now, :sweep) do
    spec = [
      {{:"$1", :"$2", :"$3", :"$4"}, [{:andalso, {:"/=", :"$4", nil}, {:"=<", :"$4", now}}],
       [:"$1"]}
    ]

    expired = :ets.select(table, spec)
    Enum.each(expired, &:ets.delete(table, &1))
    expired
  end

  defp zadd_entry(zset, member, score, opts, added, changed) do
    exists = SkipList.member?(zset, member)
    nx = Keyword.get(opts, :nx, false)
    xx = Keyword.get(opts, :xx, false)
    gt = Keyword.get(opts, :gt, false)
    lt = Keyword.get(opts, :lt, false)

    cond do
      nx and exists ->
        {zset, added, changed}

      xx and not exists ->
        {zset, added, changed}

      not exists ->
        {SkipList.insert(zset, member, score), added + 1, changed + 1}

      true ->
        {:ok, current_score} = SkipList.score(zset, member)

        cond do
          gt and score <= current_score -> {zset, added, changed}
          lt and score >= current_score -> {zset, added, changed}
          score == current_score -> {zset, added, changed}
          true -> {SkipList.insert(zset, member, score), added, changed + 1}
        end
    end
  end

  defp drop_zset_members(zset, popped) do
    Enum.reduce(popped, zset, fn {_score, member}, acc -> SkipList.delete(acc, member) end)
  end
end
