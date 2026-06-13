defmodule Excache.Store.Ops.Extra do
  @moduledoc false

  alias Excache.SkipList
  alias Excache.Store.Ops.Helpers

  import Helpers, only: [fetch: 3, collection: 6, with_kind: 6, expired?: 2, compile_pattern: 1]

  def apply_op(table, now, {:raw_put, key, kind, value, ea}) do
    if ea != nil and ea <= now do
      0
    else
      :ets.insert(table, {key, kind, value, ea})
      1
    end
  end

  def apply_op(table, now, {:getdel, key}) do
    case fetch(table, key, now) do
      :miss ->
        nil

      {:string, value, _ea} ->
        :ets.delete(table, key)
        {:ok, value}

      _ ->
        {:error, :wrongtype}
    end
  end

  def apply_op(table, now, {:incrbyfloat, key, delta}) do
    case fetch(table, key, now) do
      :miss ->
        stored = formatFloat(delta)
        :ets.insert(table, {key, :string, stored, nil})
        {:ok, stored}

      {:string, value, ea} ->
        case parseFloat(value) do
          {:ok, current} ->
            stored = formatFloat(current + delta)
            :ets.insert(table, {key, :string, stored, ea})
            {:ok, stored}

          :error ->
            {:error, :not_float}
        end

      _ ->
        {:error, :wrongtype}
    end
  end

  def apply_op(table, now, {:hsetnx, key, field, value}) do
    collection(table, key, now, :hash, %{}, fn current ->
      if Map.has_key?(current, field) do
        {current, {:ok, 0}}
      else
        {Map.put(current, field, value), {:ok, 1}}
      end
    end)
  end

  def apply_op(table, now, {:spop, key, count}) do
    collection(table, key, now, :set, MapSet.new(), fn current ->
      members = MapSet.to_list(current)
      taken = randomSample(members, popCount(count, length(members)))
      remaining = Enum.reduce(taken, current, &MapSet.delete(&2, &1))
      {remaining, {:ok, taken}}
    end)
  end

  def apply_op(table, now, {:srandmember, key, count}) do
    with_kind(table, key, now, :set, MapSet.new(), fn set ->
      members = MapSet.to_list(set)

      result =
        cond do
          count == nil -> randomSample(members, min(1, length(members)))
          count >= 0 -> randomSample(members, min(count, length(members)))
          true -> withReplacement(members, -count)
        end

      {:ok, result}
    end)
  end

  def apply_op(table, now, {:lrem, key, count, value}) do
    case fetch(table, key, now) do
      :miss ->
        {:ok, 0}

      {:list, list, ea} ->
        {updated, removed} = removeOccurrences(list, count, value)
        commitList(table, key, updated, ea)
        {:ok, removed}

      _ ->
        {:error, :wrongtype}
    end
  end

  def apply_op(table, now, {:lset, key, index, value}) do
    case fetch(table, key, now) do
      :miss ->
        {:error, :nokey}

      {:list, list, ea} ->
        resolved = if index < 0, do: length(list) + index, else: index

        if resolved >= 0 and resolved < length(list) do
          :ets.insert(table, {key, :list, List.replace_at(list, resolved, value), ea})
          {:ok, "OK"}
        else
          {:error, :range}
        end

      _ ->
        {:error, :wrongtype}
    end
  end

  def apply_op(table, now, {:ltrim, key, start, stop}) do
    case fetch(table, key, now) do
      :miss ->
        {:ok, "OK"}

      {:list, list, ea} ->
        len = length(list)
        lower = max(normalize(start, len), 0)
        upper = min(normalize(stop, len), len - 1)

        trimmed =
          if len == 0 or lower > upper, do: [], else: Enum.slice(list, lower..upper)

        commitList(table, key, trimmed, ea)
        {:ok, "OK"}

      _ ->
        {:error, :wrongtype}
    end
  end

  def apply_op(table, now, {:linsert, key, position, pivot, value}) do
    case fetch(table, key, now) do
      :miss ->
        {:ok, 0}

      {:list, list, ea} ->
        case insertRelative(list, position, pivot, value) do
          :not_found ->
            {:ok, -1}

          updated ->
            :ets.insert(table, {key, :list, updated, ea})
            {:ok, length(updated)}
        end

      _ ->
        {:error, :wrongtype}
    end
  end

  def apply_op(table, now, {:zcount, key, minBound, maxBound}) do
    with_kind(table, key, now, :zset, SkipList.new(), fn zset ->
      {:ok, length(SkipList.range_by_score(zset, minBound, maxBound))}
    end)
  end

  def apply_op(table, now, {:zrevrange, key, start, stop}) do
    with_kind(table, key, now, :zset, SkipList.new(), fn zset ->
      descending = zset |> SkipList.to_list() |> Enum.reverse()
      len = length(descending)
      lower = max(normalize(start, len), 0)
      upper = min(normalize(stop, len), len - 1)

      window =
        if len == 0 or lower > upper, do: [], else: Enum.slice(descending, lower..upper)

      {:ok, window}
    end)
  end

  def apply_op(table, now, {:zrevrank, key, member}) do
    with_kind(table, key, now, :zset, SkipList.new(), fn zset ->
      case SkipList.rank(zset, member) do
        {:ok, rank} -> {:ok, SkipList.size(zset) - 1 - rank}
        :error -> nil
      end
    end)
  end

  def apply_op(table, now, {:zmscore, key, members}) do
    with_kind(table, key, now, :zset, SkipList.new(), fn zset ->
      scores =
        Enum.map(members, fn member ->
          case SkipList.score(zset, member) do
            {:ok, score} -> score
            :error -> nil
          end
        end)

      {:ok, scores}
    end)
  end

  def apply_op(table, now, {:expireat, key, msAbs}) do
    case fetch(table, key, now) do
      :miss ->
        0

      {kind, value, _ea} ->
        if msAbs <= now do
          :ets.delete(table, key)
          1
        else
          :ets.insert(table, {key, kind, value, msAbs})
          1
        end
    end
  end

  def apply_op(table, now, {:expiretime, key, unit}) do
    case fetch(table, key, now) do
      :miss -> -2
      {_kind, _value, nil} -> -1
      {_kind, _value, ea} -> convertUnit(ea, unit)
    end
  end

  def apply_op(table, now, {:lpos, key, element, rank, count, maxlen}) do
    with_kind(table, key, now, :list, [], fn list ->
      {:ok, locate(list, element, rank, count, maxlen)}
    end)
  end

  def apply_op(table, now, {:scan_shard, regex, typeFilter}) do
    :ets.foldl(
      fn {key, kind, _value, ea}, acc ->
        if not expired?(ea, now) and matchesPattern(regex, key) and matchesType(typeFilter, kind) do
          [key | acc]
        else
          acc
        end
      end,
      [],
      table
    )
  end

  defp commitList(table, key, [], _ea), do: :ets.delete(table, key)
  defp commitList(table, key, list, ea), do: :ets.insert(table, {key, :list, list, ea})

  defp popCount(nil, size), do: min(1, size)
  defp popCount(count, size), do: min(count, size)

  defp randomSample(_members, 0), do: []

  defp randomSample(members, count) do
    members |> Enum.shuffle() |> Enum.take(count)
  end

  defp withReplacement(_members, 0), do: []
  defp withReplacement([], _count), do: []

  defp withReplacement(members, count) do
    for _ <- 1..count, do: Enum.random(members)
  end

  defp removeOccurrences(list, 0, value) do
    filtered = Enum.reject(list, &(&1 == value))
    {filtered, length(list) - length(filtered)}
  end

  defp removeOccurrences(list, count, value) when count > 0 do
    {kept, removed} = dropMatches(list, count, value, [])
    {kept, removed}
  end

  defp removeOccurrences(list, count, value) when count < 0 do
    {kept, removed} = dropMatches(Enum.reverse(list), -count, value, [])
    {Enum.reverse(kept), removed}
  end

  defp dropMatches([], _remaining, _value, acc), do: {Enum.reverse(acc), 0}

  defp dropMatches(rest, 0, _value, acc), do: {Enum.reverse(acc) ++ rest, 0}

  defp dropMatches([head | tail], remaining, value, acc) do
    if head == value do
      {kept, removed} = dropMatches(tail, remaining - 1, value, acc)
      {kept, removed + 1}
    else
      dropMatches(tail, remaining, value, [head | acc])
    end
  end

  defp insertRelative(list, position, pivot, value) do
    case Enum.find_index(list, &(&1 == pivot)) do
      nil ->
        :not_found

      index ->
        insertAt = if position == :before, do: index, else: index + 1
        List.insert_at(list, insertAt, value)
    end
  end

  defp locate(list, element, rank, count, maxlen) do
    indexed = Enum.with_index(list)

    scanned =
      if rank < 0, do: Enum.reverse(indexed), else: indexed

    skip = abs(rank) - 1
    limit = if count == 0, do: :all, else: count

    collectMatches(scanned, element, skip, limit, maxlen, 0, [])
    |> then(fn matches ->
      if count == nil do
        case matches do
          [] -> nil
          [first | _] -> first
        end
      else
        matches
      end
    end)
  end

  defp collectMatches([], _element, _skip, _limit, _maxlen, _compared, acc),
    do: Enum.reverse(acc)

  defp collectMatches(_rest, _element, _skip, 0, _maxlen, _compared, acc),
    do: Enum.reverse(acc)

  defp collectMatches([{value, index} | tail], element, skip, limit, maxlen, compared, acc) do
    if maxlen != 0 and compared >= maxlen do
      Enum.reverse(acc)
    else
      if value == element do
        if skip > 0 do
          collectMatches(tail, element, skip - 1, limit, maxlen, compared + 1, acc)
        else
          nextLimit = if limit == :all, do: :all, else: limit - 1
          collectMatches(tail, element, skip, nextLimit, maxlen, compared + 1, [index | acc])
        end
      else
        collectMatches(tail, element, skip, limit, maxlen, compared + 1, acc)
      end
    end
  end

  defp matchesPattern(nil, _key), do: true
  defp matchesPattern(regex, key), do: Regex.match?(regex, key)

  defp matchesType(nil, _kind), do: true
  defp matchesType(typeFilter, kind), do: typeFilter == Atom.to_string(kind)

  defp normalize(index, len) when index < 0, do: len + index
  defp normalize(index, _len), do: index

  defp convertUnit(ea, :seconds), do: div(ea, 1000)
  defp convertUnit(ea, :milliseconds), do: ea

  defp parseFloat(value) do
    case Integer.parse(value) do
      {number, ""} ->
        {:ok, number * 1.0}

      _ ->
        case Float.parse(value) do
          {number, ""} -> {:ok, number}
          _ -> :error
        end
    end
  end

  defp formatFloat(number) do
    rounded = Float.round(number * 1.0, 0)

    if number == rounded and abs(number) < 1.0e15 do
      Integer.to_string(trunc(number))
    else
      value = number * 1.0
      trimmed = value |> :erlang.float_to_binary([:short])
      trimmed
    end
  end

  def compilePattern(pattern), do: compile_pattern(pattern)
end
