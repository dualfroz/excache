defmodule Excache.Stream do
  @moduledoc false

  alias Excache.Stream.Group

  defstruct entries: :gb_trees.empty(),
            lastId: {0, 0},
            maxDeletedId: {0, 0},
            length: 0,
            entriesAdded: 0,
            groups: %{}

  def new, do: %__MODULE__{}

  def length(%__MODULE__{length: length}), do: length

  def formatId({ms, seq}), do: "#{ms}-#{seq}"

  def parseId(raw, defaultSeq) do
    case String.split(raw, "-", parts: 2) do
      [ms] ->
        withInteger(ms, fn value -> {:ok, {value, defaultSeq}} end)

      [ms, "*"] ->
        withInteger(ms, fn value -> {:ok, {:autoSeq, value}} end)

      [ms, seq] ->
        withInteger(ms, fn msValue ->
          withInteger(seq, fn seqValue -> {:ok, {msValue, seqValue}} end)
        end)
    end
  end

  def parseAddId("*", _now), do: {:ok, :auto}

  def parseAddId(raw, _now) do
    case String.split(raw, "-", parts: 2) do
      [ms] ->
        withInteger(ms, fn value -> {:ok, {:autoSeq, value}} end)

      [ms, "*"] ->
        withInteger(ms, fn value -> {:ok, {:autoSeq, value}} end)

      [ms, seq] ->
        withInteger(ms, fn msValue ->
          withInteger(seq, fn seqValue -> {:ok, {:full, msValue, seqValue}} end)
        end)
    end
  end

  def parseRangeStart("-"), do: {:ok, {:incl, {0, 0}}}
  def parseRangeStart("+"), do: {:ok, {:incl, {maxMs(), maxSeq()}}}
  def parseRangeStart("(" <> rest), do: rangeBound(rest, 0, :excl)
  def parseRangeStart(raw), do: rangeBound(raw, 0, :incl)

  def parseRangeEnd("-"), do: {:ok, {:incl, {0, 0}}}
  def parseRangeEnd("+"), do: {:ok, {:incl, {maxMs(), maxSeq()}}}
  def parseRangeEnd("(" <> rest), do: rangeBound(rest, maxSeq(), :excl)
  def parseRangeEnd(raw), do: rangeBound(raw, maxSeq(), :incl)

  def add(stream, idSpec, fields, now) do
    case resolveId(stream, idSpec, now) do
      {:ok, id} ->
        entries = :gb_trees.insert(id, fields, stream.entries)

        updated = %{
          stream
          | entries: entries,
            lastId: id,
            length: stream.length + 1,
            entriesAdded: stream.entriesAdded + 1
        }

        {:ok, updated, id}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def get(stream, id) do
    case :gb_trees.lookup(id, stream.entries) do
      {:value, fields} -> {:ok, fields}
      :none -> :miss
    end
  end

  def member?(stream, id), do: :gb_trees.is_defined(id, stream.entries)

  def range(stream, {startKind, startId}, {endKind, endId}, count) do
    lower = if startKind == :excl, do: successor(startId), else: startId

    upperOk = fn id ->
      case endKind do
        :incl -> id <= endId
        :excl -> id < endId
      end
    end

    if stream.length == 0 or lower > endId do
      []
    else
      iterator = :gb_trees.iterator_from(lower, stream.entries)
      collectForward(iterator, upperOk, count, [])
    end
  end

  def reverseRange(stream, endBound, startBound, count) do
    stream
    |> range(startBound, endBound, :all)
    |> Enum.reverse()
    |> take(count)
  end

  def afterId(stream, id, count) do
    range(stream, {:excl, id}, {:incl, {maxMs(), maxSeq()}}, count)
  end

  def delete(stream, ids) do
    Enum.reduce(ids, {stream, 0}, fn id, {acc, removed} ->
      if :gb_trees.is_defined(id, acc.entries) do
        entries = :gb_trees.delete(id, acc.entries)
        maxDeleted = max(acc.maxDeletedId, id)
        {%{acc | entries: entries, length: acc.length - 1, maxDeletedId: maxDeleted}, removed + 1}
      else
        {acc, removed}
      end
    end)
  end

  def trimMaxlen(stream, maxlen) when maxlen < 0, do: {stream, 0}

  def trimMaxlen(stream, maxlen) do
    dropSmallest(stream, max(stream.length - maxlen, 0), 0)
  end

  def trimMinId(stream, minId) do
    dropBelow(stream, minId, 0)
  end

  def setLastId(stream, id), do: %{stream | lastId: id}

  def lastEntryId(%__MODULE__{lastId: lastId}), do: lastId

  def info(stream) do
    %{
      length: stream.length,
      lastGeneratedId: stream.lastId,
      maxDeletedId: stream.maxDeletedId,
      entriesAdded: stream.entriesAdded,
      groups: map_size(stream.groups),
      firstEntry: firstEntryPair(stream),
      lastEntry: lastEntry(stream)
    }
  end

  defp firstEntryPair(stream) do
    case :gb_trees.is_empty(stream.entries) do
      true -> nil
      false -> :gb_trees.smallest(stream.entries) |> pairFromKV()
    end
  end

  def createGroup(stream, name, startId) do
    if Map.has_key?(stream.groups, name) do
      {:error, :busygroup}
    else
      group = %Group{lastDeliveredId: startId, entriesRead: 0}
      {:ok, %{stream | groups: Map.put(stream.groups, name, group)}}
    end
  end

  def destroyGroup(stream, name) do
    if Map.has_key?(stream.groups, name) do
      {%{stream | groups: Map.delete(stream.groups, name)}, 1}
    else
      {stream, 0}
    end
  end

  def fetchGroup(stream, name), do: Map.fetch(stream.groups, name)

  def putGroup(stream, name, group), do: %{stream | groups: Map.put(stream.groups, name, group)}

  def firstId(stream) do
    case :gb_trees.is_empty(stream.entries) do
      true -> nil
      false -> :gb_trees.smallest(stream.entries) |> elem(0)
    end
  end

  def lastEntry(stream) do
    case :gb_trees.is_empty(stream.entries) do
      true -> nil
      false -> :gb_trees.largest(stream.entries) |> pairFromKV()
    end
  end

  def toList(stream) do
    stream.entries |> :gb_trees.to_list() |> Enum.map(&pairFromKV/1)
  end

  def successor({ms, seq}) when seq >= 0, do: {ms, seq + 1}

  defp pairFromKV({id, fields}), do: {id, fields}

  defp resolveId(stream, :auto, now) do
    {lastMs, lastSeq} = stream.lastId
    ms = max(now, lastMs)
    seq = if ms == lastMs, do: lastSeq + 1, else: 0
    validate(stream, {ms, seq})
  end

  defp resolveId(stream, {:autoSeq, ms}, _now) do
    {lastMs, lastSeq} = stream.lastId

    cond do
      ms < lastMs -> {:error, :smaller}
      ms == lastMs -> validate(stream, {ms, lastSeq + 1})
      true -> validate(stream, {ms, 0})
    end
  end

  defp resolveId(stream, {:full, ms, seq}, _now) do
    validate(stream, {ms, seq})
  end

  defp validate(stream, {0, 0}), do: validateAgainst(stream, {0, 0}, :zero)
  defp validate(stream, id), do: validateAgainst(stream, id, :normal)

  defp validateAgainst(_stream, {0, 0}, :zero), do: {:error, :zero}

  defp validateAgainst(stream, id, _kind) do
    if id > stream.lastId, do: {:ok, id}, else: {:error, :smaller}
  end

  defp collectForward(_iterator, _upperOk, 0, acc), do: Enum.reverse(acc)

  defp collectForward(iterator, upperOk, count, acc) do
    case :gb_trees.next(iterator) do
      :none ->
        Enum.reverse(acc)

      {id, fields, nextIterator} ->
        if upperOk.(id) do
          remaining = if count == :all, do: :all, else: count - 1
          collectForward(nextIterator, upperOk, remaining, [{id, fields} | acc])
        else
          Enum.reverse(acc)
        end
    end
  end

  defp take(list, :all), do: list
  defp take(list, count), do: Enum.take(list, count)

  defp dropSmallest(stream, 0, removed), do: {stream, removed}

  defp dropSmallest(stream, remaining, removed) do
    {id, _fields, entries} = :gb_trees.take_smallest(stream.entries)
    maxDeleted = max(stream.maxDeletedId, id)
    updated = %{stream | entries: entries, length: stream.length - 1, maxDeletedId: maxDeleted}
    dropSmallest(updated, remaining - 1, removed + 1)
  end

  defp dropBelow(stream, minId, removed) do
    case :gb_trees.is_empty(stream.entries) do
      true ->
        {stream, removed}

      false ->
        {id, _fields} = :gb_trees.smallest(stream.entries)

        if id < minId do
          {_, _, entries} = :gb_trees.take_smallest(stream.entries)
          maxDeleted = max(stream.maxDeletedId, id)

          updated = %{
            stream
            | entries: entries,
              length: stream.length - 1,
              maxDeletedId: maxDeleted
          }

          dropBelow(updated, minId, removed + 1)
        else
          {stream, removed}
        end
    end
  end

  defp rangeBound(raw, defaultSeq, kind) do
    case parseId(raw, defaultSeq) do
      {:ok, {:autoSeq, _ms}} -> :error
      {:ok, id} -> {:ok, {kind, id}}
      :error -> :error
    end
  end

  defp withInteger(raw, continue) do
    case Integer.parse(raw) do
      {value, ""} when value >= 0 -> continue.(value)
      _ -> :error
    end
  end

  defp maxMs, do: 0xFFFFFFFFFFFFFFFF
  defp maxSeq, do: 0xFFFFFFFFFFFFFFFF
end
