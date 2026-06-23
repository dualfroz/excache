defmodule Excache.Memory do
  @moduledoc false

  # Byte accounting for stored entries, the input every eviction policy needs.
  #
  # Structural cost comes from :erts_debug.size/1, which walks the term and
  # counts machine words (shared subterms once). Two things it cannot see are
  # added explicitly:
  #
  #   * reference counted binaries. ERTS keeps binaries up to 64 bytes on the
  #     heap, so :erts_debug.size/1 covers those; anything larger lives
  #     off-heap and only its ProcBin header is counted, so the payload is
  #     added by walking the term.
  #
  #   * the per-row cost of living in an ETS set. Measured against
  #     :ets.info(table, :memory) deltas on 64-bit OTP it is 41.24 bytes per
  #     row above the flat term size: five words of hash bucket bookkeeping
  #     plus an amortised bucket slot. Together with the five words of the
  #     {key, kind, value, expiresAt} tuple itself (header plus four slots)
  #     that is the ten word constant below; memory_test.exs asserts the
  #     resulting estimate against a real ETS insert.

  alias Excache.SkipList
  alias Excache.Store
  alias Excache.Stream, as: CacheStream

  @kinds [:string, :list, :hash, :set, :zset, :stream]
  @entryOverheadWords 10
  @heapBinaryLimit 64

  def kinds, do: @kinds

  def wordSize, do: :erlang.system_info(:wordsize)

  def entryOverhead, do: @entryOverheadWords * wordSize()

  def keyBytes(key) when is_binary(key), do: termBytes(key)

  def estimateEntry(key, {kind, value, _expiresAt}) do
    keyBytes(key) + estimateValue(kind, value) + entryOverhead()
  end

  def entryBytes({key, kind, value, expiresAt}),
    do: estimateEntry(key, {kind, value, expiresAt})

  def estimateValue(:string, value) when is_binary(value), do: termBytes(value)
  def estimateValue(:list, value) when is_list(value), do: termBytes(value)
  def estimateValue(:set, %MapSet{} = value), do: termBytes(value)
  def estimateValue(:zset, %SkipList{} = value), do: termBytes(value)
  def estimateValue(:stream, %CacheStream{} = value), do: termBytes(value)
  def estimateValue(:hash, value) when is_map(value), do: termBytes(value)

  def elementCount(:string, value) when is_binary(value), do: 1
  def elementCount(:list, value), do: length(value)
  def elementCount(:set, value), do: MapSet.size(value)
  def elementCount(:zset, value), do: SkipList.size(value)
  def elementCount(:stream, value), do: CacheStream.length(value)
  def elementCount(:hash, value) when is_map(value), do: map_size(value)

  def payloadBytes(:string, value), do: byte_size(value)

  def payloadBytes(:list, value), do: sumBytes(value)

  def payloadBytes(:set, value), do: value |> MapSet.to_list() |> sumBytes()

  def payloadBytes(:zset, value) do
    Enum.reduce(SkipList.to_list(value), 0, fn {_score, member}, total ->
      total + byte_size(member) + wordSize()
    end)
  end

  def payloadBytes(:stream, value) do
    Enum.reduce(CacheStream.toList(value), 0, fn {_id, fields}, total ->
      total + 2 * wordSize() + sumBytes(fields)
    end)
  end

  def payloadBytes(:hash, value) when is_map(value) do
    Enum.reduce(value, 0, fn {field, member}, total ->
      total + byte_size(field) + byte_size(member)
    end)
  end

  def describe(key, {kind, value, expiresAt}) do
    total = estimateEntry(key, {kind, value, expiresAt})
    payload = payloadBytes(kind, value)

    %{
      key: key,
      kind: kind,
      bytes: total,
      keyBytes: keyBytes(key),
      valueBytes: estimateValue(kind, value),
      payloadBytes: payload,
      overheadBytes: total - payload,
      elements: elementCount(kind, value),
      volatile: expiresAt != nil
    }
  end

  def usage(key) do
    case Store.watch_snapshot(key) do
      :miss -> nil
      entry -> estimateEntry(key, entry)
    end
  end

  def describeKey(key) do
    case Store.watch_snapshot(key) do
      :miss -> nil
      entry -> describe(key, entry)
    end
  end

  def usedMemory, do: usedMemory(Store.dump())

  def usedMemory(entries) when is_list(entries),
    do: Enum.reduce(entries, 0, fn entry, total -> total + entryBytes(entry) end)

  def usageByType, do: usageByType(Store.dump())

  def usageByType(entries) when is_list(entries) do
    empty = Map.new(@kinds, &{&1, %{keys: 0, bytes: 0}})

    Enum.reduce(entries, empty, fn {_key, kind, _value, _expiresAt} = entry, acc ->
      Map.update!(acc, kind, fn %{keys: keys, bytes: bytes} ->
        %{keys: keys + 1, bytes: bytes + entryBytes(entry)}
      end)
    end)
  end

  def stats, do: stats(Store.dump())

  def stats(entries) when is_list(entries) do
    keys = length(entries)
    used = usedMemory(entries)

    overhead =
      Enum.reduce(entries, keys * entryOverhead(), fn {key, _kind, _value, _expiresAt}, total ->
        total + keyBytes(key)
      end)

    %{
      keys: keys,
      usedMemory: used,
      overheadBytes: overhead,
      datasetBytes: used - overhead,
      bytesPerKey: if(keys == 0, do: 0, else: div(used, keys)),
      volatileKeys:
        Enum.count(entries, fn {_key, _kind, _value, expiresAt} -> expiresAt != nil end),
      byType: usageByType(entries),
      largestKey: largestKey(entries)
    }
  end

  defp largestKey([]), do: nil

  defp largestKey(entries) do
    entries
    |> Enum.map(fn {key, _kind, _value, _expiresAt} = entry -> {key, entryBytes(entry)} end)
    |> Enum.max_by(fn {_key, bytes} -> bytes end)
  end

  defp termBytes(term), do: :erts_debug.size(term) * wordSize() + refcBytes(term)

  defp sumBytes(binaries),
    do: Enum.reduce(binaries, 0, fn binary, total -> total + byte_size(binary) end)

  defp refcBytes(term) when is_binary(term) do
    size = byte_size(term)
    if size > @heapBinaryLimit, do: size, else: 0
  end

  defp refcBytes(term) when is_list(term),
    do: Enum.reduce(term, 0, fn element, total -> total + refcBytes(element) end)

  defp refcBytes(%_struct{} = term), do: term |> Map.from_struct() |> refcBytes()

  defp refcBytes(term) when is_map(term) do
    Enum.reduce(term, 0, fn {mapKey, mapValue}, total ->
      total + refcBytes(mapKey) + refcBytes(mapValue)
    end)
  end

  defp refcBytes(term) when is_tuple(term), do: term |> Tuple.to_list() |> refcBytes()

  defp refcBytes(_term), do: 0
end
