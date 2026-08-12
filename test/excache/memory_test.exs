defmodule Excache.MemoryTest do
  use ExUnit.Case, async: false

  alias Excache.Memory
  alias Excache.SkipList
  alias Excache.Store
  alias Excache.Stream, as: CacheStream

  setup do
    Store.flushdb()
    on_exit(&Store.flushdb/0)
    :ok
  end

  test "entry overhead matches the real cost of an ETS insert" do
    entries = for index <- 1..2000, do: {"probe:#{index}", :string, "value-#{index}", nil}

    table = :ets.new(:memoryProbe, [:set, :public])
    before = :ets.info(table, :memory)
    Enum.each(entries, &:ets.insert(table, &1))
    measured = (:ets.info(table, :memory) - before) * Memory.wordSize()
    :ets.delete(table)

    estimated = Memory.usedMemory(entries)

    assert measured > 0
    assert_in_delta estimated / measured, 1.0, 0.02
  end

  test "reference counted binaries are counted beyond the flat term size" do
    value = :binary.copy("x", 4096)
    flat = :erts_debug.size(value) * Memory.wordSize()

    assert flat < 200
    assert Memory.estimateValue(:string, value) == flat + 4096

    nested = [:binary.copy("a", 1024), :binary.copy("b", 1024)]
    assert Memory.estimateValue(:list, nested) >= 2048

    heapBinary = :binary.copy("y", 64)

    assert Memory.estimateValue(:string, heapBinary) ==
             :erts_debug.size(heapBinary) * Memory.wordSize()
  end

  test "estimates grow with the element count of every aggregate type" do
    pairs = [
      {:list, listValue(2), listValue(40)},
      {:hash, hashValue(2), hashValue(40)},
      {:set, setValue(2), setValue(40)},
      {:zset, zsetValue(2), zsetValue(40)},
      {:stream, streamValue(2), streamValue(40)}
    ]

    for {kind, small, large} <- pairs do
      assert Memory.estimateValue(kind, small) > 0, "#{kind} small estimate is not positive"

      assert Memory.estimateValue(kind, large) > Memory.estimateValue(kind, small),
             "#{kind} estimate did not grow with the element count"
    end
  end

  test "element counts and payload bytes are reported per type" do
    assert Memory.elementCount(:string, "abc") == 1
    assert Memory.payloadBytes(:string, "abc") == 3

    assert Memory.elementCount(:list, ["a", "bb"]) == 2
    assert Memory.payloadBytes(:list, ["a", "bb"]) == 3

    hash = %{"f1" => "v1", "f2" => "vvv2"}
    assert Memory.elementCount(:hash, hash) == 2
    assert Memory.payloadBytes(:hash, hash) == 2 + 2 + 2 + 4

    set = MapSet.new(["a", "bb", "ccc"])
    assert Memory.elementCount(:set, set) == 3
    assert Memory.payloadBytes(:set, set) == 6

    zset = zsetValue(2)
    assert Memory.elementCount(:zset, zset) == 2
    assert Memory.payloadBytes(:zset, zset) == 2 * (byte_size("member1") + Memory.wordSize())

    stream = streamValue(2)
    assert Memory.elementCount(:stream, stream) == 2

    assert Memory.payloadBytes(:stream, stream) ==
             2 * (2 * Memory.wordSize() + byte_size("field") + byte_size("value1"))
  end

  test "usedMemory sums every entry and usageByType splits them by kind" do
    entries = [
      {"s", :string, "value", nil},
      {"l", :list, listValue(3), nil},
      {"h", :hash, hashValue(3), nil},
      {"st", :set, setValue(3), nil},
      {"z", :zset, zsetValue(3), nil},
      {"x", :stream, streamValue(3), nil}
    ]

    assert Memory.usedMemory(entries) ==
             Enum.reduce(entries, 0, fn entry, total -> total + Memory.entryBytes(entry) end)

    byType = Memory.usageByType(entries)
    assert Map.keys(byType) |> Enum.sort() == Enum.sort(Memory.kinds())
    assert Enum.all?(byType, fn {_kind, usage} -> usage.keys == 1 end)

    assert byType.string.bytes == Memory.entryBytes({"s", :string, "value", nil})

    assert Enum.sum(Enum.map(byType, fn {_kind, usage} -> usage.bytes end)) ==
             Memory.usedMemory(entries)

    withoutStreams = Enum.reject(entries, fn {_key, kind, _value, _ea} -> kind == :stream end)
    assert Memory.usageByType(withoutStreams).stream == %{keys: 0, bytes: 0}
  end

  test "stats separate overhead from dataset and point at the largest key" do
    entries = [
      {"tiny", :string, "a", nil},
      {"huge", :string, :binary.copy("z", 8192), 1_700_000_000_000},
      {"mid", :list, listValue(10), nil}
    ]

    stats = Memory.stats(entries)

    assert stats.keys == 3
    assert stats.volatileKeys == 1
    assert stats.usedMemory == Memory.usedMemory(entries)
    assert stats.datasetBytes == stats.usedMemory - stats.overheadBytes
    assert stats.overheadBytes == 3 * Memory.entryOverhead() + overheadKeyBytes(entries)
    assert stats.bytesPerKey == div(stats.usedMemory, 3)
    assert {"huge", huge} = stats.largestKey
    assert huge >= 8192
    assert Memory.stats([]).largestKey == nil
    assert Memory.stats([]).bytesPerKey == 0
  end

  test "describe reports the volatile flag and separates payload from overhead" do
    value = :binary.copy("p", 512)
    described = Memory.describe("session:1", {:string, value, 1_700_000_000_000})

    assert described.kind == :string
    assert described.volatile
    assert described.payloadBytes == 512
    assert described.bytes == described.keyBytes + described.valueBytes + Memory.entryOverhead()
    assert described.overheadBytes == described.bytes - 512
    assert Memory.describe("k", {:string, "v", nil}).volatile == false
  end

  test "usage reads a live key and reports nil for a missing one" do
    :ok = Store.set("alpha", "value", [])
    {:ok, 3} = Store.rpush("beta", ["a", "b", "c"])

    assert Memory.usage("alpha") == Memory.estimateEntry("alpha", Store.watch_snapshot("alpha"))
    assert Memory.usage("ghost") == nil
    assert Memory.describeKey("ghost") == nil
    assert Memory.describeKey("beta").kind == :list
    assert Memory.describeKey("beta").elements == 3

    assert Memory.usedMemory() == Memory.usage("alpha") + Memory.usage("beta")
    assert Memory.usageByType().list.keys == 1
    assert Memory.stats().keys == 2
  end

  test "keyBytes grows with the key length" do
    assert Memory.keyBytes(String.duplicate("k", 200)) > Memory.keyBytes("k")
    assert Memory.entryOverhead() == 10 * Memory.wordSize()
  end

  defp overheadKeyBytes(entries) do
    Enum.reduce(entries, 0, fn {key, _kind, _value, _ea}, total ->
      total + Memory.keyBytes(key)
    end)
  end

  defp listValue(count), do: Enum.map(1..count, &"item#{&1}")

  defp hashValue(count), do: Map.new(1..count, fn index -> {"field#{index}", "value#{index}"} end)

  defp setValue(count), do: MapSet.new(1..count, &"member#{&1}")

  defp zsetValue(count) do
    Enum.reduce(1..count, SkipList.new(), fn index, zset ->
      SkipList.insert(zset, "member#{index}", index * 1.0)
    end)
  end

  defp streamValue(count) do
    Enum.reduce(1..count, CacheStream.new(), fn index, stream ->
      {:ok, updated, _id} =
        CacheStream.add(stream, {:full, index, 0}, ["field", "value#{index}"], 0)

      updated
    end)
  end
end
