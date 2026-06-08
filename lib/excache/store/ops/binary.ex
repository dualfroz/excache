defmodule Excache.Store.Ops.Binary do
  @moduledoc false

  alias Excache.{Bitmap, Bitfield, HyperLogLog}
  alias Excache.Store.Ops.Helpers

  def apply_op(table, now, {:setbit, key, offset, value}) do
    case fetchString(table, key, now) do
      :wrongtype ->
        {:error, :wrongtype}

      {:ok, current, ea, _exists} ->
        {updated, old} = Bitmap.setBit(current, offset, value)
        store(table, key, updated, ea)
        {:ok, old}
    end
  end

  def apply_op(table, now, {:getbit, key, offset}) do
    withString(table, key, now, fn value -> {:ok, Bitmap.getBit(value, offset)} end)
  end

  def apply_op(table, now, {:bitcount, key, range}) do
    withString(table, key, now, fn value -> {:ok, Bitmap.count(value, range)} end)
  end

  def apply_op(table, now, {:bitpos, key, bit, range, endGiven}) do
    withString(table, key, now, fn value ->
      {:ok, Bitmap.position(value, bit, range, endGiven)}
    end)
  end

  def apply_op(table, now, {:bitfield, key, operations, writes?}) do
    case fetchString(table, key, now) do
      :wrongtype ->
        {:error, :wrongtype}

      {:ok, current, ea, _exists} ->
        {updated, results} = Bitfield.run(current, operations)
        if writes?, do: store(table, key, updated, ea)
        {:ok, results}
    end
  end

  def apply_op(table, now, {:strlen, key}) do
    withString(table, key, now, fn value -> {:ok, byte_size(value)} end)
  end

  def apply_op(table, now, {:getrange, key, start, stop}) do
    withString(table, key, now, fn value -> {:ok, substring(value, start, stop)} end)
  end

  def apply_op(table, now, {:setrange, key, offset, value}) do
    case fetchString(table, key, now) do
      :wrongtype ->
        {:error, :wrongtype}

      {:ok, current, ea, _exists} ->
        updated = overwrite(current, offset, value)
        store(table, key, updated, ea)
        {:ok, byte_size(updated)}
    end
  end

  def apply_op(table, now, {:pfadd, key, elements}) do
    case fetchHll(table, key, now) do
      :wrongtype ->
        {:error, :wrongtype}

      :not_hll ->
        {:error, :not_hll}

      {:ok, current, ea, exists} ->
        base = if exists, do: current, else: HyperLogLog.empty()
        {updated, changed} = HyperLogLog.add(base, elements)

        cond do
          changed -> store(table, key, updated, ea)
          not exists -> store(table, key, updated, ea)
          true -> :ok
        end

        {:ok, if(changed or not exists, do: 1, else: 0)}
    end
  end

  def apply_op(table, now, {:pfget, key}) do
    case fetchHll(table, key, now) do
      :wrongtype -> {:error, :wrongtype}
      :not_hll -> {:error, :not_hll}
      {:ok, _current, _ea, false} -> {:ok, nil}
      {:ok, current, _ea, true} -> {:ok, current}
    end
  end

  def apply_op(table, now, {:pfstore, key, binary}) do
    ea =
      case Helpers.fetch(table, key, now) do
        {:string, _value, existingEa} -> existingEa
        _ -> nil
      end

    store(table, key, binary, ea)
    {:ok, "OK"}
  end

  defp fetchString(table, key, now) do
    case Helpers.fetch(table, key, now) do
      :miss -> {:ok, "", nil, false}
      {:string, value, ea} -> {:ok, value, ea, true}
      _ -> :wrongtype
    end
  end

  defp fetchHll(table, key, now) do
    case Helpers.fetch(table, key, now) do
      :miss ->
        {:ok, "", nil, false}

      {:string, value, ea} ->
        if HyperLogLog.valid?(value), do: {:ok, value, ea, true}, else: :not_hll

      _ ->
        :wrongtype
    end
  end

  defp withString(table, key, now, fun) do
    case fetchString(table, key, now) do
      :wrongtype -> {:error, :wrongtype}
      {:ok, value, _ea, _exists} -> fun.(value)
    end
  end

  defp store(table, key, value, ea) do
    :ets.insert(table, {key, :string, value, ea})
  end

  defp substring(value, start, stop) do
    size = byte_size(value)

    if size == 0 do
      ""
    else
      lower = max(normalize(start, size), 0)
      upper = min(normalize(stop, size), size - 1)

      if lower > upper, do: "", else: :binary.part(value, lower, upper - lower + 1)
    end
  end

  defp overwrite(current, offset, value) when value == "", do: current

  defp overwrite(current, offset, value) do
    size = byte_size(current)
    padded = if size < offset, do: current <> :binary.copy(<<0>>, offset - size), else: current
    head = :binary.part(padded, 0, offset)
    tailStart = offset + byte_size(value)

    tail =
      if byte_size(padded) > tailStart do
        :binary.part(padded, tailStart, byte_size(padded) - tailStart)
      else
        ""
      end

    head <> value <> tail
  end

  defp normalize(index, size) when index < 0, do: size + index
  defp normalize(index, _size), do: index
end
