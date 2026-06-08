defmodule Excache.Bitmap do
  @moduledoc false

  import Bitwise

  def getBit(binary, offset) do
    byteIndex = offset >>> 3

    if byteIndex >= byte_size(binary) do
      0
    else
      bitIndex = 7 - (offset &&& 7)
      :binary.at(binary, byteIndex) >>> bitIndex &&& 1
    end
  end

  def setBit(binary, offset, value) do
    byteIndex = offset >>> 3
    bitIndex = 7 - (offset &&& 7)
    padded = extend(binary, byteIndex + 1)
    byte = :binary.at(padded, byteIndex)
    old = byte >>> bitIndex &&& 1

    updated =
      case value do
        1 -> byte ||| 1 <<< bitIndex
        0 -> byte &&& bnot(1 <<< bitIndex)
      end

    {replaceByte(padded, byteIndex, updated), old}
  end

  def count(binary, range) do
    case windowOf(binary, range) do
      :empty -> 0
      {window, _absStart} -> popcount(window)
    end
  end

  def position(binary, bit, range, endGiven) do
    case windowOf(binary, range) do
      :empty ->
        if bit == 0 and not endGiven, do: bit_size(binary), else: -1

      {window, absStart} ->
        case findBit(window, bit, 0) do
          nil ->
            if bit == 0 and not endGiven, do: bit_size(binary), else: -1

          localIndex ->
            absStart + localIndex
        end
    end
  end

  def combine(:not, [binary]) do
    for <<byte <- binary>>, into: <<>>, do: <<bnot(byte) &&& 0xFF>>
  end

  def combine(operation, binaries) do
    width = binaries |> Enum.map(&byte_size/1) |> Enum.max(fn -> 0 end)
    aligned = Enum.map(binaries, &extend(&1, width))

    for index <- 0..(width - 1), width > 0, into: <<>> do
      bytes = Enum.map(aligned, &:binary.at(&1, index))
      <<foldBytes(operation, bytes)>>
    end
  end

  defp foldBytes(:and, [first | rest]), do: Enum.reduce(rest, first, &(&1 &&& &2))
  defp foldBytes(:or, [first | rest]), do: Enum.reduce(rest, first, &(&1 ||| &2))
  defp foldBytes(:xor, [first | rest]), do: Enum.reduce(rest, first, &bxor(&1, &2))

  defp windowOf(binary, :whole) do
    if binary == <<>>, do: :empty, else: {binary, 0}
  end

  defp windowOf(binary, {:byte, start, stop}) do
    size = byte_size(binary)
    lower = clampIndex(start, size)
    upper = clampIndex(stop, size)

    if size == 0 or lower >= size or lower > upper do
      :empty
    else
      upper = min(upper, size - 1)
      {:binary.part(binary, lower, upper - lower + 1), lower * 8}
    end
  end

  defp windowOf(binary, {:bit, start, stop}) do
    size = bit_size(binary)
    lower = clampIndex(start, size)
    upper = clampIndex(stop, size)

    if size == 0 or lower >= size or lower > upper do
      :empty
    else
      upper = min(upper, size - 1)
      len = upper - lower + 1
      <<_::size(lower), window::bitstring-size(len), _::bitstring>> = binary
      {window, lower}
    end
  end

  defp popcount(window), do: for(<<bit::1 <- window>>, do: bit) |> Enum.sum()

  defp findBit(<<value::1, _rest::bitstring>>, bit, position) when value == bit, do: position
  defp findBit(<<_::1, rest::bitstring>>, bit, position), do: findBit(rest, bit, position + 1)
  defp findBit(<<>>, _bit, _position), do: nil

  defp clampIndex(index, size) when index < 0, do: max(size + index, 0)
  defp clampIndex(index, _size), do: index

  defp extend(binary, width) do
    missing = width - byte_size(binary)
    if missing > 0, do: binary <> :binary.copy(<<0>>, missing), else: binary
  end

  defp replaceByte(binary, index, byte) do
    <<head::binary-size(index), _::8, tail::binary>> = binary
    <<head::binary, byte::8, tail::binary>>
  end
end
