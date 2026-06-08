defmodule Excache.Bitfield do
  @moduledoc false

  import Bitwise

  def run(binary, operations) do
    {finalBinary, reversed} =
      Enum.reduce(operations, {binary, []}, fn operation, {bin, results} ->
        {updated, result} = applyOne(bin, operation)
        {updated, [result | results]}
      end)

    {finalBinary, Enum.reverse(reversed)}
  end

  defp applyOne(binary, {:get, signed, bits, offset}) do
    {binary, interpret(readBits(binary, offset, bits), signed, bits)}
  end

  defp applyOne(binary, {:set, signed, bits, offset, value, overflow}) do
    old = interpret(readBits(binary, offset, bits), signed, bits)

    case clamp(value, signed, bits, overflow) do
      :fail -> {binary, nil}
      stored -> {writeBits(binary, offset, bits, stored), old}
    end
  end

  defp applyOne(binary, {:incrby, signed, bits, offset, delta, overflow}) do
    old = interpret(readBits(binary, offset, bits), signed, bits)
    candidate = old + delta

    case clamp(candidate, signed, bits, overflow) do
      :fail ->
        {binary, nil}

      stored ->
        {writeBits(binary, offset, bits, stored), interpret(rawOf(stored, bits), signed, bits)}
    end
  end

  defp readBits(binary, offset, bits) do
    padded = padToBits(binary, offset + bits)
    <<_::size(offset), raw::size(bits), _::bitstring>> = padded
    raw
  end

  defp writeBits(binary, offset, bits, value) do
    padded = padToBits(binary, offset + bits)
    raw = rawOf(value, bits)
    <<head::size(offset), _::size(bits), tail::bitstring>> = padded
    <<head::size(offset), raw::size(bits), tail::bitstring>>
  end

  defp padToBits(binary, neededBits) do
    neededBytes = div(neededBits + 7, 8)
    missing = neededBytes - byte_size(binary)
    if missing > 0, do: binary <> :binary.copy(<<0>>, missing), else: binary
  end

  defp interpret(raw, false, _bits), do: raw

  defp interpret(raw, true, bits) do
    if raw >= 1 <<< (bits - 1), do: raw - (1 <<< bits), else: raw
  end

  defp rawOf(value, bits), do: value &&& (1 <<< bits) - 1

  defp clamp(value, signed, bits, overflow) do
    {low, high} = bounds(signed, bits)

    cond do
      value >= low and value <= high ->
        value

      overflow == :wrap ->
        wrap(value, signed, bits)

      overflow == :sat ->
        if value < low, do: low, else: high

      overflow == :fail ->
        :fail
    end
  end

  defp wrap(value, signed, bits) do
    raw = rawOf(value, bits)
    interpret(raw, signed, bits)
  end

  defp bounds(false, bits), do: {0, (1 <<< bits) - 1}
  defp bounds(true, bits), do: {-(1 <<< (bits - 1)), (1 <<< (bits - 1)) - 1}
end
