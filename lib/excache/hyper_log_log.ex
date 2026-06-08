defmodule Excache.HyperLogLog do
  @moduledoc false

  import Bitwise

  @precision 14
  @registerCount 1 <<< @precision
  @tailBits 64 - @precision
  @magic "EXHLL1"
  def registerCount, do: @registerCount

  def empty do
    @magic <> :binary.copy(<<0>>, @registerCount)
  end

  def valid?(<<@magic, rest::binary>>) when byte_size(rest) == @registerCount, do: true
  def valid?(_), do: false

  def add(binary, elements) do
    registers = registersOf(binary)

    {updated, changed} =
      Enum.reduce(elements, {registers, false}, fn element, {acc, touched} ->
        {index, rank} = placement(element)
        current = :binary.at(acc, index)

        if rank > current do
          {replaceByte(acc, index, rank), true}
        else
          {acc, touched}
        end
      end)

    {@magic <> updated, changed}
  end

  def count(binary), do: estimate(registersOf(binary))

  def merge(binaries) do
    merged =
      binaries
      |> Enum.map(&registersOf/1)
      |> Enum.reduce(:binary.copy(<<0>>, @registerCount), fn registers, acc ->
        maxRegisters(acc, registers)
      end)

    @magic <> merged
  end

  def countMerged(binaries) do
    binaries
    |> merge()
    |> count()
  end

  defp registersOf(<<@magic, registers::binary-size(@registerCount)>>), do: registers

  defp maxRegisters(left, right) do
    for index <- 0..(@registerCount - 1), into: <<>> do
      <<max(:binary.at(left, index), :binary.at(right, index))>>
    end
  end

  defp placement(element) do
    <<hash::unsigned-integer-size(64), _::binary>> = :crypto.hash(:sha256, element)
    index = hash >>> @tailBits
    tail = hash &&& (1 <<< @tailBits) - 1
    {index, rank(tail) + 1}
  end

  defp rank(tail) when tail == 0, do: @tailBits
  defp rank(tail), do: @tailBits - bitLength(tail)

  defp bitLength(0), do: 0
  defp bitLength(value), do: bitLength(value >>> 1) + 1

  defp replaceByte(binary, index, byte) do
    <<head::binary-size(index), _::8, tail::binary>> = binary
    <<head::binary, byte::8, tail::binary>>
  end

  defp estimate(registers) do
    {harmonic, zeros} =
      Enum.reduce(0..(@registerCount - 1), {0.0, 0}, fn index, {sum, empties} ->
        value = :binary.at(registers, index)
        empties = if value == 0, do: empties + 1, else: empties
        {sum + :math.pow(2, -value), empties}
      end)

    raw = alpha() * @registerCount * @registerCount / harmonic

    corrected =
      cond do
        raw <= 2.5 * @registerCount and zeros != 0 ->
          @registerCount * :math.log(@registerCount / zeros)

        true ->
          raw
      end

    round(corrected)
  end

  defp alpha, do: 0.7213 / (1 + 1.079 / @registerCount)
end
