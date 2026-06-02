defmodule Excache.RESP.Parser do
  @moduledoc false

  @type result :: {:ok, [binary], binary} | :incomplete | {:error, String.t()}

  @spec parse_command(binary) :: result
  def parse_command(<<>>), do: :incomplete
  def parse_command(<<"*", _::binary>> = buffer), do: parse_multibulk(buffer)
  def parse_command(buffer), do: parse_inline(buffer)

  defp parse_inline(buffer) do
    case read_line(buffer) do
      :incomplete -> :incomplete
      {:ok, line, rest} -> {:ok, tokenize(line), rest}
    end
  end

  defp tokenize(line) do
    line
    |> String.split([" ", "\t"], trim: true)
  end

  defp parse_multibulk(buffer) do
    case read_line(buffer) do
      :incomplete ->
        :incomplete

      {:ok, <<"*", digits::binary>>, rest} ->
        case Integer.parse(digits) do
          {count, ""} when count <= 0 -> {:ok, [], rest}
          {count, ""} -> parse_bulks(count, rest, [])
          _ -> {:error, "invalid multibulk length"}
        end

      {:ok, _other, _rest} ->
        {:error, "expected multibulk header"}
    end
  end

  defp parse_bulks(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp parse_bulks(remaining, buffer, acc) do
    case read_line(buffer) do
      :incomplete ->
        :incomplete

      {:ok, <<"$", digits::binary>>, rest} ->
        case Integer.parse(digits) do
          {len, ""} when len >= 0 -> take_bulk(len, rest, remaining, acc)
          {-1, ""} -> parse_bulks(remaining - 1, rest, [nil | acc])
          _ -> {:error, "invalid bulk length"}
        end

      {:ok, _other, _rest} ->
        {:error, "expected bulk header"}
    end
  end

  defp take_bulk(len, rest, remaining, acc) do
    needed = len + 2

    cond do
      byte_size(rest) < needed ->
        :incomplete

      binary_part(rest, len, 2) == "\r\n" ->
        data = binary_part(rest, 0, len)
        tail = binary_part(rest, needed, byte_size(rest) - needed)
        parse_bulks(remaining - 1, tail, [data | acc])

      true ->
        {:error, "bulk terminator missing"}
    end
  end

  defp read_line(buffer) do
    case :binary.split(buffer, "\n") do
      [line, rest] -> {:ok, strip_cr(line), rest}
      [_only] -> :incomplete
    end
  end

  defp strip_cr(line) do
    size = byte_size(line)

    if size > 0 and binary_part(line, size - 1, 1) == "\r" do
      binary_part(line, 0, size - 1)
    else
      line
    end
  end
end
