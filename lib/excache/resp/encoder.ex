defmodule Excache.RESP.Encoder do
  @moduledoc false

  @crlf "\r\n"

  @type value ::
          {:simple, iodata}
          | {:error, iodata}
          | {:integer, integer}
          | {:bulk, binary | nil}
          | {:array, [value] | nil}

  @spec encode(value) :: iodata
  def encode({:simple, text}), do: ["+", text, @crlf]
  def encode({:error, text}), do: ["-", text, @crlf]
  def encode({:integer, number}), do: [":", Integer.to_string(number), @crlf]
  def encode({:bulk, nil}), do: ["$-1", @crlf]
  def encode({:bulk, bin}), do: ["$", Integer.to_string(byte_size(bin)), @crlf, bin, @crlf]
  def encode({:array, nil}), do: ["*-1", @crlf]

  def encode({:array, items}) do
    ["*", Integer.to_string(length(items)), @crlf, Enum.map(items, &encode/1)]
  end
end
