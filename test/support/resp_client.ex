defmodule Excache.Test.RespClient do
  @moduledoc false

  def connect(port) do
    {:ok, socket} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])

    %{socket: socket, buffer: ""}
  end

  def close(%{socket: socket}), do: :gen_tcp.close(socket)

  def command(conn, args) do
    :ok = :gen_tcp.send(conn.socket, encode_request(args))
    read(conn)
  end

  def send_raw(conn, bytes) do
    :ok = :gen_tcp.send(conn.socket, bytes)
    conn
  end

  def read(conn) do
    {<<type, payload::binary>>, conn} = read_line(conn)

    case type do
      ?+ -> {{:simple, payload}, conn}
      ?- -> {{:error, payload}, conn}
      ?: -> {String.to_integer(payload), conn}
      ?$ -> read_bulk(String.to_integer(payload), conn)
      ?* -> read_array(String.to_integer(payload), conn)
    end
  end

  defp read_bulk(length, conn) when length < 0, do: {nil, conn}

  defp read_bulk(length, conn) do
    conn = fill(conn, length + 2)
    <<data::binary-size(length), "\r\n", rest::binary>> = conn.buffer
    {data, %{conn | buffer: rest}}
  end

  defp read_array(count, conn) when count < 0, do: {nil, conn}

  defp read_array(count, conn), do: read_elements(count, conn, [])

  defp read_elements(0, conn, acc), do: {Enum.reverse(acc), conn}

  defp read_elements(count, conn, acc) do
    {value, conn} = read(conn)
    read_elements(count - 1, conn, [value | acc])
  end

  defp read_line(conn) do
    case :binary.split(conn.buffer, "\r\n") do
      [line, rest] ->
        {line, %{conn | buffer: rest}}

      [_only] ->
        {:ok, more} = :gen_tcp.recv(conn.socket, 0, 2000)
        read_line(%{conn | buffer: conn.buffer <> more})
    end
  end

  defp fill(conn, needed) do
    if byte_size(conn.buffer) >= needed do
      conn
    else
      {:ok, more} = :gen_tcp.recv(conn.socket, 0, 2000)
      fill(%{conn | buffer: conn.buffer <> more}, needed)
    end
  end

  defp encode_request(args) do
    [
      "*",
      Integer.to_string(length(args)),
      "\r\n",
      Enum.map(args, fn arg ->
        ["$", Integer.to_string(byte_size(arg)), "\r\n", arg, "\r\n"]
      end)
    ]
  end
end
