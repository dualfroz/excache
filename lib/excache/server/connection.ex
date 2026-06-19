defmodule Excache.Server.Connection do
  @moduledoc false

  use GenServer, restart: :temporary

  alias Excache.RESP.{Parser, Encoder}
  alias Excache.Command
  alias Excache.Command.Transaction

  def start_link(socket), do: GenServer.start_link(__MODULE__, socket)

  @impl true
  def init(socket) do
    base = %{socket: socket, buffer: "", subscriptions: MapSet.new()}
    {:ok, Map.merge(base, Transaction.initial_state())}
  end

  @impl true
  def handle_info(:socket_ready, state) do
    :inet.setopts(state.socket, active: :once)
    {:noreply, state}
  end

  def handle_info({:tcp, socket, data}, state) do
    {output, state} = consume(state.buffer <> data, %{state | buffer: ""})

    unless output == [] do
      :gen_tcp.send(socket, output)
    end

    :inet.setopts(socket, active: :once)
    {:noreply, state}
  end

  def handle_info({:tcp_closed, _socket}, state), do: {:stop, :normal, state}
  def handle_info({:tcp_error, _socket, _reason}, state), do: {:stop, :normal, state}

  def handle_info({:pubsub, channel, payload}, state) do
    message = Encoder.encode({:array, [{:bulk, "message"}, {:bulk, channel}, {:bulk, payload}]})
    :gen_tcp.send(state.socket, message)
    {:noreply, state}
  end

  defp consume(buffer, state) do
    case Parser.parse_command(buffer) do
      :incomplete ->
        {[], %{state | buffer: buffer}}

      {:error, reason} ->
        {[Encoder.encode({:error, "ERR Protocol error: #{reason}"})], %{state | buffer: ""}}

      {:ok, args, rest} ->
        {replies, state} = Command.execute(args, state)
        encoded = Enum.map(replies, &Encoder.encode/1)
        {more, state} = consume(rest, state)
        {[encoded | more], state}
    end
  end
end
