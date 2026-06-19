defmodule Excache.Server.Acceptor do
  @moduledoc false

  use GenServer

  @listen_opts [:binary, packet: :raw, active: false, reuseaddr: true, backlog: 1024]

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def port(server), do: GenServer.call(server, :port)

  @impl true
  def init(opts) do
    requested = Keyword.get(opts, :port, 0)
    {:ok, listen} = :gen_tcp.listen(requested, @listen_opts)
    {:ok, bound} = :inet.port(listen)

    for _ <- 1..Keyword.get(opts, :acceptors, 4) do
      spawn_link(fn -> accept_loop(listen) end)
    end

    {:ok, %{listen: listen, port: bound}}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  defp accept_loop(listen) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            Excache.Server.ConnectionSupervisor,
            {Excache.Server.Connection, socket}
          )

        case :gen_tcp.controlling_process(socket, pid) do
          :ok -> send(pid, :socket_ready)
          {:error, _} -> :gen_tcp.close(socket)
        end

        accept_loop(listen)

      {:error, :closed} ->
        :ok

      {:error, _reason} ->
        accept_loop(listen)
    end
  end
end
