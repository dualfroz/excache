defmodule Excache.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    port = Application.get_env(:excache, :port, 6379)
    Excache.WriteGate.init()

    children = [
      {Registry, keys: :duplicate, name: Excache.PubSub.Registry},
      Excache.Store.Supervisor,
      Excache.Aof,
      Excache.Blocking,
      Excache.Expiry,
      trackerSpec(),
      Excache.Eviction.Runner,
      {DynamicSupervisor, strategy: :one_for_one, name: Excache.Server.ConnectionSupervisor},
      Excache.Clients
    ]

    case Supervisor.start_link(children, strategy: :one_for_one, name: Excache.Supervisor) do
      {:ok, supervisor} ->
        Excache.Aof.replay(Excache.Aof.path())
        start_acceptor(supervisor, port)

      error ->
        error
    end
  end

  def trackerSpec do
    {Excache.Eviction.Tracker,
     [
       name: Excache.Eviction.Tracker,
       logFactor: Excache.Config.lfuLogFactor(),
       decayTime: Excache.Config.lfuDecayTime()
     ]}
  end

  defp start_acceptor(supervisor, port) when is_integer(port) do
    case Supervisor.start_child(supervisor, {Excache.Server.Acceptor, [port: port]}) do
      {:ok, _pid} -> {:ok, supervisor}
      error -> error
    end
  end

  defp start_acceptor(supervisor, _port), do: {:ok, supervisor}
end
