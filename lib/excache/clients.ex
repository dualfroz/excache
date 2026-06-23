defmodule Excache.Clients do
  @moduledoc false

  use GenServer

  @name __MODULE__
  @supervisor Excache.Server.ConnectionSupervisor
  @pubsubRegistry Excache.PubSub.Registry
  @defaultPoll 500
  @selfKillDelay 50

  def start_link(_arg), do: GenServer.start_link(__MODULE__, nil, name: @name)

  def snapshot, do: GenServer.call(@name, :snapshot)

  def find(pid), do: GenServer.call(@name, {:find, pid})

  def setName(pid, name), do: GenServer.call(@name, {:setName, pid, name})

  def kill(matcher, caller), do: GenServer.call(@name, {:kill, matcher, caller})

  def subscriptionCount(pid), do: length(Registry.keys(@pubsubRegistry, pid))

  def subscribedChannels do
    @pubsubRegistry
    |> Registry.select([{{:"$1", :_, :_}, [], [:"$1"]}])
    |> Enum.uniq()
    |> length()
  end

  def blocked?(pid) do
    case Process.info(pid, :monitored_by) do
      {:monitored_by, watchers} -> Process.whereis(Excache.Blocking) in watchers
      nil -> false
    end
  end

  @impl true
  def init(_arg) do
    schedule()

    {:ok,
     %{
       nextId: 1,
       clients: %{},
       monitors: %{},
       connectionsReceived: 0,
       closedNetIn: 0,
       closedNetOut: 0,
       runId: newRunId(),
       startedAt: System.system_time(:millisecond)
     }}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    state = sync(state)
    {:reply, buildSnapshot(state), state}
  end

  @impl true
  def handle_call({:find, pid}, _from, state) do
    state = sync(state)
    {:reply, Map.get(state.clients, pid), state}
  end

  @impl true
  def handle_call({:setName, pid, name}, _from, state) do
    state = sync(state)

    case Map.fetch(state.clients, pid) do
      {:ok, client} ->
        renamed = Map.put(state.clients, pid, %{client | name: name})
        {:reply, :ok, %{state | clients: renamed}}

      :error ->
        {:reply, :error, state}
    end
  end

  @impl true
  def handle_call({:kill, matcher, caller}, _from, state) do
    state = sync(state)
    victims = state.clients |> Map.values() |> Enum.filter(matcher)
    {itself, others} = Enum.split_with(victims, &(&1.pid == caller))

    Enum.each(others, fn client -> terminate(client.pid) end)

    Enum.each(itself, fn client ->
      Process.send_after(self(), {:terminate, client.pid}, @selfKillDelay)
    end)

    {:reply, length(victims), state}
  end

  @impl true
  def handle_info(:poll, state) do
    schedule()
    {:noreply, sync(state)}
  end

  @impl true
  def handle_info({:terminate, pid}, state) do
    terminate(pid)
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, pid, _reason}, state) do
    {_pid, monitors} = Map.pop(state.monitors, monitor)
    {client, clients} = Map.pop(state.clients, pid)
    {:noreply, accountClosed(%{state | monitors: monitors, clients: clients}, client)}
  end

  defp schedule do
    interval = Application.get_env(:excache, :clientsPollInterval, @defaultPoll)
    Process.send_after(self(), :poll, interval)
  end

  defp sync(state) do
    Enum.reduce(livePids(), state, fn pid, acc ->
      case Map.fetch(acc.clients, pid) do
        {:ok, client} -> %{acc | clients: Map.put(acc.clients, pid, refresh(client))}
        :error -> register(acc, pid)
      end
    end)
  end

  defp livePids do
    for {_id, pid, _type, _modules} <- DynamicSupervisor.which_children(@supervisor),
        is_pid(pid),
        do: pid
  end

  defp register(state, pid) do
    with socket when socket != nil <- socketOf(pid),
         {netIn, netOut} <- netStats(socket) do
      now = System.system_time(:millisecond)

      client = %{
        id: state.nextId,
        pid: pid,
        socket: socket,
        addr: addressOf(socket, &:inet.peername/1),
        laddr: addressOf(socket, &:inet.sockname/1),
        name: "",
        connectedAt: now,
        lastActiveAt: now,
        netIn: netIn,
        netOut: netOut
      }

      %{
        state
        | nextId: state.nextId + 1,
          clients: Map.put(state.clients, pid, client),
          monitors: Map.put(state.monitors, Process.monitor(pid), pid),
          connectionsReceived: state.connectionsReceived + 1
      }
    else
      _unavailable -> state
    end
  end

  defp refresh(client) do
    case netStats(client.socket) do
      {netIn, netOut} when netIn > client.netIn ->
        %{
          client
          | netIn: netIn,
            netOut: netOut,
            lastActiveAt: System.system_time(:millisecond)
        }

      {netIn, netOut} ->
        %{client | netIn: netIn, netOut: netOut}

      :closed ->
        client
    end
  end

  defp accountClosed(state, nil), do: state

  defp accountClosed(state, client) do
    %{
      state
      | closedNetIn: state.closedNetIn + client.netIn,
        closedNetOut: state.closedNetOut + client.netOut
    }
  end

  defp buildSnapshot(state) do
    clients = state.clients |> Map.values() |> Enum.sort_by(& &1.id)

    %{
      clients: clients,
      connectionsReceived: state.connectionsReceived,
      netIn: Enum.reduce(clients, state.closedNetIn, fn client, sum -> sum + client.netIn end),
      netOut: Enum.reduce(clients, state.closedNetOut, fn client, sum -> sum + client.netOut end),
      runId: state.runId,
      startedAt: state.startedAt
    }
  end

  defp terminate(pid), do: DynamicSupervisor.terminate_child(@supervisor, pid)

  defp socketOf(pid) do
    case Process.info(pid, :links) do
      {:links, links} -> Enum.find(links, &tcpPort?/1)
      nil -> nil
    end
  end

  defp tcpPort?(link) when is_port(link) do
    match?({:name, ~c"tcp_inet"}, Port.info(link, :name))
  end

  defp tcpPort?(_link), do: false

  defp netStats(socket) do
    case :inet.getstat(socket, [:recv_oct, :send_oct]) do
      {:ok, [recv_oct: netIn, send_oct: netOut]} -> {netIn, netOut}
      {:error, _reason} -> :closed
    end
  end

  defp addressOf(socket, fetch) do
    case fetch.(socket) do
      {:ok, {ip, port}} -> formatAddress(ip, port)
      {:error, _reason} -> ""
    end
  end

  defp formatAddress(ip, port) when tuple_size(ip) == 8, do: "[#{:inet.ntoa(ip)}]:#{port}"
  defp formatAddress(ip, port), do: "#{:inet.ntoa(ip)}:#{port}"

  defp newRunId, do: 20 |> :rand.bytes() |> Base.encode16(case: :lower)
end
