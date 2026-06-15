defmodule Excache.Aof do
  @moduledoc false

  use GenServer

  alias Excache.RESP.{Encoder, Parser}

  def start_link(_arg), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def path do
    Application.get_env(:excache, :aof_path) ||
      Path.join(to_string(:code.priv_dir(:excache)), "excache.aof")
  end

  def fsync_policy, do: Application.get_env(:excache, :aof_fsync, :everysec)

  def append(command, args), do: GenServer.cast(__MODULE__, {:append, command, args})

  def flush, do: GenServer.call(__MODULE__, :flush)

  def restart do
    Supervisor.terminate_child(Excache.Supervisor, __MODULE__)
    Supervisor.restart_child(Excache.Supervisor, __MODULE__)
    :ok
  end

  def replay(file_path) do
    case File.read(file_path) do
      {:ok, binary} -> replay_commands(binary)
      {:error, _reason} -> :ok
    end
  end

  @impl true
  def init(_arg) do
    file_path = path()
    File.mkdir_p!(Path.dirname(file_path))
    {:ok, device} = File.open(file_path, [:append, :binary])
    schedule_fsync(fsync_policy())
    {:ok, %{device: device}}
  end

  @impl true
  def handle_cast({:append, command, args}, state) do
    IO.binwrite(state.device, Encoder.encode({:array, bulks([command | args])}))
    if fsync_policy() == :always, do: :file.sync(state.device)
    {:noreply, state}
  end

  @impl true
  def handle_call(:flush, _from, state) do
    :file.sync(state.device)
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:fsync_tick, state) do
    if fsync_policy() == :everysec, do: :file.sync(state.device)
    schedule_fsync(fsync_policy())
    {:noreply, state}
  end

  defp replay_commands(<<>>), do: :ok

  defp replay_commands(binary) do
    case Parser.parse_command(binary) do
      {:ok, [], rest} ->
        replay_commands(rest)

      {:ok, [name | args], rest} ->
        Excache.Command.replay_dispatch(String.upcase(name), args)
        replay_commands(rest)

      _incomplete_or_error ->
        :ok
    end
  end

  defp bulks(values), do: Enum.map(values, fn value -> {:bulk, value} end)

  defp schedule_fsync(:everysec), do: Process.send_after(self(), :fsync_tick, 1000)
  defp schedule_fsync(_policy), do: :ok
end
