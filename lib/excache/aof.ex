defmodule Excache.Aof do
  @moduledoc false

  use GenServer

  alias Excache.RESP.{Encoder, Parser}

  @appended {__MODULE__, :appended}

  def start_link(_arg), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def path do
    Application.get_env(:excache, :aof_path) ||
      Path.join(to_string(:code.priv_dir(:excache)), "excache.aof")
  end

  def fsync_policy, do: Application.get_env(:excache, :aof_fsync, :everysec)

  def append(command, args) do
    :atomics.add(appendCounter(), 1, 1)
    GenServer.cast(__MODULE__, {:append, command, args})
  end

  def flush, do: GenServer.call(__MODULE__, :flush)

  def truncate, do: GenServer.call(__MODULE__, :truncate)

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
    ensureAppendCounter()
    {:ok, device} = File.open(file_path, [:append, :binary])
    schedule_fsync(fsync_policy())
    {:ok, %{device: device, path: file_path, written: :atomics.get(appendCounter(), 1)}}
  end

  @impl true
  def handle_cast({:append, command, args}, state), do: {:noreply, write(state, command, args)}

  @impl true
  def handle_call(:flush, _from, state) do
    state = drain(state)
    :file.sync(state.device)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:truncate, _from, state) do
    state = drain(state)
    File.close(state.device)
    {:ok, device} = File.open(state.path, [:write, :binary])
    :file.sync(device)
    {:reply, :ok, %{state | device: device}}
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

  defp write(state, command, args) do
    IO.binwrite(state.device, Encoder.encode({:array, bulks([command | args])}))
    if fsync_policy() == :always, do: :file.sync(state.device)
    %{state | written: state.written + 1}
  end

  defp drain(state) do
    if state.written >= :atomics.get(appendCounter(), 1) do
      state
    else
      receive do
        {:"$gen_cast", {:append, command, args}} -> drain(write(state, command, args))
      end
    end
  end

  defp ensureAppendCounter do
    unless :persistent_term.get(@appended, nil) do
      :persistent_term.put(@appended, :atomics.new(1, signed: false))
    end
  end

  defp appendCounter, do: :persistent_term.get(@appended)

  defp bulks(values), do: Enum.map(values, fn value -> {:bulk, value} end)

  defp schedule_fsync(:everysec), do: Process.send_after(self(), :fsync_tick, 1000)
  defp schedule_fsync(_policy), do: :ok
end
