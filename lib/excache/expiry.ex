defmodule Excache.Expiry do
  @moduledoc false

  use GenServer

  def start_link(_arg), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(_arg) do
    interval = Application.get_env(:excache, :sweep_interval, 1000)
    schedule(interval)
    {:ok, interval}
  end

  @impl true
  def handle_info(:sweep, interval) do
    Excache.Store.sweep() |> Enum.each(&Excache.Notify.expired/1)
    schedule(interval)
    {:noreply, interval}
  end

  defp schedule(interval), do: Process.send_after(self(), :sweep, interval)
end
