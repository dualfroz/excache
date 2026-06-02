defmodule Excache.Store.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(_arg) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    count = Excache.Store.shard_count()

    children =
      for index <- 0..(count - 1) do
        Supervisor.child_spec({Excache.Store.Shard, {index, count}}, id: {:shard, index})
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
