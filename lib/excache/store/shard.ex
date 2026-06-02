defmodule Excache.Store.Shard do
  @moduledoc false

  use GenServer

  alias Excache.Store.Ops

  def start_link({index, shard_count}) do
    GenServer.start_link(__MODULE__, {index, shard_count}, name: Excache.Store.shard_name(index))
  end

  @impl true
  def init({index, shard_count}) do
    table =
      :ets.new(:excache_shard, [
        :set,
        :protected,
        read_concurrency: true,
        write_concurrency: true
      ])

    load_snapshot(table, index, shard_count, Excache.Persistence.snapshot_path())
    {:ok, %{index: index, table: table}}
  end

  @impl true
  def handle_call({:op, op}, _from, state) do
    now = System.system_time(:millisecond)
    {:reply, Ops.apply_op(state.table, now, op), state}
  end

  @impl true
  def handle_call(:dump, _from, state) do
    {:reply, :ets.tab2list(state.table), state}
  end

  defp load_snapshot(table, index, shard_count, snapshot_path) do
    now = System.system_time(:millisecond)

    for {key, _kind, _value, ea} = entry <- Excache.Persistence.load_entries(snapshot_path),
        rem(:erlang.phash2(key), shard_count) == index,
        ea == nil or ea > now do
      :ets.insert(table, entry)
    end
  end
end
