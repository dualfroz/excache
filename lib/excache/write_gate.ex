defmodule Excache.WriteGate do
  @moduledoc false

  @inflight 1
  @closed 2

  def init, do: :persistent_term.put(__MODULE__, :atomics.new(2, signed: true))

  def pass(fun) do
    gate = gate()
    enter(gate)

    try do
      fun.()
    after
      :atomics.sub(gate, @inflight, 1)
    end
  end

  def exclusive(fun) do
    gate = gate()
    close(gate)
    awaitDrained(gate)

    try do
      fun.()
    after
      :atomics.put(gate, @closed, 0)
    end
  end

  defp gate, do: :persistent_term.get(__MODULE__)

  defp enter(gate) do
    awaitOpen(gate)
    :atomics.add(gate, @inflight, 1)

    if :atomics.get(gate, @closed) == 1 do
      :atomics.sub(gate, @inflight, 1)
      enter(gate)
    end
  end

  defp awaitOpen(gate) do
    if :atomics.get(gate, @closed) == 1 do
      Process.sleep(1)
      awaitOpen(gate)
    end
  end

  defp close(gate) do
    unless :atomics.compare_exchange(gate, @closed, 0, 1) == :ok do
      Process.sleep(1)
      close(gate)
    end
  end

  defp awaitDrained(gate) do
    if :atomics.get(gate, @inflight) > 0 do
      Process.sleep(1)
      awaitDrained(gate)
    end
  end
end
