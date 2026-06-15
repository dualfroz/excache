defmodule Excache.Command.HyperLogLog do
  @moduledoc false

  alias Excache.Store
  alias Excache.Command.Reply

  def dispatch("PFADD", [key | elements], state) do
    case Store.pfadd(key, elements) do
      {:ok, changed} -> reply({:integer, changed}, state)
      {:error, reason} -> reply(hllError(reason), state)
    end
  end

  def dispatch("PFCOUNT", [_ | _] = keys, state) do
    case Store.pfcount(keys) do
      {:ok, count} -> reply({:integer, count}, state)
      {:error, reason} -> reply(hllError(reason), state)
    end
  end

  def dispatch("PFMERGE", [dest | sources], state) do
    case Store.pfmerge(dest, sources) do
      {:ok, "OK"} -> reply({:simple, "OK"}, state)
      {:error, reason} -> reply(hllError(reason), state)
    end
  end

  def dispatch(_name, _args, _state), do: :unhandled

  defp hllError(:not_hll),
    do: {:error, "WRONGTYPE Key is not a valid HyperLogLog string value."}

  defp hllError(:wrongtype), do: Reply.wrongtype()
  defp hllError(other), do: {:error, "ERR #{other}"}

  defp reply(term, state), do: {[term], state}
end
