defmodule Excache do
  @moduledoc """
  In-memory key-value server speaking a subset of the RESP2 protocol.
  """

  defdelegate save(), to: Excache.Persistence
  defdelegate flushdb(), to: Excache.Store
  defdelegate dbsize(), to: Excache.Store
end
