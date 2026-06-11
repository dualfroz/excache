defmodule Excache.Stream.Group do
  @moduledoc false

  defstruct lastDeliveredId: {0, 0}, pending: %{}, consumers: %{}, entriesRead: 0
end
