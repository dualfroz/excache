defmodule Excache.PubSub do
  @moduledoc false

  @registry Excache.PubSub.Registry

  def subscribe(channel), do: Registry.register(@registry, channel, nil)
  def unsubscribe(channel), do: Registry.unregister(@registry, channel)

  def publish(channel, payload) do
    subscribers = Registry.lookup(@registry, channel)
    Enum.each(subscribers, fn {pid, _} -> send(pid, {:pubsub, channel, payload}) end)
    length(subscribers)
  end
end
