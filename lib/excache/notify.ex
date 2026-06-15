defmodule Excache.Notify do
  @moduledoc false

  alias Excache.PubSub

  def key_event(key, event) do
    if enabled?() do
      PubSub.publish(keyspace_channel(key), event)
      PubSub.publish(keyevent_channel(event), key)
    end

    :ok
  end

  def expired(key), do: key_event(key, "expired")

  def for_command("SET", [key | _]), do: key_event(key, "set")
  def for_command("DEL", keys), do: Enum.each(keys, &key_event(&1, "del"))
  def for_command("EXPIRE", [key | _]), do: key_event(key, "expire")
  def for_command("PEXPIRE", [key | _]), do: key_event(key, "expire")
  def for_command("PERSIST", [key | _]), do: key_event(key, "persist")
  def for_command("INCR", [key]), do: key_event(key, "incrby")
  def for_command("DECR", [key]), do: key_event(key, "decrby")
  def for_command("INCRBY", [key | _]), do: key_event(key, "incrby")
  def for_command("DECRBY", [key | _]), do: key_event(key, "decrby")
  def for_command("APPEND", [key | _]), do: key_event(key, "append")
  def for_command("GETSET", [key | _]), do: key_event(key, "set")

  def for_command("MSET", args),
    do: args |> Enum.chunk_every(2) |> Enum.each(fn [key, _] -> key_event(key, "set") end)

  def for_command("LPUSH", [key | _]), do: key_event(key, "lpush")
  def for_command("RPUSH", [key | _]), do: key_event(key, "rpush")
  def for_command("LPOP", [key | _]), do: key_event(key, "lpop")
  def for_command("RPOP", [key | _]), do: key_event(key, "rpop")
  def for_command("HSET", [key | _]), do: key_event(key, "hset")
  def for_command("HDEL", [key | _]), do: key_event(key, "hdel")
  def for_command("HINCRBY", [key | _]), do: key_event(key, "hincrby")
  def for_command("SADD", [key | _]), do: key_event(key, "sadd")
  def for_command("SREM", [key | _]), do: key_event(key, "srem")
  def for_command("ZADD", [key | _]), do: key_event(key, "zadd")
  def for_command("ZREM", [key | _]), do: key_event(key, "zrem")
  def for_command("ZINCRBY", [key | _]), do: key_event(key, "zincrby")
  def for_command("ZPOPMIN", [key | _]), do: key_event(key, "zpopmin")
  def for_command("ZPOPMAX", [key | _]), do: key_event(key, "zpopmax")
  def for_command("SINTERSTORE", [dest | _]), do: key_event(dest, "sinterstore")
  def for_command("SUNIONSTORE", [dest | _]), do: key_event(dest, "sunionstore")
  def for_command("SDIFFSTORE", [dest | _]), do: key_event(dest, "sdiffstore")
  def for_command(_command, _args), do: :ok

  defp enabled?, do: Application.get_env(:excache, :keyspace_notifications, true)

  defp keyspace_channel(key), do: "__keyspace@0__:#{key}"
  defp keyevent_channel(event), do: "__keyevent@0__:#{event}"
end
