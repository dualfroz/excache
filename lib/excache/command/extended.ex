defmodule Excache.Command.Extended do
  @moduledoc false

  alias Excache.Command.{Streams, Bitmaps, HyperLogLog, Keyspace, Server, Blocking, Introspection}

  @stream ~w(XADD XLEN XRANGE XREVRANGE XDEL XTRIM XSETID XREAD XINFO XGROUP
             XREADGROUP XACK XPENDING XCLAIM XAUTOCLAIM)

  @bitmap ~w(SETBIT GETBIT BITCOUNT BITPOS BITOP BITFIELD)

  @hll ~w(PFADD PFCOUNT PFMERGE)

  @keyspace ~w(SCAN HSCAN SSCAN ZSCAN OBJECT LPOS
               STRLEN SETRANGE GETRANGE SETNX GETDEL INCRBYFLOAT HSETNX HMGET
               SMISMEMBER SINTERCARD SPOP SRANDMEMBER SMOVE
               LREM LSET LTRIM LINSERT RPOPLPUSH
               ZCOUNT ZREVRANGE ZREVRANK ZMSCORE
               RENAME RENAMENX COPY TOUCH UNLINK EXPIREAT EXPIRETIME PEXPIRETIME)

  @server ~w(MEMORY CONFIG)

  @blocking Excache.Command.Blocking.commandNames()

  @introspection Excache.Command.Introspection.commandNames()

  def dispatch(name, args, state) do
    cond do
      name in @stream -> Streams.dispatch(name, args, state)
      name in @bitmap -> Bitmaps.dispatch(name, args, state)
      name in @hll -> HyperLogLog.dispatch(name, args, state)
      name in @keyspace -> Keyspace.dispatch(name, args, state)
      name in @server -> Server.dispatch(name, args, state)
      name in @blocking -> Blocking.dispatch(name, args, state)
      name in @introspection -> Introspection.dispatch(name, args, state)
      true -> :unhandled
    end
  end

  def command_names,
    do: @stream ++ @bitmap ++ @hll ++ @keyspace ++ @server ++ @blocking ++ @introspection

  def write_command_names do
    ~w(XADD XDEL XTRIM XSETID XGROUP XREADGROUP XACK XCLAIM XAUTOCLAIM
       SETBIT BITOP BITFIELD PFADD PFMERGE
       SETRANGE SETNX GETDEL INCRBYFLOAT HSETNX SPOP SMOVE
       LREM LSET LTRIM LINSERT RPOPLPUSH
       RENAME RENAMENX COPY UNLINK EXPIREAT)
  end
end
