defmodule Excache.Command.Keyspace do
  @moduledoc false

  alias Excache.{SkipList, Store}
  alias Excache.Command.Keyspace.Scan
  alias Excache.Command.Reply
  alias Excache.Eviction.Tracker

  @scanFamily ~w(SCAN HSCAN SSCAN ZSCAN)
  @sharedRefcount 2_147_483_647
  @listpackEntries 128
  @listpackValue 64
  @intsetEntries 512

  def dispatch(name, args, state) when name in @scanFamily,
    do: Scan.dispatch(name, args, state)

  def dispatch("OBJECT", [subcommand | rest], state),
    do: object(String.upcase(subcommand), rest, state)

  def dispatch("LPOS", [key, element | rest], state) do
    case parseLposOptions(rest, %{rank: 1, count: nil, maxlen: 0}) do
      {:ok, options} -> lpos(key, element, options, state)
      {:reply, term} -> reply(term, state)
    end
  end

  def dispatch("STRLEN", [key], state), do: reply(integerReply(Store.strlen(key)), state)

  def dispatch("SETRANGE", [key, offsetRaw, value], state) do
    case Integer.parse(offsetRaw) do
      {offset, ""} when offset >= 0 -> reply(setrange(key, offset, value), state)
      {_negative, ""} -> reply({:error, "ERR offset is out of range"}, state)
      _ -> reply(Reply.not_integer(), state)
    end
  end

  def dispatch("GETRANGE", [key, startRaw, stopRaw], state) do
    Reply.with_two_ints(startRaw, stopRaw, state, fn start, stop ->
      bulkReply(Store.getrange(key, start, stop))
    end)
  end

  def dispatch("SETNX", [key, value], state),
    do: reply({:integer, Store.setnx(key, value)}, state)

  def dispatch("GETDEL", [key], state), do: reply(bulkReply(Store.getdel(key)), state)

  def dispatch("INCRBYFLOAT", [key, deltaRaw], state) do
    case Reply.parse_score(deltaRaw) do
      {:ok, delta} -> reply(bulkReply(Store.incrbyfloat(key, delta)), state)
      :error -> reply(Reply.not_float(), state)
    end
  end

  def dispatch("HSETNX", [key, field, value], state),
    do: reply(integerReply(Store.hsetnx(key, field, value)), state)

  def dispatch("HMGET", [key | [_ | _] = fields], state) do
    withType(key, "hash", state, fn ->
      {:array, Reply.bulks_nullable(Store.hmget(key, fields))}
    end)
  end

  def dispatch("SMISMEMBER", [key | [_ | _] = members], state) do
    withType(key, "set", state, fn ->
      {:array, Enum.map(Store.smismember(key, members), &{:integer, &1})}
    end)
  end

  def dispatch("SINTERCARD", [numkeysRaw | [_ | _] = rest], state) do
    case parseIntercard(numkeysRaw, rest) do
      {:ok, keys, limit} -> reply(integerReply(Store.sintercard(keys, limit)), state)
      {:reply, term} -> reply(term, state)
    end
  end

  def dispatch("SPOP", [key], state), do: reply(singleReply(Store.spop(key, nil)), state)

  def dispatch("SPOP", [key, countRaw], state) do
    case Integer.parse(countRaw) do
      {count, ""} when count >= 0 -> reply(arrayReply(Store.spop(key, count)), state)
      {_negative, ""} -> reply({:error, "ERR value is out of range, must be positive"}, state)
      _ -> reply(Reply.not_integer(), state)
    end
  end

  def dispatch("SRANDMEMBER", [key], state),
    do: reply(singleReply(Store.srandmember(key, nil)), state)

  def dispatch("SRANDMEMBER", [key, countRaw], state) do
    Reply.with_int(countRaw, state, fn count -> arrayReply(Store.srandmember(key, count)) end)
  end

  def dispatch("SMOVE", [source, dest, member], state) do
    withType(dest, "set", state, fn -> integerReply(Store.smove(source, dest, member)) end)
  end

  def dispatch("LREM", [key, countRaw, value], state) do
    Reply.with_int(countRaw, state, fn count -> integerReply(Store.lrem(key, count, value)) end)
  end

  def dispatch("LSET", [key, indexRaw, value], state) do
    Reply.with_int(indexRaw, state, fn index -> statusReply(Store.lset(key, index, value)) end)
  end

  def dispatch("LTRIM", [key, startRaw, stopRaw], state) do
    Reply.with_two_ints(startRaw, stopRaw, state, fn start, stop ->
      statusReply(Store.ltrim(key, start, stop))
    end)
  end

  def dispatch("LINSERT", [key, positionRaw, pivot, value], state) do
    case String.upcase(positionRaw) do
      "BEFORE" -> reply(integerReply(Store.linsert(key, :before, pivot, value)), state)
      "AFTER" -> reply(integerReply(Store.linsert(key, :after, pivot, value)), state)
      _ -> reply(Reply.syntax(), state)
    end
  end

  def dispatch("RPOPLPUSH", [source, dest], state) do
    withType(dest, "list", state, fn -> bulkReply(Store.rpoplpush(source, dest)) end)
  end

  def dispatch("ZCOUNT", [key, minRaw, maxRaw], state) do
    case {Reply.parse_bound(minRaw), Reply.parse_bound(maxRaw)} do
      {{:ok, minBound}, {:ok, maxBound}} ->
        reply(integerReply(Store.zcount(key, minBound, maxBound)), state)

      _ ->
        reply({:error, "ERR min or max is not a float"}, state)
    end
  end

  def dispatch("ZREVRANGE", [key, startRaw, stopRaw], state),
    do: zrevrange(key, startRaw, stopRaw, false, state)

  def dispatch("ZREVRANGE", [key, startRaw, stopRaw, flag], state) do
    if String.upcase(flag) == "WITHSCORES" do
      zrevrange(key, startRaw, stopRaw, true, state)
    else
      reply(Reply.syntax(), state)
    end
  end

  def dispatch("ZREVRANK", [key, member], state),
    do: reply(rankReply(Store.zrevrank(key, member)), state)

  def dispatch("ZMSCORE", [key | [_ | _] = members], state) do
    case Store.zmscore(key, members) do
      {:ok, scores} -> reply({:array, Enum.map(scores, &scoreEntry/1)}, state)
      {:error, reason} -> reply(errorTerm(reason), state)
    end
  end

  def dispatch("RENAME", [source, dest], state),
    do: reply(statusReply(Store.rename(source, dest)), state)

