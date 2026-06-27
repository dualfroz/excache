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

  def dispatch("RENAMENX", [source, dest], state),
    do: reply(integerReply(Store.renamenx(source, dest)), state)

  def dispatch("COPY", [source, dest | rest], state) do
    case parseCopyOptions(rest, false) do
      {:ok, _replace} when source == dest ->
        reply({:error, "ERR source and destination objects are the same"}, state)

      {:ok, replace} ->
        reply(integerReply(Store.copy(source, dest, replace)), state)

      {:reply, term} ->
        reply(term, state)
    end
  end

  def dispatch("TOUCH", [_ | _] = keys, state), do: reply({:integer, Store.touch(keys)}, state)
  def dispatch("UNLINK", [_ | _] = keys, state), do: reply({:integer, Store.del(keys)}, state)

  def dispatch("EXPIREAT", [key, secondsRaw | rest], state) do
    case Integer.parse(secondsRaw) do
      {seconds, ""} ->
        case parseExpireFlags(rest) do
          {:ok, flags} -> reply({:integer, expireAt(key, seconds * 1000, flags)}, state)
          {:reply, term} -> reply(term, state)
        end

      _ ->
        reply(Reply.not_integer(), state)
    end
  end

  def dispatch("EXPIRETIME", [key], state),
    do: reply({:integer, Store.expiretime(key, :seconds)}, state)

  def dispatch("PEXPIRETIME", [key], state),
    do: reply({:integer, Store.expiretime(key, :milliseconds)}, state)

  def dispatch(_name, _args, _state), do: :unhandled

  defp setrange(key, _offset, ""), do: integerReply(Store.strlen(key))
  defp setrange(key, offset, value), do: integerReply(Store.setrange(key, offset, value))

  defp lpos(key, element, %{count: nil} = options, state) do
    case Store.lpos(key, element, options.rank, 1, options.maxlen) do
      {:ok, []} -> reply({:bulk, nil}, state)
      {:ok, [index]} -> reply({:integer, index}, state)
      {:error, reason} -> reply(errorTerm(reason), state)
    end
  end

  defp lpos(key, element, options, state) do
    case Store.lpos(key, element, options.rank, options.count, options.maxlen) do
      {:ok, indexes} -> reply({:array, Enum.map(indexes, &{:integer, &1})}, state)
      {:error, reason} -> reply(errorTerm(reason), state)
    end
  end

  defp parseLposOptions([], options), do: {:ok, options}

  defp parseLposOptions([token | rest], options) do
    case {String.upcase(token), rest} do
      {"RANK", [raw | tail]} ->
        case Integer.parse(raw) do
          {0, ""} -> {:reply, {:error, rankZeroMessage()}}
          {rank, ""} -> parseLposOptions(tail, %{options | rank: rank})
          _ -> {:reply, Reply.not_integer()}
        end

      {"COUNT", [raw | tail]} ->
        case Integer.parse(raw) do
          {count, ""} when count >= 0 -> parseLposOptions(tail, %{options | count: count})
          {_negative, ""} -> {:reply, {:error, "ERR COUNT can't be negative"}}
          _ -> {:reply, Reply.not_integer()}
        end

      {"MAXLEN", [raw | tail]} ->
        case Integer.parse(raw) do
          {maxlen, ""} when maxlen >= 0 -> parseLposOptions(tail, %{options | maxlen: maxlen})
          {_negative, ""} -> {:reply, {:error, "ERR MAXLEN can't be negative"}}
          _ -> {:reply, Reply.not_integer()}
        end

      _ ->
        {:reply, Reply.syntax()}
    end
  end

  defp rankZeroMessage do
    "ERR RANK can't be zero. Use 1 to start searching from the first matching element in the " <>
      "head of the list or -1 searching backward from the tail of the list"
  end

  defp object("ENCODING", [key], state) do
    withEntry(key, state, fn entry -> {:bulk, encodingName(entry)} end)
  end

  defp object("REFCOUNT", [key], state) do
    withEntry(key, state, fn entry -> {:integer, refcount(entry)} end)
  end

  defp object("IDLETIME", [key], state) do
    withEntry(key, state, fn _entry -> {:integer, idleSeconds(key)} end)
  end

  defp object("HELP", [], state) do
    lines = [
      "OBJECT <subcommand> <key>. Subcommands are:",
      "ENCODING <key>",
      "    Return the kind of internal representation used to store the value of <key>.",
      "IDLETIME <key>",
      "    Return the seconds since <key> was last accessed, or 0 when the current",
      "    maxmemory-policy keeps no access metadata for it.",
      "REFCOUNT <key>",
      "    Return the number of references of the value associated with <key>.",
      "HELP",
      "    Print this help."
    ]

    reply({:array, Enum.map(lines, &{:simple, &1})}, state)
  end

  defp object(subcommand, _rest, state) do
    message =
      "ERR Unknown subcommand or wrong number of arguments for '#{subcommand}'. " <>
        "Try OBJECT HELP."

    reply({:error, message}, state)
  end

  defp idleSeconds(key) do
    case Tracker.idleMs(Tracker, key, System.system_time(:millisecond)) do
      nil -> 0
      idleMs -> div(idleMs, 1000)
    end
  end

  defp withEntry(key, state, fun) do
    case Store.encoding(key) do
      nil -> reply({:error, "ERR no such key"}, state)
      {:ok, entry} -> reply(fun.(entry), state)
    end
  end

  defp encodingName({:string, value, _ea}) do
    cond do
      integerString?(value) -> "int"
      byte_size(value) <= 44 -> "embstr"
      true -> "raw"
    end
  end

  defp encodingName({:list, value, _ea}) do
    if compact?(value, length(value)), do: "listpack", else: "quicklist"
  end

  defp encodingName({:hash, value, _ea}) do
    flat = Enum.flat_map(value, fn {field, item} -> [field, item] end)
    if compact?(flat, map_size(value)), do: "listpack", else: "hashtable"
  end

  defp encodingName({:set, value, _ea}) do
    members = MapSet.to_list(value)
    size = MapSet.size(value)

    cond do
      size <= @intsetEntries and Enum.all?(members, &integerString?/1) -> "intset"
      compact?(members, size) -> "listpack"
      true -> "hashtable"
    end
  end

  defp encodingName({:zset, value, _ea}) do
    members = value |> SkipList.to_list() |> Enum.map(fn {_score, member} -> member end)
    if compact?(members, SkipList.size(value)), do: "listpack", else: "skiplist"
  end

  defp encodingName({:stream, _value, _ea}), do: "stream"

  defp compact?(values, size) do
    size <= @listpackEntries and Enum.all?(values, &(byte_size(&1) <= @listpackValue))
  end

  defp refcount({:string, value, _ea}) do
    if integerString?(value) and String.to_integer(value) in 0..9999 do
      @sharedRefcount
    else
      1
    end
  end

  defp refcount(_entry), do: 1

  defp integerString?(value) do
    case Integer.parse(value) do
      {number, ""} -> Integer.to_string(number) == value and byte_size(value) <= 20
      _ -> false
    end
  end

  defp parseIntercard(numkeysRaw, rest) do
    case Integer.parse(numkeysRaw) do
      {numkeys, ""} when numkeys <= 0 ->
        {:reply, {:error, "ERR numkeys should be greater than 0"}}

      {numkeys, ""} when numkeys > length(rest) ->
        {:reply, {:error, "ERR Number of keys can't be greater than number of args"}}

      {numkeys, ""} ->
        {keys, tail} = Enum.split(rest, numkeys)

        case parseIntercardLimit(tail) do
          {:ok, limit} -> {:ok, keys, limit}
          {:reply, term} -> {:reply, term}
        end

      _ ->
        {:reply, Reply.not_integer()}
    end
  end

  defp parseIntercardLimit([]), do: {:ok, 0}

  defp parseIntercardLimit([token, raw]) do
    if String.upcase(token) == "LIMIT" do
      case Integer.parse(raw) do
        {limit, ""} when limit >= 0 -> {:ok, limit}
        {_negative, ""} -> {:reply, {:error, "ERR LIMIT can't be negative"}}
        _ -> {:reply, Reply.not_integer()}
      end
    else
      {:reply, Reply.syntax()}
    end
  end

  defp parseIntercardLimit(_tail), do: {:reply, Reply.syntax()}

  defp parseCopyOptions([], replace), do: {:ok, replace}

  defp parseCopyOptions([token | rest], replace) do
    case {String.upcase(token), rest} do
      {"REPLACE", tail} ->
        parseCopyOptions(tail, true)

      {"DB", [indexRaw | tail]} ->
        case Integer.parse(indexRaw) do
          {0, ""} -> parseCopyOptions(tail, replace)
          {_other, ""} -> {:reply, {:error, "ERR DB index is out of range"}}
          _ -> {:reply, Reply.not_integer()}
        end

      _ ->
        {:reply, Reply.syntax()}
    end
  end

  defp parseExpireFlags(tokens) do
    tokens
    |> Enum.reduce_while({:ok, %{nx: false, xx: false, gt: false, lt: false}}, fn token,
                                                                                  {:ok, flags} ->
      case String.upcase(token) do
        "NX" -> {:cont, {:ok, %{flags | nx: true}}}
        "XX" -> {:cont, {:ok, %{flags | xx: true}}}
        "GT" -> {:cont, {:ok, %{flags | gt: true}}}
        "LT" -> {:cont, {:ok, %{flags | lt: true}}}
        other -> {:halt, {:reply, {:error, "ERR Unsupported option #{other}"}}}
      end
    end)
    |> validateExpireFlags()
  end

  defp validateExpireFlags({:reply, term}), do: {:reply, term}

  defp validateExpireFlags({:ok, flags}) do
    incompatible = (flags.nx and (flags.xx or flags.gt or flags.lt)) or (flags.gt and flags.lt)

    if incompatible do
      {:reply, {:error, "ERR NX and XX, GT or LT options at the same time are not compatible"}}
    else
      {:ok, flags}
    end
  end

  defp expireAt(key, msAbs, flags) do
    case Store.expiretime(key, :milliseconds) do
      -2 -> 0
      current -> if expireAllowed?(flags, current, msAbs), do: Store.expireat(key, msAbs), else: 0
    end
  end

  defp expireAllowed?(flags, current, msAbs) do
    cond do
      flags.nx -> current == -1
      flags.xx and current == -1 -> false
      flags.gt -> current != -1 and msAbs > current
      flags.lt -> current == -1 or msAbs < current
      true -> true
    end
  end

  defp zrevrange(key, startRaw, stopRaw, withscores, state) do
    Reply.with_two_ints(startRaw, stopRaw, state, fn start, stop ->
      Reply.zrange_reply(Store.zrevrange(key, start, stop), withscores)
    end)
  end

  defp scoreEntry(nil), do: Reply.map_score(nil)
  defp scoreEntry(score), do: Reply.map_score({:ok, score})

  defp withType(key, expected, state, fun) do
    case Store.type(key) do
      "none" -> reply(fun.(), state)
      ^expected -> reply(fun.(), state)
      _ -> reply(Reply.wrongtype(), state)
    end
  end

  defp singleReply({:ok, [value | _rest]}), do: {:bulk, value}
  defp singleReply({:ok, []}), do: {:bulk, nil}
  defp singleReply({:error, reason}), do: errorTerm(reason)

  defp arrayReply({:ok, values}), do: {:array, Reply.bulks(values)}
  defp arrayReply({:error, reason}), do: errorTerm(reason)

  defp integerReply({:ok, value}), do: {:integer, value}
  defp integerReply({:error, reason}), do: errorTerm(reason)

  defp bulkReply({:ok, value}), do: {:bulk, value}
  defp bulkReply(nil), do: {:bulk, nil}
  defp bulkReply({:error, reason}), do: errorTerm(reason)

  defp rankReply({:ok, rank}), do: {:integer, rank}
  defp rankReply(nil), do: {:bulk, nil}
  defp rankReply({:error, reason}), do: errorTerm(reason)

  defp statusReply({:ok, "OK"}), do: {:simple, "OK"}
  defp statusReply({:error, reason}), do: errorTerm(reason)

  defp errorTerm(:wrongtype), do: Reply.wrongtype()
  defp errorTerm(:not_integer), do: Reply.not_integer()
  defp errorTerm(:not_float), do: Reply.not_float()
  defp errorTerm(:nokey), do: {:error, "ERR no such key"}
  defp errorTerm(:range), do: {:error, "ERR index out of range"}

  defp reply(term, state), do: {[term], state}
end
