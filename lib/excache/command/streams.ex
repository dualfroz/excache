defmodule Excache.Command.Streams do
  @moduledoc false

  alias Excache.{Store, Stream}
  alias Excache.Command.Reply

  def dispatch("XADD", args, state), do: xadd(args, state)
  def dispatch("XLEN", [key], state), do: reply({:integer, orZero(Store.xlen(key))}, state)
  def dispatch("XRANGE", args, state), do: xrange(args, state, :forward)
  def dispatch("XREVRANGE", args, state), do: xrange(args, state, :reverse)
  def dispatch("XDEL", [key | [_ | _] = ids], state), do: xdel(key, ids, state)
  def dispatch("XTRIM", args, state), do: xtrim(args, state)
  def dispatch("XSETID", args, state), do: xsetid(args, state)
  def dispatch("XREAD", args, state), do: xread(args, state)
  def dispatch("XINFO", args, state), do: xinfo(args, state)
  def dispatch("XGROUP", args, state), do: xgroup(args, state)
  def dispatch("XREADGROUP", args, state), do: xreadgroup(args, state)
  def dispatch("XACK", [key, group | [_ | _] = ids], state), do: xack(key, group, ids, state)
  def dispatch("XPENDING", args, state), do: xpending(args, state)
  def dispatch("XCLAIM", args, state), do: xclaim(args, state)
  def dispatch("XAUTOCLAIM", args, state), do: xautoclaim(args, state)
  def dispatch(_name, _args, _state), do: :unhandled

  def rewriteAddId(args, id) do
    case parseAdd(args) do
      {:ok, key, _nomkstream, trim, _idToken, fields} ->
        [key] ++ trimTokens(trim) ++ [id] ++ fields

      :error ->
        args
    end
  end

  defp xadd(args, state) do
    case parseAdd(args) do
      {:ok, key, nomkstream, trim, idToken, fields} ->
        case Stream.parseAddId(idToken, now()) do
          {:ok, idSpec} ->
            case Store.xadd(key, idSpec, fields, trim, nomkstream) do
              {:ok, nil} -> reply({:bulk, nil}, state)
              {:ok, id} -> reply({:bulk, id}, state)
              {:error, reason} -> reply(streamError(reason), state)
            end

          :error ->
            reply({:error, "ERR Invalid stream ID specified as stream command argument"}, state)
        end

      :error ->
        reply(Reply.syntax(), state)
    end
  end

  defp xrange([key, startRaw, endRaw], state, direction),
    do: xrangeWith(key, startRaw, endRaw, :all, state, direction)

  defp xrange([key, startRaw, endRaw, count | rest], state, direction) do
    case {String.upcase(count), rest} do
      {"COUNT", [countRaw]} ->
        case Integer.parse(countRaw) do
          {value, ""} when value >= 0 ->
            xrangeWith(key, startRaw, endRaw, value, state, direction)

          _ ->
            reply(Reply.not_integer(), state)
        end

      _ ->
        reply(Reply.syntax(), state)
    end
  end

  defp xrange(_args, state, _direction), do: reply(Reply.syntax(), state)

  defp xrangeWith(key, startRaw, endRaw, count, state, direction) do
    with {:ok, startBound} <- Stream.parseRangeStart(startRaw),
         {:ok, endBound} <- Stream.parseRangeEnd(endRaw) do
      result =
        case direction do
          :forward -> Store.xrange(key, startBound, endBound, count)
          :reverse -> Store.xrevrange(key, endBound, startBound, count)
        end

      case result do
        {:ok, entries} -> reply({:array, Enum.map(entries, &entryReply/1)}, state)
        {:error, reason} -> reply(streamError(reason), state)
      end
    else
      :error ->
        reply({:error, "ERR Invalid stream ID specified as stream command argument"}, state)
    end
  end

  defp xdel(key, idsRaw, state) do
    case parseIds(idsRaw) do
      {:ok, ids} ->
        case Store.xdel(key, ids) do
          {:ok, removed} -> reply({:integer, removed}, state)
          {:error, reason} -> reply(streamError(reason), state)
        end

      :error ->
        reply({:error, "ERR Invalid stream ID specified as stream command argument"}, state)
    end
  end

  defp xtrim([key | rest], state) do
    case parseTrim(rest) do
      {:ok, {trim, _leftover}} when trim != nil ->
        case Store.xtrim(key, trim) do
          {:ok, removed} -> reply({:integer, removed}, state)
          {:error, reason} -> reply(streamError(reason), state)
        end

      _ ->
        reply(Reply.syntax(), state)
    end
  end

  defp xtrim(_args, state), do: reply(Reply.syntax(), state)

  defp xsetid([key, idRaw | _opts], state) do
    case parseThresholdId(idRaw) do
      {:ok, id} ->
        case Store.xsetid(key, id) do
          {:ok, "OK"} -> reply({:simple, "OK"}, state)
          {:error, :nokey} -> reply({:error, "ERR no such key"}, state)
          {:error, :setid_smaller} -> reply({:error, setidSmallerMessage()}, state)
          {:error, reason} -> reply(streamError(reason), state)
        end

      :error ->
        reply({:error, "ERR Invalid stream ID specified as stream command argument"}, state)
    end
  end

  defp xsetid(_args, state), do: reply(Reply.syntax(), state)

  defp xread(args, state) do
    case parseRead(args) do
      {:ok, count, pairs} ->
        collected =
          Enum.reduce(pairs, [], fn {key, idRaw}, acc ->
            afterId = resolveReadId(key, idRaw)

            case Store.xread(key, afterId, count) do
              {:ok, nil} -> acc
              {:ok, entries} -> [streamEntriesReply(key, entries) | acc]
              {:error, _} -> acc
            end
          end)
          |> Enum.reverse()

        case collected do
          [] -> reply({:array, nil}, state)
          results -> reply({:array, results}, state)
        end

      :error ->
        reply(Reply.syntax(), state)
    end
  end

  defp xreadgroup(args, state) do
    case parseReadGroup(args) do
      {:ok, group, consumer, count, noack, pairs} ->
        collected =
          Enum.reduce_while(pairs, {:ok, []}, fn {key, idRaw}, {:ok, acc} ->
            mode = if idRaw == ">", do: {:new}, else: {:history, parseThresholdBang(idRaw)}

            case Store.xreadgroup(key, group, consumer, mode, count, noack) do
              {:ok, []} -> {:cont, {:ok, acc}}
              {:ok, entries} -> {:cont, {:ok, [streamEntriesReply(key, entries) | acc]}}
              {:error, :nogroup} -> {:halt, {:error, nogroup(key, group)}}
              {:error, reason} -> {:halt, {:error, streamError(reason)}}
            end
          end)

        case collected do
          {:ok, []} -> reply({:array, nil}, state)
          {:ok, results} -> reply({:array, Enum.reverse(results)}, state)
          {:error, term} -> reply(term, state)
        end

      :error ->
        reply(Reply.syntax(), state)
    end
  end

  defp xinfo(["STREAM", key | _], state) do
    case Store.xinfoStream(key) do
      {:ok, info} -> reply({:array, infoReply(info)}, state)
      {:error, :nokey} -> reply({:error, "ERR no such key"}, state)
      {:error, reason} -> reply(streamError(reason), state)
    end
  end

  defp xinfo(_args, state), do: reply(Reply.syntax(), state)

  defp xgroup(["CREATE", key, group, startRaw | opts], state) do
    mkstream = Enum.any?(opts, &(String.upcase(&1) == "MKSTREAM"))

    case parseGroupStart(startRaw) do
      {:ok, startSpec} ->
        case Store.xgroupCreate(key, group, startSpec, mkstream) do
          {:ok, "OK"} -> reply({:simple, "OK"}, state)
          {:error, :busygroup} -> reply({:error, "BUSYGROUP Consumer Group name already exists"}, state)
          {:error, :nokey} -> reply({:error, noKeyGroupMessage()}, state)
          {:error, reason} -> reply(streamError(reason), state)
        end

      :error ->
        reply({:error, "ERR Invalid stream ID specified as stream command argument"}, state)
    end
  end

  defp xgroup(["DESTROY", key, group], state) do
    case Store.xgroupDestroy(key, group) do
      {:ok, removed} -> reply({:integer, removed}, state)
      {:error, reason} -> reply(streamError(reason), state)
    end
  end

  defp xgroup(["CREATECONSUMER", key, group, consumer], state) do
    case Store.xgroupCreateConsumer(key, group, consumer) do
      {:ok, added} -> reply({:integer, added}, state)
      {:error, :nogroup} -> reply(nogroup(key, group), state)
      {:error, reason} -> reply(streamError(reason), state)
    end
  end

  defp xgroup(["DELCONSUMER", key, group, consumer], state) do
    case Store.xgroupDelConsumer(key, group, consumer) do
      {:ok, removed} -> reply({:integer, removed}, state)
      {:error, :nogroup} -> reply(nogroup(key, group), state)
      {:error, reason} -> reply(streamError(reason), state)
    end
  end

  defp xgroup(["SETID", key, group, idRaw | _opts], state) do
    case parseGroupStart(idRaw) do
      {:ok, startSpec} ->
        id =
          case startSpec do
            :last -> lastIdOf(key)
            {:id, value} -> value
          end

        case Store.xgroupSetId(key, group, id) do
          {:ok, "OK"} -> reply({:simple, "OK"}, state)
          {:error, :nogroup} -> reply(nogroup(key, group), state)
          {:error, reason} -> reply(streamError(reason), state)
        end

      :error ->
        reply({:error, "ERR Invalid stream ID specified as stream command argument"}, state)
    end
  end

  defp xgroup(_args, state), do: reply(Reply.syntax(), state)

  defp xack(key, group, idsRaw, state) do
    case parseIds(idsRaw) do
      {:ok, ids} ->
        case Store.xack(key, group, ids) do
          {:ok, count} -> reply({:integer, count}, state)
          {:error, reason} -> reply(streamError(reason), state)
        end

      :error ->
        reply({:error, "ERR Invalid stream ID specified as stream command argument"}, state)
    end
  end

  defp xpending([key, group], state) do
    case Store.xpendingSummary(key, group) do
      {:ok, summary} -> reply(pendingSummaryReply(summary), state)
      {:error, :nogroup} -> reply(nogroup(key, group), state)
      {:error, reason} -> reply(streamError(reason), state)
    end
  end

  defp xpending([key, group | rest], state) do
    case parsePendingRange(rest) do
      {:ok, startRaw, endRaw, count, consumer, minIdle} ->
        with {:ok, startId} <- pendingBound(startRaw, {0, 0}),
             {:ok, endId} <- pendingBound(endRaw, {maxMs(), maxSeq()}) do
          case Store.xpendingRange(key, group, startId, endId, count, consumer, minIdle) do
            {:ok, rows} -> reply({:array, Enum.map(rows, &pendingRowReply/1)}, state)
            {:error, :nogroup} -> reply(nogroup(key, group), state)
            {:error, reason} -> reply(streamError(reason), state)
          end
        else
          :error ->
            reply({:error, "ERR Invalid stream ID specified as stream command argument"}, state)
        end

      :error ->
        reply(Reply.syntax(), state)
    end
  end

  defp xpending(_args, state), do: reply(Reply.syntax(), state)

  defp xclaim([key, group, consumer, minIdleRaw | rest], state) do
    with {minIdle, ""} <- Integer.parse(minIdleRaw),
         {:ok, ids, opts} <- parseClaim(rest) do
      case Store.xclaim(key, group, consumer, minIdle, ids, opts) do
        {:ok, results} -> reply(claimReply(results, opts.justId), state)
        {:error, :nogroup} -> reply(nogroup(key, group), state)
        {:error, reason} -> reply(streamError(reason), state)
      end
    else
      _ -> reply(Reply.syntax(), state)
    end
  end

  defp xclaim(_args, state), do: reply(Reply.syntax(), state)

  defp xautoclaim([key, group, consumer, minIdleRaw, startRaw | rest], state) do
    with {minIdle, ""} <- Integer.parse(minIdleRaw),
         {:ok, startId} <- Stream.parseRangeStart(startRaw),
         {:ok, count, justId} <- parseAutoclaim(rest) do
      {_kind, startTuple} = startId

      case Store.xautoclaim(key, group, consumer, minIdle, startTuple, count) do
        {:ok, {cursor, claimed, deleted}} ->
          reply(autoclaimReply(cursor, claimed, deleted, justId), state)

        {:error, :nogroup} ->
          reply(nogroup(key, group), state)

        {:error, reason} ->
          reply(streamError(reason), state)
      end
    else
      _ -> reply(Reply.syntax(), state)
    end
  end

  defp xautoclaim(_args, state), do: reply(Reply.syntax(), state)

  defp parseAdd([key | rest]), do: parseAddOptions(rest, key, false, nil)
  defp parseAdd(_args), do: :error

  defp parseAddOptions([token | rest], key, nomkstream, trim) do
    case String.upcase(token) do
      "NOMKSTREAM" ->
        parseAddOptions(rest, key, true, trim)

      "MAXLEN" ->
        case parseTrimBody(rest, :maxlen) do
          {:ok, parsed, remaining} -> parseAddOptions(remaining, key, nomkstream, parsed)
          :error -> :error
        end

      "MINID" ->
        case parseTrimBody(rest, :minid) do
          {:ok, parsed, remaining} -> parseAddOptions(remaining, key, nomkstream, parsed)
          :error -> :error
        end

      _ ->
        case rest do
          [_ | _] = fields ->
            if rem(length(fields), 2) == 0, do: {:ok, key, nomkstream, trim, token, fields}, else: :error

          [] ->
            :error
        end
    end
  end

  defp parseAddOptions([], _key, _nomkstream, _trim), do: :error

  defp parseTrim([]), do: {:ok, {nil, []}}

  defp parseTrim([token | rest]) do
    case String.upcase(token) do
      "MAXLEN" ->
        case parseTrimBody(rest, :maxlen) do
          {:ok, parsed, remaining} -> {:ok, {parsed, remaining}}
          :error -> :error
        end

      "MINID" ->
        case parseTrimBody(rest, :minid) do
          {:ok, parsed, remaining} -> {:ok, {parsed, remaining}}
          :error -> :error
        end

      _ ->
        :error
    end
  end

  defp parseTrimBody(tokens, kind) do
    tokens = dropApproxMarker(tokens)

    case tokens do
      [threshold | rest] ->
        rest = dropLimit(rest)

        case buildTrim(kind, threshold) do
          {:ok, parsed} -> {:ok, parsed, rest}
          :error -> :error
        end

      [] ->
        :error
    end
  end

  defp dropApproxMarker([marker | rest]) when marker in ["~", "="], do: rest
  defp dropApproxMarker(tokens), do: tokens

  defp dropLimit(["LIMIT", _value | rest]), do: rest
  defp dropLimit(["limit", _value | rest]), do: rest
  defp dropLimit(tokens), do: tokens

  defp buildTrim(:maxlen, threshold) do
    case Integer.parse(threshold) do
      {value, ""} when value >= 0 -> {:ok, {:maxlen, value}}
      _ -> :error
    end
  end

  defp buildTrim(:minid, threshold) do
    case parseThresholdId(threshold) do
      {:ok, id} -> {:ok, {:minid, id}}
      :error -> :error
    end
  end

  defp trimTokens(nil), do: []
  defp trimTokens({:maxlen, count}), do: ["MAXLEN", Integer.to_string(count)]
  defp trimTokens({:minid, {ms, seq}}), do: ["MINID", "#{ms}-#{seq}"]

  defp parseRead(args) do
    {count, rest} = takeReadCount(args, :all)

    case splitStreams(rest) do
      {:ok, keys, ids} when length(keys) == length(ids) and keys != [] ->
        {:ok, count, Enum.zip(keys, ids)}

      _ ->
        :error
    end
  end

  defp parseReadGroup(["GROUP", group, consumer | rest]) do
    {count, rest} = takeReadCount(rest, :all)
    {noack, rest} = takeNoack(rest)

    case splitStreams(rest) do
      {:ok, keys, ids} when length(keys) == length(ids) and keys != [] ->
        {:ok, group, consumer, count, noack, Enum.zip(keys, ids)}

      _ ->
        :error
    end
  end

  defp parseReadGroup(_args), do: :error

  defp takeReadCount(["COUNT", raw | rest], _default) do
    case Integer.parse(raw) do
      {value, ""} when value >= 0 -> {value, rest}
      _ -> {:all, rest}
    end
  end

  defp takeReadCount([token, raw | rest], default) when token in ["count"] do
    takeReadCount(["COUNT", raw | rest], default)
  end

  defp takeReadCount(rest, default), do: {default, rest}

  defp takeNoack(["NOACK" | rest]), do: {true, rest}
  defp takeNoack(["noack" | rest]), do: {true, rest}
  defp takeNoack(rest), do: {false, rest}

  defp splitStreams(tokens) do
    case Enum.split_while(tokens, &(String.upcase(&1) != "STREAMS")) do
      {leading, ["STREAMS" | rest]} ->
        # any leading BLOCK ms is tolerated and ignored here
        _ = leading
        half = div(length(rest), 2)

        if rem(length(rest), 2) == 0 and half > 0 do
          {:ok, Enum.take(rest, half), Enum.drop(rest, half)}
        else
          :error
        end

      _ ->
        :error
    end
  end

  defp resolveReadId(key, "$"), do: lastIdOf(key)
  defp resolveReadId(_key, "+"), do: {maxMs(), maxSeq()}

  defp resolveReadId(_key, raw) do
    case parseThresholdId(raw) do
      {:ok, id} -> id
      :error -> {0, 0}
    end
  end

  defp lastIdOf(key) do
    case Store.xinfoStream(key) do
      {:ok, info} -> info.lastGeneratedId
      _ -> {0, 0}
    end
  end

  defp parseGroupStart("$"), do: {:ok, :last}
  defp parseGroupStart("0"), do: {:ok, {:id, {0, 0}}}

  defp parseGroupStart(raw) do
    case parseThresholdId(raw) do
      {:ok, id} -> {:ok, {:id, id}}
      :error -> :error
    end
  end

  defp parsePendingRange(["IDLE", idleRaw | rest]) do
    case Integer.parse(idleRaw) do
      {idle, ""} -> withPendingCore(rest, idle)
      _ -> :error
    end
  end

  defp parsePendingRange(rest), do: withPendingCore(rest, nil)

  defp withPendingCore([startRaw, endRaw, countRaw | rest], minIdle) do
    case Integer.parse(countRaw) do
      {count, ""} when count >= 0 ->
        consumer = List.first(rest)
        {:ok, startRaw, endRaw, count, consumer, minIdle}

      _ ->
        :error
    end
  end

  defp withPendingCore(_rest, _minIdle), do: :error

  defp pendingBound("-", _default), do: {:ok, {0, 0}}
  defp pendingBound("+", _default), do: {:ok, {maxMs(), maxSeq()}}

  defp pendingBound(raw, _default) do
    case parseThresholdId(raw) do
      {:ok, id} -> {:ok, id}
      :error -> :error
    end
  end

  defp parseClaim(tokens), do: parseClaimIds(tokens, [])

  defp parseClaimIds([token | rest], acc) do
    case parseThresholdId(token) do
      {:ok, id} ->
        parseClaimIds(rest, [id | acc])

      :error ->
        case acc do
          [] -> :error
          _ -> parseClaimOpts([token | rest], Enum.reverse(acc), defaultClaimOpts())
        end
    end
  end

  defp parseClaimIds([], acc) when acc != [], do: {:ok, Enum.reverse(acc), defaultClaimOpts()}
  defp parseClaimIds([], _acc), do: :error

  defp parseClaimOpts([], ids, opts), do: {:ok, ids, opts}

  defp parseClaimOpts([token | rest], ids, opts) do
    case String.upcase(token) do
      "IDLE" -> claimIntOpt(rest, ids, opts, :idle)
      "TIME" -> claimIntOpt(rest, ids, opts, :time)
      "RETRYCOUNT" -> claimIntOpt(rest, ids, opts, :retryCount)
      "FORCE" -> parseClaimOpts(rest, ids, %{opts | force: true})
      "JUSTID" -> parseClaimOpts(rest, ids, %{opts | justId: true})
      "LASTID" -> parseClaimOpts(tl(rest), ids, opts)
      _ -> :error
    end
  end

  defp claimIntOpt([raw | rest], ids, opts, field) do
    case Integer.parse(raw) do
      {value, ""} -> parseClaimOpts(rest, ids, Map.put(opts, field, value))
      _ -> :error
    end
  end

  defp claimIntOpt([], _ids, _opts, _field), do: :error

  defp defaultClaimOpts,
    do: %{idle: nil, time: nil, retryCount: nil, force: false, justId: false}

  defp parseAutoclaim(["COUNT", raw | rest]) do
    case Integer.parse(raw) do
      {value, ""} when value > 0 -> finishAutoclaim(rest, value)
      _ -> :error
    end
  end

  defp parseAutoclaim(rest), do: finishAutoclaim(rest, 100)

  defp finishAutoclaim([], count), do: {:ok, count, false}
  defp finishAutoclaim(["JUSTID"], count), do: {:ok, count, true}
  defp finishAutoclaim(["justid"], count), do: {:ok, count, true}
  defp finishAutoclaim(_rest, _count), do: :error

  defp parseIds(idsRaw) do
    Enum.reduce_while(idsRaw, {:ok, []}, fn raw, {:ok, acc} ->
      case parseThresholdId(raw) do
        {:ok, id} -> {:cont, {:ok, [id | acc]}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, ids} -> {:ok, Enum.reverse(ids)}
      :error -> :error
    end
  end

  defp parseThresholdId(raw) do
    case String.split(raw, "-", parts: 2) do
      [ms] -> withMs(ms, 0)
      [ms, seq] -> withMsSeq(ms, seq)
    end
  end

  defp parseThresholdBang(raw) do
    case parseThresholdId(raw) do
      {:ok, id} -> id
      :error -> {0, 0}
    end
  end

  defp withMs(ms, seq) do
    case Integer.parse(ms) do
      {value, ""} when value >= 0 -> {:ok, {value, seq}}
      _ -> :error
    end
  end

  defp withMsSeq(ms, seq) do
    with {msValue, ""} <- Integer.parse(ms),
         {seqValue, ""} <- Integer.parse(seq),
         true <- msValue >= 0 and seqValue >= 0 do
      {:ok, {msValue, seqValue}}
    else
      _ -> :error
    end
  end

  defp entryReply({id, fields}) do
    {:array, [{:bulk, Stream.formatId(id)}, fieldsReply(fields)]}
  end

  defp fieldsReply(nil), do: {:array, nil}
  defp fieldsReply(fields), do: {:array, Enum.map(fields, &{:bulk, &1})}

  defp streamEntriesReply(key, entries) do
    {:array, [{:bulk, key}, {:array, Enum.map(entries, &entryReply/1)}]}
  end

  defp infoReply(info) do
    [
      {:bulk, "length"},
      {:integer, info.length},
      {:bulk, "last-generated-id"},
      {:bulk, Stream.formatId(info.lastGeneratedId)},
      {:bulk, "max-deleted-entry-id"},
      {:bulk, Stream.formatId(info.maxDeletedId)},
      {:bulk, "entries-added"},
      {:integer, info.entriesAdded},
      {:bulk, "groups"},
      {:integer, info.groups},
      {:bulk, "first-entry"},
      firstOrNil(info.firstEntry),
      {:bulk, "last-entry"},
      firstOrNil(info.lastEntry)
    ]
  end

  defp firstOrNil(nil), do: {:array, nil}
  defp firstOrNil(entry), do: entryReply(entry)

  defp pendingSummaryReply({0, _minId, _maxId, _perConsumer}) do
    {:array, [{:integer, 0}, {:bulk, nil}, {:bulk, nil}, {:array, nil}]}
  end

  defp pendingSummaryReply({total, minId, maxId, perConsumer}) do
    consumers =
      Enum.map(perConsumer, fn {consumer, count} ->
        {:array, [{:bulk, consumer}, {:bulk, Integer.to_string(count)}]}
      end)

    {:array,
     [
       {:integer, total},
       {:bulk, Stream.formatId(minId)},
       {:bulk, Stream.formatId(maxId)},
       {:array, consumers}
     ]}
  end

  defp pendingRowReply({id, consumer, idle, deliveryCount}) do
    {:array,
     [
       {:bulk, Stream.formatId(id)},
       {:bulk, consumer},
       {:integer, idle},
       {:integer, deliveryCount}
     ]}
  end

  defp claimReply(results, true) do
    {:array, Enum.map(results, fn {id, _fields} -> {:bulk, Stream.formatId(id)} end)}
  end

  defp claimReply(results, false) do
    {:array, Enum.map(results, &entryReply/1)}
  end

  defp autoclaimReply(cursor, claimed, deleted, justId) do
    entriesReply =
      if justId do
        {:array, Enum.map(claimed, fn {id, _fields} -> {:bulk, Stream.formatId(id)} end)}
      else
        {:array, Enum.map(claimed, &entryReply/1)}
      end

    deletedReply = {:array, Enum.map(deleted, fn id -> {:bulk, Stream.formatId(id)} end)}
    {:array, [{:bulk, Stream.formatId(cursor)}, entriesReply, deletedReply]}
  end

  defp reply(term, state), do: {[term], state}

  defp orZero({:ok, value}), do: value
  defp orZero({:error, _}), do: 0

  defp streamError(:wrongtype), do: Reply.wrongtype()
  defp streamError(:smaller),
    do: {:error, "ERR The ID specified in XADD is equal or smaller than the target stream top item"}

  defp streamError(:zero), do: {:error, "ERR The ID specified in XADD must be greater than 0-0"}
  defp streamError(other), do: {:error, "ERR #{other}"}

  defp nogroup(key, group) do
    {:error,
     "NOGROUP No such key '#{key}' or consumer group '#{group}' in XREADGROUP with GROUP option"}
  end

  defp noKeyGroupMessage do
    "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically."
  end

  defp setidSmallerMessage do
    "ERR The ID specified in XSETID is smaller than the target stream top item"
  end

  defp now, do: System.system_time(:millisecond)
  defp maxMs, do: 0xFFFFFFFFFFFFFFFF
  defp maxSeq, do: 0xFFFFFFFFFFFFFFFF
end
