defmodule Excache.Command.Keyspace.Scan do
  @moduledoc false

  alias Excache.Command.Reply
  alias Excache.Store
  alias Excache.Store.Ops.Extra

  @defaultCount 10
  @buckets 64

  def dispatch("SCAN", [cursorRaw | rest], state) do
    with {:ok, cursor} <- parseCursor(cursorRaw),
         {:ok, options} <- parseOptions(rest, [:match, :count, :type], defaults()) do
      {next, keys} = walkShards(cursor, options)
      reply(cursorReply(next, Reply.bulks(keys)), state)
    else
      {:reply, term} -> reply(term, state)
    end
  end

  def dispatch("HSCAN", [key, cursorRaw | rest], state),
    do: collection(:hash, key, cursorRaw, rest, state)

  def dispatch("SSCAN", [key, cursorRaw | rest], state),
    do: collection(:set, key, cursorRaw, rest, state)

  def dispatch("ZSCAN", [key, cursorRaw | rest], state),
    do: collection(:zset, key, cursorRaw, rest, state)

  def dispatch(_name, _args, _state), do: :unhandled

  defp collection(kind, key, cursorRaw, rest, state) do
    with {:ok, cursor} <- parseCursor(cursorRaw),
         {:ok, options} <- parseOptions(rest, allowedOptions(kind), defaults()),
         {:ok, elements} <- fetchElements(kind, key) do
      {next, selected} = walkBuckets(elements, cursor, options)
      reply(cursorReply(next, renderElements(kind, selected, options)), state)
    else
      {:reply, term} -> reply(term, state)
    end
  end

  defp allowedOptions(:hash), do: [:match, :count, :novalues]
  defp allowedOptions(_kind), do: [:match, :count]

  defp fetchElements(:hash, key) do
    case Store.hgetall(key) do
      {:ok, flat} ->
        {:ok, flat |> Enum.chunk_every(2) |> Enum.map(fn [field, value] -> {field, value} end)}

      {:error, :wrongtype} ->
        {:reply, Reply.wrongtype()}
    end
  end

  defp fetchElements(:set, key) do
    case Store.smembers(key) do
      {:ok, members} -> {:ok, Enum.map(members, &{&1, nil})}
      {:error, :wrongtype} -> {:reply, Reply.wrongtype()}
    end
  end

  defp fetchElements(:zset, key) do
    case Store.zrange(key, 0, -1) do
      {:ok, entries} -> {:ok, Enum.map(entries, fn {score, member} -> {member, score} end)}
      {:error, :wrongtype} -> {:reply, Reply.wrongtype()}
    end
  end

  defp renderElements(:hash, elements, %{novalues: true}),
    do: Enum.map(elements, fn {field, _value} -> {:bulk, field} end)

  defp renderElements(:hash, elements, _options),
    do: Enum.flat_map(elements, fn {field, value} -> [{:bulk, field}, {:bulk, value}] end)

  defp renderElements(:set, elements, _options),
    do: Enum.map(elements, fn {member, _value} -> {:bulk, member} end)

  defp renderElements(:zset, elements, _options) do
    Enum.flat_map(elements, fn {member, score} ->
      [{:bulk, member}, Reply.map_score({:ok, score})]
    end)
  end

  defp walkShards(cursor, options) do
    if cursor >= Store.shard_count() do
      {0, []}
    else
      consumeShards(cursor, options, [])
    end
  end

  defp consumeShards(cursor, options, acc) do
    {next, keys} = Store.scan(cursor, options.match, options.type)
    gathered = acc ++ keys

    cond do
      next == 0 -> {0, gathered}
      length(gathered) >= options.count -> {next, gathered}
      true -> consumeShards(next, options, gathered)
    end
  end

  defp walkBuckets(_elements, cursor, _options) when cursor >= @buckets, do: {0, []}

  defp walkBuckets(elements, cursor, options) do
    grouped = Enum.group_by(elements, fn {member, _value} -> bucketOf(member) end)
    consumeBuckets(grouped, cursor, options, [], 0)
  end

  defp consumeBuckets(grouped, bucket, options, acc, examined) do
    chunk = Map.get(grouped, bucket, [])
    selected = acc ++ matching(chunk, options.match)
    seen = examined + length(chunk)
    next = bucket + 1

    cond do
      next >= @buckets -> {0, selected}
      seen >= options.count -> {next, selected}
      true -> consumeBuckets(grouped, next, options, selected, seen)
    end
  end

  defp bucketOf(member), do: rem(:erlang.phash2(member), @buckets)

  defp matching(chunk, nil), do: chunk

  defp matching(chunk, regex),
    do: Enum.filter(chunk, fn {member, _value} -> Regex.match?(regex, member) end)

  defp parseCursor(raw) do
    case Integer.parse(raw) do
      {cursor, ""} when cursor >= 0 -> {:ok, cursor}
      _ -> {:reply, {:error, "ERR invalid cursor"}}
    end
  end

  defp defaults, do: %{match: nil, count: @defaultCount, type: nil, novalues: false}

  defp parseOptions([], _allowed, options), do: {:ok, options}

  defp parseOptions([token | rest], allowed, options) do
    case {String.upcase(token), rest} do
      {"MATCH", [pattern | tail]} ->
        withOption(:match, allowed, fn ->
          parseOptions(tail, allowed, %{options | match: Extra.compilePattern(pattern)})
        end)

      {"COUNT", [raw | tail]} ->
        withOption(:count, allowed, fn -> parseCount(raw, tail, allowed, options) end)

      {"TYPE", [raw | tail]} ->
        withOption(:type, allowed, fn ->
          parseOptions(tail, allowed, %{options | type: String.downcase(raw)})
        end)

      {"NOVALUES", tail} ->
        withOption(:novalues, allowed, fn ->
          parseOptions(tail, allowed, %{options | novalues: true})
        end)

      _ ->
        {:reply, Reply.syntax()}
    end
  end

  defp withOption(name, allowed, fun) do
    if name in allowed, do: fun.(), else: {:reply, Reply.syntax()}
  end

  defp parseCount(raw, tail, allowed, options) do
    case Integer.parse(raw) do
      {count, ""} when count > 0 -> parseOptions(tail, allowed, %{options | count: count})
      {_tooSmall, ""} -> {:reply, Reply.syntax()}
      _ -> {:reply, Reply.not_integer()}
    end
  end

  defp cursorReply(cursor, items),
    do: {:array, [{:bulk, Integer.to_string(cursor)}, {:array, items}]}

  defp reply(term, state), do: {[term], state}
end
