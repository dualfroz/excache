defmodule Excache.Command.Reply do
  @moduledoc false

  @wrongtype {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
  @not_integer {:error, "ERR value is not an integer or out of range"}
  @not_float {:error, "ERR value is not a valid float"}
  @syntax {:error, "ERR syntax error"}

  def wrongtype, do: @wrongtype
  def not_integer, do: @not_integer
  def not_float, do: @not_float
  def syntax, do: @syntax

  def reply(term, state), do: {[term], state}

  def with_int(raw, state, fun) do
    case Integer.parse(raw) do
      {value, ""} -> reply(fun.(value), state)
      _ -> {[@not_integer], state}
    end
  end

  def with_two_ints(raw_a, raw_b, state, fun) do
    case {Integer.parse(raw_a), Integer.parse(raw_b)} do
      {{a, ""}, {b, ""}} -> reply(fun.(a, b), state)
      _ -> {[@not_integer], state}
    end
  end

  def map_get({:ok, value}), do: {:bulk, value}
  def map_get(nil), do: {:bulk, nil}
  def map_get({:error, reason}), do: error_term(reason)

  def map_int({:ok, value}), do: {:integer, value}
  def map_int(value) when is_integer(value), do: {:integer, value}
  def map_int(nil), do: {:bulk, nil}
  def map_int({:error, reason}), do: error_term(reason)

  def map_score({:ok, score}), do: {:bulk, format_score(score)}
  def map_score(nil), do: {:bulk, nil}
  def map_score({:error, reason}), do: error_term(reason)

  def map_list({:ok, values}), do: {:array, bulks(values)}
  def map_list({:error, reason}), do: error_term(reason)

  def zrange_reply({:ok, entries}, withscores),
    do: {:array, bulks(flatten_zset(entries, withscores))}

  def zrange_reply({:error, reason}, _withscores), do: error_term(reason)

  def map_pop({:ok, values}, count) when is_integer(count), do: {:array, bulks(values)}
  def map_pop(nil, count) when is_integer(count), do: {:array, nil}
  def map_pop({:ok, value}, nil), do: {:bulk, value}
  def map_pop(nil, nil), do: {:bulk, nil}
  def map_pop({:error, reason}, _count), do: error_term(reason)

  def bulks(values), do: Enum.map(values, fn value -> {:bulk, value} end)
  def bulks_nullable(values), do: Enum.map(values, fn value -> {:bulk, value} end)

  def pairs(list) do
    list
    |> Enum.chunk_every(2)
    |> Enum.map(fn [key, value] -> {key, value} end)
  end

  def parse_set_opts([], acc), do: {:ok, acc}

  def parse_set_opts([token | rest], acc) do
    case String.upcase(token) do
      "NX" -> parse_set_opts(rest, Keyword.put(acc, :nx, true))
      "XX" -> parse_set_opts(rest, Keyword.put(acc, :xx, true))
      "EX" -> parse_set_ttl(rest, acc, :ex)
      "PX" -> parse_set_ttl(rest, acc, :px)
      _ -> :error
    end
  end

  @zadd_flags ~w(NX XX GT LT CH)

  def parse_zadd_args(args) do
    {flags, rest} = Enum.split_while(args, &(String.upcase(&1) in @zadd_flags))
    opts = Enum.map(flags, &{zadd_flag_key(String.upcase(&1)), true})

    cond do
      rest == [] or rem(length(rest), 2) != 0 ->
        :error

      not valid_zadd_opts?(opts) ->
        :error

      true ->
        case parse_score_pairs(rest) do
          {:ok, entries} -> {:ok, opts, entries}
          :error -> :error
        end
    end
  end

  defp zadd_flag_key("NX"), do: :nx
  defp zadd_flag_key("XX"), do: :xx
  defp zadd_flag_key("GT"), do: :gt
  defp zadd_flag_key("LT"), do: :lt
  defp zadd_flag_key("CH"), do: :ch

  defp valid_zadd_opts?(opts) do
    nx = Keyword.get(opts, :nx, false)
    xx = Keyword.get(opts, :xx, false)
    gt = Keyword.get(opts, :gt, false)
    lt = Keyword.get(opts, :lt, false)

    not (nx and xx) and not (nx and (gt or lt)) and not (gt and lt)
  end

  def parse_score_pairs(rest) do
    rest
    |> Enum.chunk_every(2)
    |> Enum.reduce_while({:ok, []}, fn [score, member], {:ok, acc} ->
      case parse_score(score) do
        {:ok, value} -> {:cont, {:ok, [{member, value} | acc]}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      :error -> :error
    end
  end

  def parse_bound("-inf"), do: {:ok, :neg_infinity}
  def parse_bound("+inf"), do: {:ok, :pos_infinity}

  def parse_bound("(" <> rest) do
    case parse_score(rest) do
      {:ok, value} -> {:ok, {value, false}}
      :error -> :error
    end
  end

  def parse_bound(raw) do
    case parse_score(raw) do
      {:ok, value} -> {:ok, {value, true}}
      :error -> :error
    end
  end

  def parse_score(raw) do
    case Integer.parse(raw) do
      {value, ""} ->
        {:ok, value}

      _ ->
        case Float.parse(raw) do
          {value, ""} -> {:ok, value}
          _ -> :error
        end
    end
  end

  defp parse_set_ttl([raw | rest], acc, key) do
    case Integer.parse(raw) do
      {value, ""} when value > 0 -> parse_set_opts(rest, Keyword.put(acc, key, value))
      _ -> :error
    end
  end

  defp parse_set_ttl([], _acc, _key), do: :error

  defp flatten_zset(entries, true),
    do: Enum.flat_map(entries, fn {score, member} -> [member, format_score(score)] end)

  defp flatten_zset(entries, false), do: Enum.map(entries, fn {_score, member} -> member end)

  defp error_term(:wrongtype), do: @wrongtype
  defp error_term(:not_integer), do: @not_integer

  defp format_score(score) when is_integer(score), do: Integer.to_string(score)

  defp format_score(score) when is_float(score) do
    if score == Float.round(score) and abs(score) < 1.0e15 do
      Integer.to_string(trunc(score))
    else
      Float.to_string(score)
    end
  end
end
