defmodule Excache.Store.Ops.Helpers do
  @moduledoc false

  alias Excache.SkipList

  def fetch(table, key, now) do
    case :ets.lookup(table, key) do
      [{^key, kind, value, ea}] ->
        if expired?(ea, now) do
          :ets.delete(table, key)
          :miss
        else
          {kind, value, ea}
        end

      [] ->
        :miss
    end
  end

  def collection(table, key, now, kind, empty, fun) do
    case fetch(table, key, now) do
      :miss -> commit(table, key, kind, empty, nil, fun)
      {^kind, value, ea} -> commit(table, key, kind, value, ea, fun)
      _ -> {:error, :wrongtype}
    end
  end

  def with_kind(table, key, now, kind, empty, fun) do
    case fetch(table, key, now) do
      :miss -> fun.(empty)
      {^kind, value, _ea} -> fun.(value)
      _ -> {:error, :wrongtype}
    end
  end

  def pop_list([], _side, nil), do: {[], nil}
  def pop_list([], _side, _count), do: {[], nil}
  def pop_list([head | tail], :left, nil), do: {tail, {:ok, head}}

  def pop_list(list, :right, nil) do
    {last, init} = List.pop_at(list, -1)
    {init, {:ok, last}}
  end

  def pop_list(list, :left, count) do
    {Enum.drop(list, count), {:ok, Enum.take(list, count)}}
  end

  def pop_list(list, :right, count) do
    size = length(list)
    keep = max(size - count, 0)
    taken = list |> Enum.drop(keep) |> Enum.reverse()
    {Enum.take(list, keep), {:ok, taken}}
  end

  def rank_window(len, start, stop) do
    lower = max(normalize_index(start, len), 0)
    upper = min(normalize_index(stop, len), len - 1)

    if len == 0 or lower > upper do
      {0, 0}
    else
      {lower, upper - lower + 1}
    end
  end

  def slice(list, start, stop) do
    len = length(list)
    lower = max(normalize_index(start, len), 0)
    upper = min(normalize_index(stop, len), len - 1)

    if len == 0 or lower > upper do
      []
    else
      Enum.slice(list, lower..upper)
    end
  end

  def at(list, index) do
    len = length(list)
    resolved = normalize_index(index, len)

    if resolved >= 0 and resolved < len do
      {:ok, Enum.at(list, resolved)}
    else
      nil
    end
  end

  def expired?(nil, _now), do: false
  def expired?(ea, now), do: ea <= now

  def expiry_from_opts(opts, now) do
    cond do
      Keyword.has_key?(opts, :ex) -> now + Keyword.fetch!(opts, :ex) * 1000
      Keyword.has_key?(opts, :px) -> now + Keyword.fetch!(opts, :px)
      true -> nil
    end
  end

  def remaining(ms_left, :milliseconds), do: max(ms_left, 0)
  def remaining(ms_left, :seconds), do: div(max(ms_left, 0) + 999, 1000)

  def compile_pattern(pattern) do
    source =
      pattern
      |> :binary.bin_to_list()
      |> Enum.map(&translate_glob/1)
      |> IO.iodata_to_binary()

    Regex.compile!("^" <> source <> "$", "s")
  end

  defp commit(table, key, kind, current, ea, fun) do
    {updated, reply} = fun.(current)

    if empty_value?(updated) do
      :ets.delete(table, key)
    else
      :ets.insert(table, {key, kind, updated, ea})
    end

    reply
  end

  defp empty_value?(value) when is_list(value), do: value == []
  defp empty_value?(%MapSet{} = set), do: MapSet.size(set) == 0
  defp empty_value?(%SkipList{} = zset), do: SkipList.size(zset) == 0
  defp empty_value?(map) when is_map(map), do: map_size(map) == 0

  defp normalize_index(index, len) when index < 0, do: len + index
  defp normalize_index(index, _len), do: index

  defp translate_glob(?*), do: ".*"
  defp translate_glob(??), do: "."

  defp translate_glob(char) when char in ~c".^$+{}()|[]\\/",
    do: <<"\\", char>>

  defp translate_glob(char), do: <<char>>
end
