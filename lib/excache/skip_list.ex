defmodule Excache.SkipList do
  @moduledoc false

  @max_level 32
  @probability 0.25

  defmodule Node do
    @moduledoc false
    defstruct [:member, :score, :forward, :span]
  end

  defstruct nodes: %{},
            index: %{},
            header_forward: List.to_tuple(List.duplicate(nil, 32)),
            header_span: List.to_tuple(List.duplicate(0, 32)),
            level: 1,
            length: 0,
            next_id: 0

  def new, do: %__MODULE__{}

  def size(%__MODULE__{length: length}), do: length

  def member?(%__MODULE__{index: index}, member), do: Map.has_key?(index, member)

  def score(%__MODULE__{index: index} = list, member) do
    case Map.fetch(index, member) do
      {:ok, id} -> {:ok, node!(list, id).score}
      :error -> :error
    end
  end

  def rank(list, member) do
    case Map.fetch(list.index, member) do
      :error ->
        :error

      {:ok, id} ->
        node = node!(list, id)
        {_updates, ranks} = locate(list, node.score, node.member)
        {:ok, Map.fetch!(ranks, 0)}
    end
  end

  def insert(list, member, score) do
    list
    |> delete(member)
    |> do_insert(member, score)
  end

  def delete(list, member) do
    case Map.fetch(list.index, member) do
      :error ->
        list

      {:ok, id} ->
        node = node!(list, id)
        {updates, _ranks} = locate(list, node.score, node.member)

        list
        |> splice(updates, id, node)
        |> shrink_level()
        |> forget(id, member)
    end
  end

  def to_list(list) do
    collect_from(list, forward_at(list, :header, 0), [])
  end

  def range_by_rank(_list, lower, _count) when lower < 0, do: []
  def range_by_rank(_list, _lower, count) when count <= 0, do: []

  def range_by_rank(list, lower, count) do
    case at_rank(list, lower) do
      nil -> []
      id -> take_forward(list, id, count, [])
    end
  end

  def range_by_score(list, min_bound, max_bound) do
    case first_at_or_after(list, min_bound) do
      nil -> []
      id -> collect_while(list, id, max_bound, [])
    end
  end

  defp do_insert(list, member, score) do
    {updates, ranks} = locate(list, score, member)
    new_level = random_level()

    {updates, ranks, list} =
      if new_level > list.level do
        extended_updates =
          Enum.reduce(list.level..(new_level - 1), updates, &Map.put(&2, &1, :header))

        extended_ranks = Enum.reduce(list.level..(new_level - 1), ranks, &Map.put(&2, &1, 0))
        {extended_updates, extended_ranks, %{list | level: new_level}}
      else
        {updates, ranks, list}
      end

    id = list.next_id
    base_rank = Map.fetch!(ranks, 0)

    forward =
      for i <- 0..(new_level - 1), into: [], do: forward_at(list, Map.fetch!(updates, i), i)

    span =
      for i <- 0..(new_level - 1), into: [] do
        span_at(list, Map.fetch!(updates, i), i) - (base_rank - Map.fetch!(ranks, i))
      end

    node = %Node{
      member: member,
      score: score,
      forward: List.to_tuple(forward),
      span: List.to_tuple(span)
    }

    list
    |> put_node(id, node)
    |> rewire_updates(updates, ranks, base_rank, new_level, id)
    |> bump_untouched_spans(updates, new_level)
    |> remember(id, member)
  end

  defp locate(list, score, member) do
    top = list.level - 1

    Enum.reduce(top..0//-1, {%{}, %{}, :header}, fn i, {updates, ranks, x} ->
      rank_above = Map.get(ranks, i + 1, 0)
      {x, rank_here} = advance(list, x, i, score, member, rank_above)
      {Map.put(updates, i, x), Map.put(ranks, i, rank_here), x}
    end)
    |> then(fn {updates, ranks, _x} -> {updates, ranks} end)
  end

  defp advance(list, x, i, score, member, rank_acc) do
    case forward_at(list, x, i) do
      nil ->
        {x, rank_acc}

      next_id ->
        next = node!(list, next_id)

        if before_target?(next.score, next.member, score, member) do
          advance(list, next_id, i, score, member, rank_acc + span_at(list, x, i))
        else
          {x, rank_acc}
        end
    end
  end

  defp before_target?(score, member, target_score, target_member) do
    score < target_score or (score == target_score and member < target_member)
  end

  defp rewire_updates(list, updates, ranks, base_rank, new_level, new_id) do
    Enum.reduce(0..(new_level - 1), list, fn i, acc ->
      update_id = Map.fetch!(updates, i)
      new_span = base_rank - Map.fetch!(ranks, i) + 1

      acc
      |> set_forward(update_id, i, new_id)
      |> set_span(update_id, i, new_span)
    end)
  end

  defp bump_untouched_spans(list, updates, new_level) do
    if new_level > list.level - 1 do
      list
    else
      Enum.reduce(new_level..(list.level - 1), list, fn i, acc ->
        bump_span(acc, Map.fetch!(updates, i), i, 1)
      end)
    end
  end

  defp splice(list, updates, id, node) do
    Enum.reduce(0..(list.level - 1), list, fn i, acc ->
      update_id = Map.fetch!(updates, i)

      if forward_at(acc, update_id, i) == id do
        new_span = span_at(acc, update_id, i) + elem(node.span, i) - 1

        acc
        |> set_forward(update_id, i, elem(node.forward, i))
        |> set_span(update_id, i, new_span)
      else
        bump_span(acc, update_id, i, -1)
      end
    end)
  end

  defp shrink_level(list) do
    if list.level > 1 and forward_at(list, :header, list.level - 1) == nil do
      shrink_level(%{list | level: list.level - 1})
    else
      list
    end
  end

  defp at_rank(list, rank) when rank < 0 or rank >= list.length, do: nil

  defp at_rank(list, rank) do
    target = rank + 1
    top = list.level - 1

    {_traversed, x} =
      Enum.reduce(top..0//-1, {0, :header}, fn i, {traversed, x} ->
        advance_to_rank(list, x, i, traversed, target)
      end)

    case x do
      :header -> nil
      id -> id
    end
  end

  defp advance_to_rank(list, x, i, traversed, rank) do
    case forward_at(list, x, i) do
      nil ->
        {traversed, x}

      next_id ->
        span = span_at(list, x, i)

        if traversed + span <= rank do
          advance_to_rank(list, next_id, i, traversed + span, rank)
        else
          {traversed, x}
        end
    end
  end

  defp take_forward(_list, nil, _count, acc), do: Enum.reverse(acc)
  defp take_forward(_list, _id, 0, acc), do: Enum.reverse(acc)

  defp take_forward(list, id, count, acc) do
    node = node!(list, id)
    take_forward(list, elem(node.forward, 0), count - 1, [{node.score, node.member} | acc])
  end

  defp first_at_or_after(list, bound) do
    top = list.level - 1

    {_traversed, x} =
      Enum.reduce(top..0//-1, {0, :header}, fn i, {traversed, x} ->
        skip_below_bound(list, x, i, traversed, bound)
      end)

    forward_at(list, x, 0)
  end

  defp skip_below_bound(list, x, i, traversed, bound) do
    case forward_at(list, x, i) do
      nil ->
        {traversed, x}

      next_id ->
        next = node!(list, next_id)

        if below_bound?(next.score, bound) do
          skip_below_bound(list, next_id, i, traversed + span_at(list, x, i), bound)
        else
          {traversed, x}
        end
    end
  end

  defp collect_while(list, id, max_bound, acc) do
    node = node!(list, id)

    if within_upper?(node.score, max_bound) do
      updated = [{node.score, node.member} | acc]

      case elem(node.forward, 0) do
        nil -> Enum.reverse(updated)
        next_id -> collect_while(list, next_id, max_bound, updated)
      end
    else
      Enum.reverse(acc)
    end
  end

  defp collect_from(_list, nil, acc), do: Enum.reverse(acc)

  defp collect_from(list, id, acc) do
    node = node!(list, id)
    collect_from(list, elem(node.forward, 0), [{node.score, node.member} | acc])
  end

  defp below_bound?(_score, :neg_infinity), do: false
  defp below_bound?(_score, :pos_infinity), do: true
  defp below_bound?(score, {value, true}), do: score < value
  defp below_bound?(score, {value, false}), do: score <= value

  defp within_upper?(_score, :pos_infinity), do: true
  defp within_upper?(_score, :neg_infinity), do: false
  defp within_upper?(score, {value, true}), do: score <= value
  defp within_upper?(score, {value, false}), do: score < value

  defp forward_at(list, :header, i), do: elem(list.header_forward, i)
  defp forward_at(list, id, i), do: elem(node!(list, id).forward, i)

  defp span_at(list, :header, i), do: elem(list.header_span, i)
  defp span_at(list, id, i), do: elem(node!(list, id).span, i)

  defp set_forward(list, :header, i, value),
    do: %{list | header_forward: put_elem(list.header_forward, i, value)}

  defp set_forward(list, id, i, value) do
    node = node!(list, id)
    %{list | nodes: Map.put(list.nodes, id, %{node | forward: put_elem(node.forward, i, value)})}
  end

  defp set_span(list, :header, i, value),
    do: %{list | header_span: put_elem(list.header_span, i, value)}

  defp set_span(list, id, i, value) do
    node = node!(list, id)
    %{list | nodes: Map.put(list.nodes, id, %{node | span: put_elem(node.span, i, value)})}
  end

  defp bump_span(list, target, i, delta),
    do: set_span(list, target, i, span_at(list, target, i) + delta)

  defp put_node(list, id, node), do: %{list | nodes: Map.put(list.nodes, id, node)}

  defp remember(list, id, member),
    do: %{list | index: Map.put(list.index, member, id), length: list.length + 1, next_id: id + 1}

  defp forget(list, id, member) do
    %{
      list
      | nodes: Map.delete(list.nodes, id),
        index: Map.delete(list.index, member),
        length: list.length - 1
    }
  end

  defp node!(list, id), do: Map.fetch!(list.nodes, id)

  defp random_level, do: grow(1)

  defp grow(level) do
    if level < @max_level and :rand.uniform() < @probability do
      grow(level + 1)
    else
      level
    end
  end
end
