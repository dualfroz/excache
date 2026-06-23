defmodule Excache.SkipListTest do
  use ExUnit.Case, async: true

  alias Excache.SkipList

  test "empty skip list has zero size and no ranks" do
    list = SkipList.new()
    assert SkipList.size(list) == 0
    assert SkipList.to_list(list) == []
    assert SkipList.rank(list, "ghost") == :error
    assert SkipList.score(list, "ghost") == :error
  end

  test "insert keeps ascending score order" do
    list =
      SkipList.new()
      |> SkipList.insert("c", 3)
      |> SkipList.insert("a", 1)
      |> SkipList.insert("b", 2)

    assert SkipList.to_list(list) == [{1, "a"}, {2, "b"}, {3, "c"}]
    assert SkipList.size(list) == 3
  end

  test "equal scores break ties by member" do
    list =
      SkipList.new()
      |> SkipList.insert("zebra", 5)
      |> SkipList.insert("apple", 5)
      |> SkipList.insert("mango", 5)

    assert SkipList.to_list(list) == [{5, "apple"}, {5, "mango"}, {5, "zebra"}]
  end

  test "insert on an existing member reorders it instead of duplicating" do
    list =
      SkipList.new()
      |> SkipList.insert("a", 1)
      |> SkipList.insert("b", 2)
      |> SkipList.insert("a", 10)

    assert SkipList.size(list) == 2
    assert SkipList.to_list(list) == [{2, "b"}, {10, "a"}]
    assert SkipList.score(list, "a") == {:ok, 10}
  end

  test "delete removes a member and closes the gap" do
    list =
      SkipList.new()
      |> SkipList.insert("a", 1)
      |> SkipList.insert("b", 2)
      |> SkipList.insert("c", 3)
      |> SkipList.delete("b")

    assert SkipList.size(list) == 2
    assert SkipList.to_list(list) == [{1, "a"}, {3, "c"}]
    assert SkipList.member?(list, "b") == false
  end

  test "delete on a missing member is a no-op" do
    list = SkipList.new() |> SkipList.insert("a", 1)
    unchanged = SkipList.delete(list, "nope")
    assert SkipList.to_list(unchanged) == [{1, "a"}]
    assert SkipList.size(unchanged) == 1
  end

  test "rank matches zero-indexed position in sorted order" do
    list =
      SkipList.new()
      |> SkipList.insert("a", 10)
      |> SkipList.insert("b", 20)
      |> SkipList.insert("c", 30)
      |> SkipList.insert("d", 40)

    assert SkipList.rank(list, "a") == {:ok, 0}
    assert SkipList.rank(list, "b") == {:ok, 1}
    assert SkipList.rank(list, "c") == {:ok, 2}
    assert SkipList.rank(list, "d") == {:ok, 3}
    assert SkipList.rank(list, "missing") == :error
  end

  test "range_by_rank matches a plain list slice" do
    list =
      Enum.reduce(0..9, SkipList.new(), fn i, acc ->
        SkipList.insert(acc, "m#{i}", i)
      end)

    full = SkipList.to_list(list)
    assert SkipList.range_by_rank(list, 0, 3) == Enum.slice(full, 0, 3)
    assert SkipList.range_by_rank(list, 3, 4) == Enum.slice(full, 3, 4)
    assert SkipList.range_by_rank(list, 8, 10) == Enum.slice(full, 8, 10)
    assert SkipList.range_by_rank(list, 20, 5) == []
  end

  test "range_by_score respects inclusive and exclusive bounds" do
    list =
      Enum.reduce([{"a", 1}, {"b", 2}, {"c", 3}, {"d", 4}, {"e", 5}], SkipList.new(), fn {m, s},
                                                                                         acc ->
        SkipList.insert(acc, m, s)
      end)

    assert SkipList.range_by_score(list, {2, true}, {4, true}) == [{2, "b"}, {3, "c"}, {4, "d"}]
    assert SkipList.range_by_score(list, {2, false}, {4, false}) == [{3, "c"}]
    assert SkipList.range_by_score(list, :neg_infinity, :pos_infinity) == SkipList.to_list(list)
    assert SkipList.range_by_score(list, {10, true}, :pos_infinity) == []
  end

  test "random insert/delete sequence matches Enum.sort ground truth" do
    seed = :erlang.phash2(make_ref())
    :rand.seed(:exsplus, {seed, seed, seed})

    members = for i <- 1..200, do: "member-#{i}"

    {list, ground_truth} =
      Enum.reduce(members, {SkipList.new(), %{}}, fn member, {acc_list, acc_truth} ->
        score = :rand.uniform(50)
        {SkipList.insert(acc_list, member, score), Map.put(acc_truth, member, score)}
      end)

    expected =
      ground_truth
      |> Enum.map(fn {member, score} -> {score, member} end)
      |> Enum.sort()

    assert SkipList.to_list(list) == expected
    assert SkipList.size(list) == map_size(ground_truth)

    Enum.each(ground_truth, fn {member, score} ->
      assert SkipList.score(list, member) == {:ok, score}
    end)

    {deleted, remaining_truth} = Enum.split(members, 40)

    reduced =
      Enum.reduce(deleted, list, fn member, acc -> SkipList.delete(acc, member) end)

    remaining_expected =
      remaining_truth
      |> Enum.map(fn member -> {Map.fetch!(ground_truth, member), member} end)
      |> Enum.sort()

    assert SkipList.to_list(reduced) == remaining_expected
    assert SkipList.size(reduced) == length(remaining_truth)

    Enum.each(deleted, fn member ->
      assert SkipList.member?(reduced, member) == false
    end)
  end
end
