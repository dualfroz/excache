defmodule Excache.Stream.Groups do
  @moduledoc false

  alias Excache.Stream

  def readNew(stream, groupName, consumer, count, noack, now) do
    {:ok, group} = Stream.fetchGroup(stream, groupName)
    entries = Stream.afterId(stream, group.lastDeliveredId, count)

    case entries do
      [] ->
        {stream, []}

      _ ->
        group = ensureConsumer(group, consumer, now)
        lastId = entries |> List.last() |> elem(0)

        group = %{
          group
          | lastDeliveredId: lastId,
            entriesRead: group.entriesRead + length(entries)
        }

        group =
          if noack do
            touchConsumer(group, consumer, now)
          else
            Enum.reduce(entries, group, fn {id, _fields}, acc ->
              addPending(acc, id, consumer, now)
            end)
          end

        {Stream.putGroup(stream, groupName, group), entries}
    end
  end

  def readHistory(stream, groupName, consumer, startId, count, now) do
    {:ok, group} = Stream.fetchGroup(stream, groupName)

    case Map.fetch(group.consumers, consumer) do
      :error ->
        {stream, []}

      {:ok, state} ->
        ids =
          state.pending
          |> MapSet.to_list()
          |> Enum.filter(&(&1 >= startId))
          |> Enum.sort()
          |> takeCount(count)

        entries =
          Enum.map(ids, fn id ->
            case Stream.get(stream, id) do
              {:ok, fields} -> {id, fields}
              :miss -> {id, nil}
            end
          end)

        group = touchConsumer(group, consumer, now)
        {Stream.putGroup(stream, groupName, group), entries}
    end
  end

  def ack(stream, groupName, ids) do
    {:ok, group} = Stream.fetchGroup(stream, groupName)

    {group, count} =
      Enum.reduce(ids, {group, 0}, fn id, {acc, acked} ->
        case Map.fetch(acc.pending, id) do
          {:ok, entry} -> {removePending(acc, id, entry.consumer), acked + 1}
          :error -> {acc, acked}
        end
      end)

    {Stream.putGroup(stream, groupName, group), count}
  end

  def pendingSummary(stream, groupName) do
    {:ok, group} = Stream.fetchGroup(stream, groupName)
    ids = Map.keys(group.pending)

    case ids do
      [] ->
        {0, nil, nil, []}

      _ ->
        perConsumer =
          Enum.reduce(group.pending, %{}, fn {_id, entry}, acc ->
            Map.update(acc, entry.consumer, 1, &(&1 + 1))
          end)
          |> Enum.sort()

        {length(ids), Enum.min(ids), Enum.max(ids), perConsumer}
    end
  end

  def pendingRange(stream, groupName, startId, endId, count, consumerFilter, minIdle, now) do
    {:ok, group} = Stream.fetchGroup(stream, groupName)

    group.pending
    |> Enum.filter(fn {id, entry} ->
      id >= startId and id <= endId and
        (consumerFilter == nil or entry.consumer == consumerFilter) and
        (minIdle == nil or now - entry.deliveryTime >= minIdle)
    end)
    |> Enum.sort_by(fn {id, _entry} -> id end)
    |> Enum.take(count)
    |> Enum.map(fn {id, entry} ->
      {id, entry.consumer, now - entry.deliveryTime, entry.deliveryCount}
    end)
  end

  def claim(stream, groupName, consumer, minIdle, ids, opts, now) do
    {:ok, group} = Stream.fetchGroup(stream, groupName)
    group = ensureConsumer(group, consumer, now)

    {group, results} =
      Enum.reduce(ids, {group, []}, fn id, {acc, collected} ->
        case claimOne(stream, acc, consumer, minIdle, id, opts, now) do
          {:claimed, updated, entry} -> {updated, [entry | collected]}
          {:dropped, updated} -> {updated, collected}
          {:skip, updated} -> {updated, collected}
        end
      end)

    {Stream.putGroup(stream, groupName, group), Enum.reverse(results)}
  end

  def autoclaim(stream, groupName, consumer, minIdle, startId, count, now) do
    {:ok, group} = Stream.fetchGroup(stream, groupName)
    group = ensureConsumer(group, consumer, now)

    candidates =
      group.pending
      |> Map.keys()
      |> Enum.filter(&(&1 >= startId))
      |> Enum.sort()

    walk = autoclaimWalk(stream, group, consumer, minIdle, candidates, count, now, [], [])
    {claimedGroup, claimed, deleted, cursor} = walk
    {Stream.putGroup(stream, groupName, claimedGroup), claimed, deleted, cursor}
  end

  def createConsumer(stream, groupName, name, now) do
    {:ok, group} = Stream.fetchGroup(stream, groupName)

    if Map.has_key?(group.consumers, name) do
      {stream, 0}
    else
      group = ensureConsumer(group, name, now)
      {Stream.putGroup(stream, groupName, group), 1}
    end
  end

  def deleteConsumer(stream, groupName, name) do
    {:ok, group} = Stream.fetchGroup(stream, groupName)

    case Map.fetch(group.consumers, name) do
      :error ->
        {stream, 0}

      {:ok, state} ->
        removed = MapSet.size(state.pending)

        pending =
          Enum.reduce(state.pending, group.pending, fn id, acc -> Map.delete(acc, id) end)

        group = %{group | consumers: Map.delete(group.consumers, name), pending: pending}
        {Stream.putGroup(stream, groupName, group), removed}
    end
  end

  def setId(stream, groupName, id) do
    {:ok, group} = Stream.fetchGroup(stream, groupName)
    Stream.putGroup(stream, groupName, %{group | lastDeliveredId: id})
  end

  defp autoclaimWalk(_stream, group, _consumer, _minIdle, [], _count, _now, claimed, deleted) do
    {group, Enum.reverse(claimed), Enum.reverse(deleted), {0, 0}}
  end

  defp autoclaimWalk(_stream, group, _consumer, _minIdle, [next | _], 0, _now, claimed, deleted) do
    {group, Enum.reverse(claimed), Enum.reverse(deleted), next}
  end

  defp autoclaimWalk(stream, group, consumer, minIdle, [id | rest], count, now, claimed, deleted) do
    entry = Map.fetch!(group.pending, id)

    cond do
      now - entry.deliveryTime < minIdle ->
        autoclaimWalk(stream, group, consumer, minIdle, rest, count, now, claimed, deleted)

      not Stream.member?(stream, id) ->
        group = removePending(group, id, entry.consumer)
        autoclaimWalk(stream, group, consumer, minIdle, rest, count, now, claimed, [id | deleted])

      true ->
        {:ok, fields} = Stream.get(stream, id)
        group = reassign(group, id, entry, consumer, now, entry.deliveryCount + 1)

        autoclaimWalk(
          stream,
          group,
          consumer,
          minIdle,
          rest,
          count - 1,
          now,
          [{id, fields} | claimed],
          deleted
        )
    end
  end

  defp claimOne(stream, group, consumer, minIdle, id, opts, now) do
    case Map.fetch(group.pending, id) do
      {:ok, entry} ->
        if now - entry.deliveryTime >= minIdle do
          absorb(stream, group, consumer, id, entry, opts, now)
        else
          {:skip, group}
        end

      :error ->
        if opts.force and Stream.member?(stream, id) do
          entry = %{consumer: consumer, deliveryTime: now, deliveryCount: 0}
          absorb(stream, group, consumer, id, entry, opts, now)
        else
          {:skip, group}
        end
    end
  end

  defp absorb(stream, group, consumer, id, entry, opts, now) do
    if Stream.member?(stream, id) do
      deliveryCount = claimCount(entry, opts)
      deliveryTime = claimTime(opts, now)
      group = reassign(group, id, entry, consumer, deliveryTime, deliveryCount)
      payload = if opts.justId, do: {id, nil}, else: {id, elem(streamFields(stream, id), 1)}
      {:claimed, group, payload}
    else
      {:dropped, removePending(group, id, entry.consumer)}
    end
  end

  defp streamFields(stream, id) do
    {:ok, fields} = Stream.get(stream, id)
    {id, fields}
  end

  defp claimCount(entry, opts) do
    cond do
      opts.retryCount != nil -> opts.retryCount
      opts.justId -> entry.deliveryCount
      true -> entry.deliveryCount + 1
    end
  end

  defp claimTime(opts, now) do
    cond do
      opts.time != nil -> opts.time
      opts.idle != nil -> now - opts.idle
      true -> now
    end
  end

  defp reassign(group, id, entry, consumer, deliveryTime, deliveryCount) do
    group
    |> removeFromConsumer(id, entry.consumer)
    |> ensureConsumer(consumer, deliveryTime)
    |> putPending(id, consumer, deliveryTime, deliveryCount)
  end

  defp putPending(group, id, consumer, deliveryTime, deliveryCount) do
    pending =
      Map.put(group.pending, id, %{
        consumer: consumer,
        deliveryTime: deliveryTime,
        deliveryCount: deliveryCount
      })

    state = Map.fetch!(group.consumers, consumer)
    consumers = Map.put(group.consumers, consumer, addToConsumer(state, id, deliveryTime))
    %{group | pending: pending, consumers: consumers}
  end

  defp addPending(group, id, consumer, now) do
    pending =
      Map.put(group.pending, id, %{consumer: consumer, deliveryTime: now, deliveryCount: 1})

    state = Map.fetch!(group.consumers, consumer)
    consumers = Map.put(group.consumers, consumer, addToConsumer(state, id, now))
    %{group | pending: pending, consumers: consumers}
  end

  defp removePending(group, id, consumer) do
    pending = Map.delete(group.pending, id)
    consumers = removeFromConsumerMap(group.consumers, id, consumer)
    %{group | pending: pending, consumers: consumers}
  end

  defp removeFromConsumer(group, id, consumer) do
    %{group | consumers: removeFromConsumerMap(group.consumers, id, consumer)}
  end

  defp removeFromConsumerMap(consumers, id, consumer) do
    case Map.fetch(consumers, consumer) do
      {:ok, state} -> Map.put(consumers, consumer, %{state | pending: MapSet.delete(state.pending, id)})
      :error -> consumers
    end
  end

  defp addToConsumer(state, id, now) do
    %{state | pending: MapSet.put(state.pending, id), seenTime: now, activeTime: now}
  end

  defp ensureConsumer(group, name, now) do
    if Map.has_key?(group.consumers, name) do
      group
    else
      state = %{seenTime: now, activeTime: now, pending: MapSet.new()}
      %{group | consumers: Map.put(group.consumers, name, state)}
    end
  end

  defp touchConsumer(group, name, now) do
    state = Map.fetch!(group.consumers, name)
    %{group | consumers: Map.put(group.consumers, name, %{state | seenTime: now, activeTime: now})}
  end

  defp takeCount(list, nil), do: list
  defp takeCount(list, count), do: Enum.take(list, count)
end
