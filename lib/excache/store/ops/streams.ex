defmodule Excache.Store.Ops.Streams do
  @moduledoc false

  alias Excache.Stream
  alias Excache.Stream.Groups
  alias Excache.Store.Ops.Helpers

  def apply_op(table, now, {:xadd, key, idSpec, fields, trim, nomkstream}) do
    case fetchStream(table, key, now) do
      :wrongtype ->
        {:error, :wrongtype}

      :miss when nomkstream ->
        {:ok, nil}

      existing ->
        {stream, ea} =
          case existing do
            :miss -> {Stream.new(), nil}
            {:ok, current, currentEa} -> {current, currentEa}
          end

        case Stream.add(stream, idSpec, fields, now) do
          {:ok, updated, id} ->
            trimmed = applyTrim(updated, trim)
            store(table, key, trimmed, ea)
            {:ok, Stream.formatId(id)}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  def apply_op(table, now, {:xlen, key}) do
    withStream(table, key, now, 0, fn stream -> {:ok, Stream.length(stream)} end)
  end

  def apply_op(table, now, {:xrange, key, startBound, endBound, count}) do
    withStream(table, key, now, [], fn stream ->
      {:ok, Stream.range(stream, startBound, endBound, count)}
    end)
  end

  def apply_op(table, now, {:xrevrange, key, endBound, startBound, count}) do
    withStream(table, key, now, [], fn stream ->
      {:ok, Stream.reverseRange(stream, endBound, startBound, count)}
    end)
  end

  def apply_op(table, now, {:xread, key, afterId, count}) do
    case fetchStream(table, key, now) do
      :miss ->
        {:ok, nil}

      :wrongtype ->
        {:error, :wrongtype}

      {:ok, stream, _ea} ->
        case Stream.afterId(stream, afterId, count) do
          [] -> {:ok, nil}
          entries -> {:ok, entries}
        end
    end
  end

  def apply_op(table, now, {:xdel, key, ids}) do
    case fetchStream(table, key, now) do
      :miss ->
        {:ok, 0}

      :wrongtype ->
        {:error, :wrongtype}

      {:ok, stream, ea} ->
        {updated, removed} = Stream.delete(stream, ids)
        store(table, key, updated, ea)
        {:ok, removed}
    end
  end

  def apply_op(table, now, {:xtrim, key, strategy}) do
    case fetchStream(table, key, now) do
      :miss ->
        {:ok, 0}

      :wrongtype ->
        {:error, :wrongtype}

      {:ok, stream, ea} ->
        {updated, removed} = trimBy(stream, strategy)
        store(table, key, updated, ea)
        {:ok, removed}
    end
  end

  def apply_op(table, now, {:xsetid, key, id}) do
    case fetchStream(table, key, now) do
      :miss ->
        {:error, :nokey}

      :wrongtype ->
        {:error, :wrongtype}

      {:ok, stream, ea} ->
        cond do
          id < Stream.lastEntryId(stream) ->
            {:error, :setid_smaller}

          true ->
            store(table, key, Stream.setLastId(stream, id), ea)
            {:ok, "OK"}
        end
    end
  end

  def apply_op(table, now, {:xinfoStream, key}) do
    case fetchStream(table, key, now) do
      :miss -> {:error, :nokey}
      :wrongtype -> {:error, :wrongtype}
      {:ok, stream, _ea} -> {:ok, Stream.info(stream)}
    end
  end

  def apply_op(table, now, {:xgroupCreate, key, name, startSpec, mkstream}) do
    case fetchStream(table, key, now) do
      :wrongtype ->
        {:error, :wrongtype}

      :miss when mkstream ->
        stream = Stream.new()
        {:ok, created} = Stream.createGroup(stream, name, resolveStart(stream, startSpec))
        store(table, key, created, nil)
        {:ok, "OK"}

      :miss ->
        {:error, :nokey}

      {:ok, stream, ea} ->
        case Stream.createGroup(stream, name, resolveStart(stream, startSpec)) do
          {:ok, created} ->
            store(table, key, created, ea)
            {:ok, "OK"}

          {:error, :busygroup} ->
            {:error, :busygroup}
        end
    end
  end

  def apply_op(table, now, {:xgroupDestroy, key, name}) do
    withGroupMutation(table, key, now, fn stream ->
      {updated, removed} = Stream.destroyGroup(stream, name)
      {updated, {:ok, removed}}
    end)
  end

  def apply_op(table, now, {:xgroupCreateConsumer, key, name, consumer}) do
    withExistingGroup(table, key, now, name, fn stream ->
      {updated, added} = Groups.createConsumer(stream, name, consumer, now)
      {updated, {:ok, added}}
    end)
  end

  def apply_op(table, now, {:xgroupDelConsumer, key, name, consumer}) do
    withExistingGroup(table, key, now, name, fn stream ->
      {updated, removed} = Groups.deleteConsumer(stream, name, consumer)
      {updated, {:ok, removed}}
    end)
  end

  def apply_op(table, now, {:xgroupSetId, key, name, id}) do
    withExistingGroup(table, key, now, name, fn stream ->
      {Groups.setId(stream, name, id), {:ok, "OK"}}
    end)
  end

  def apply_op(table, now, {:xreadgroup, key, name, consumer, mode, count, noack}) do
    case fetchStream(table, key, now) do
      :miss ->
        {:error, :nogroup}

      :wrongtype ->
        {:error, :wrongtype}

      {:ok, stream, ea} ->
        case Stream.fetchGroup(stream, name) do
          :error ->
            {:error, :nogroup}

          {:ok, _group} ->
            {updated, entries} = readGroup(stream, name, consumer, mode, count, noack, now)
            store(table, key, updated, ea)
            {:ok, entries}
        end
    end
  end

  def apply_op(table, now, {:xack, key, name, ids}) do
    case fetchStream(table, key, now) do
      {:ok, stream, ea} ->
        case Stream.fetchGroup(stream, name) do
          {:ok, _group} ->
            {updated, count} = Groups.ack(stream, name, ids)
            store(table, key, updated, ea)
            {:ok, count}

          :error ->
            {:ok, 0}
        end

      _ ->
        {:ok, 0}
    end
  end

  def apply_op(table, now, {:xpendingSummary, key, name}) do
    withGroupRead(table, key, now, name, fn stream ->
      {:ok, Groups.pendingSummary(stream, name)}
    end)
  end

  def apply_op(table, now, {:xpendingRange, key, name, startId, endId, count, consumer, minIdle}) do
    withGroupRead(table, key, now, name, fn stream ->
      {:ok, Groups.pendingRange(stream, name, startId, endId, count, consumer, minIdle, now)}
    end)
  end

  def apply_op(table, now, {:xclaim, key, name, consumer, minIdle, ids, opts}) do
    withGroupMutationChecked(table, key, now, name, fn stream ->
      {updated, results} = Groups.claim(stream, name, consumer, minIdle, ids, opts, now)
      {updated, {:ok, results}}
    end)
  end

  def apply_op(table, now, {:xautoclaim, key, name, consumer, minIdle, startId, count}) do
    withGroupMutationChecked(table, key, now, name, fn stream ->
      {updated, claimed, deleted, cursor} =
        Groups.autoclaim(stream, name, consumer, minIdle, startId, count, now)

      {updated, {:ok, {cursor, claimed, deleted}}}
    end)
  end

  defp fetchStream(table, key, now) do
    case Helpers.fetch(table, key, now) do
      :miss -> :miss
      {:stream, stream, ea} -> {:ok, stream, ea}
      _ -> :wrongtype
    end
  end

  defp store(table, key, stream, ea) do
    :ets.insert(table, {key, :stream, stream, ea})
  end

  defp withStream(table, key, now, default, fun) do
    case fetchStream(table, key, now) do
      :miss -> {:ok, default}
      :wrongtype -> {:error, :wrongtype}
      {:ok, stream, _ea} -> fun.(stream)
    end
  end

  defp withGroupMutation(table, key, now, fun) do
    case fetchStream(table, key, now) do
      :miss ->
        {:ok, 0}

      :wrongtype ->
        {:error, :wrongtype}

      {:ok, stream, ea} ->
        {updated, reply} = fun.(stream)
        store(table, key, updated, ea)
        reply
    end
  end

  defp withExistingGroup(table, key, now, name, fun) do
    case fetchStream(table, key, now) do
      :miss ->
        {:error, :nogroup}

      :wrongtype ->
        {:error, :wrongtype}

      {:ok, stream, ea} ->
        case Stream.fetchGroup(stream, name) do
          :error ->
            {:error, :nogroup}

          {:ok, _group} ->
            {updated, reply} = fun.(stream)
            store(table, key, updated, ea)
            reply
        end
    end
  end

  defp withGroupMutationChecked(table, key, now, name, fun) do
    withExistingGroup(table, key, now, name, fun)
  end

  defp withGroupRead(table, key, now, name, fun) do
    case fetchStream(table, key, now) do
      :miss ->
        {:error, :nogroup}

      :wrongtype ->
        {:error, :wrongtype}

      {:ok, stream, _ea} ->
        case Stream.fetchGroup(stream, name) do
          :error -> {:error, :nogroup}
          {:ok, _group} -> fun.(stream)
        end
    end
  end

  defp readGroup(stream, name, consumer, {:new}, count, noack, now) do
    Groups.readNew(stream, name, consumer, count, noack, now)
  end

  defp readGroup(stream, name, consumer, {:history, startId}, count, _noack, now) do
    Groups.readHistory(stream, name, consumer, startId, count, now)
  end

  defp resolveStart(stream, :last), do: Stream.lastEntryId(stream)
  defp resolveStart(_stream, {:id, id}), do: id

  defp applyTrim(stream, nil), do: stream
  defp applyTrim(stream, strategy), do: elem(trimBy(stream, strategy), 0)

  defp trimBy(stream, {:maxlen, count}), do: Stream.trimMaxlen(stream, count)
  defp trimBy(stream, {:minid, id}), do: Stream.trimMinId(stream, id)
end
