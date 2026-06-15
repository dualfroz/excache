defmodule Excache.Command.Transaction do
  @moduledoc false

  alias Excache.Store

  def initial_state, do: %{in_multi: false, queued: [], multi_error: false, watches: %{}}

  def execute(command, args, state, dispatch_and_log) do
    cond do
      command == "MULTI" -> handle_multi(state)
      command == "EXEC" -> handle_exec(state, dispatch_and_log)
      command == "DISCARD" -> handle_discard(state)
      command == "WATCH" -> handle_watch(args, state)
      command == "UNWATCH" -> handle_unwatch(state)
      state.in_multi -> queue(command, args, state)
      true -> dispatch_and_log.(command, args, state)
    end
  end

  def known?(command), do: command in Excache.Command.known_commands()

  defp handle_multi(%{in_multi: true} = state) do
    {[{:error, "ERR MULTI calls can not be nested"}], state}
  end

  defp handle_multi(state) do
    {[{:simple, "OK"}], %{state | in_multi: true, queued: [], multi_error: false}}
  end

  defp handle_exec(%{in_multi: false} = state, _dispatch_and_log) do
    {[{:error, "ERR EXEC without MULTI"}], state}
  end

  defp handle_exec(%{multi_error: true} = state, _dispatch_and_log) do
    {[{:error, "ERR EXECABORT Transaction discarded because of previous errors."}], reset(state)}
  end

  defp handle_exec(state, dispatch_and_log) do
    if watches_intact?(state.watches) do
      {replies, executed} =
        Enum.reduce(Enum.reverse(state.queued), {[], state}, fn {command, args}, {acc, current} ->
          {[term], updated} = dispatch_and_log.(command, args, current)
          {[term | acc], updated}
        end)

      {[{:array, Enum.reverse(replies)}], reset(executed)}
    else
      {[{:array, nil}], reset(state)}
    end
  end

  defp handle_discard(%{in_multi: false} = state) do
    {[{:error, "ERR DISCARD without MULTI"}], state}
  end

  defp handle_discard(state), do: {[{:simple, "OK"}], reset(state)}

  defp handle_watch([], state) do
    {[{:error, "ERR wrong number of arguments for 'watch' command"}], state}
  end

  defp handle_watch(_keys, %{in_multi: true} = state) do
    {[{:error, "ERR WATCH inside MULTI is not allowed"}], state}
  end

  defp handle_watch(keys, state) do
    updated =
      Enum.reduce(keys, state, fn key, current ->
        snapshot = Store.watch_snapshot(key)
        %{current | watches: Map.put(current.watches, key, snapshot)}
      end)

    {[{:simple, "OK"}], updated}
  end

  defp handle_unwatch(state), do: {[{:simple, "OK"}], %{state | watches: %{}}}

  defp queue(command, args, state) do
    cond do
      command in ["SUBSCRIBE", "UNSUBSCRIBE"] ->
        {[{:error, "ERR #{command} is not allowed in transactions"}], mark_dirty(state)}

      not known?(command) ->
        {[{:error, "ERR unknown command '#{command}'"}], mark_dirty(state)}

      true ->
        {[{:simple, "QUEUED"}], %{state | queued: [{command, args} | state.queued]}}
    end
  end

  defp watches_intact?(watches) do
    Enum.all?(watches, fn {key, snapshot} -> Store.unchanged?(key, snapshot) end)
  end

  defp mark_dirty(state), do: %{state | multi_error: true}

  defp reset(state), do: %{state | in_multi: false, queued: [], multi_error: false, watches: %{}}
end
