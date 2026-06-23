defmodule Excache.Command.Blocking do
  @moduledoc false

  alias Excache.Command.Reply

  @names ~w(BLPOP BRPOP BLMOVE BRPOPLPUSH BLMPOP)

  def commandNames, do: @names

  def dispatch("BLPOP", args, state), do: popCommand(args, :left, state)
  def dispatch("BRPOP", args, state), do: popCommand(args, :right, state)

  def dispatch("BLMOVE", [source, destination, fromRaw, toRaw, timeoutRaw], state) do
    with {:ok, fromSide} <- parseSide(fromRaw),
         {:ok, toSide} <- parseSide(toRaw) do
      withTimeout(timeoutRaw, state, fn timeout ->
        submit([source], {:move, fromSide, toSide, destination}, timeout, state)
      end)
    else
      :error -> {[Reply.syntax()], state}
    end
  end

  def dispatch("BRPOPLPUSH", [source, destination, timeoutRaw], state) do
    withTimeout(timeoutRaw, state, fn timeout ->
      submit([source], {:move, :right, :left, destination}, timeout, state)
    end)
  end

  def dispatch("BLMPOP", [timeoutRaw, numkeysRaw | rest], state) when rest != [] do
    withTimeout(timeoutRaw, state, fn timeout ->
      case parseMpop(numkeysRaw, rest) do
        {:ok, keys, side, count} -> submit(keys, {:mpop, side, count}, timeout, state)
        {:reply, term} -> {[term], state}
      end
    end)
  end

  def dispatch(_name, _args, _state), do: :unhandled

  defp popCommand(args, side, state) when length(args) >= 2 do
    {keys, [timeoutRaw]} = Enum.split(args, length(args) - 1)

    withTimeout(timeoutRaw, state, fn timeout ->
      submit(keys, {:pop, side}, timeout, state)
    end)
  end

  defp popCommand(_args, _side, _state), do: :unhandled

  defp submit(keys, op, timeout, state) do
    request = %{keys: keys, op: op, timeout: timeout}

    if state.in_multi do
      {[Excache.Blocking.attempt(request)], state}
    else
      case Excache.Blocking.acquire(request) do
        {:served, term} -> {[term], state}
        {:blocked, ref} -> {[{:blocked, ref}], state}
      end
    end
  end

  defp withTimeout(raw, state, fun) do
    case parseTimeout(raw) do
      {:ok, timeout} -> fun.(timeout)
      {:reply, term} -> {[term], state}
    end
  end

  defp parseTimeout(raw) do
    case Reply.parse_score(raw) do
      {:ok, seconds} when seconds < 0 -> {:reply, {:error, "ERR timeout is negative"}}
      {:ok, seconds} -> {:ok, trunc(seconds * 1000)}
      :error -> {:reply, {:error, "ERR timeout is not a float or out of range"}}
    end
  end

  defp parseSide(raw) do
    case String.upcase(raw) do
      "LEFT" -> {:ok, :left}
      "RIGHT" -> {:ok, :right}
      _other -> :error
    end
  end

  defp parseMpop(numkeysRaw, rest) do
    case Integer.parse(numkeysRaw) do
      {numkeys, ""} when numkeys > 0 -> splitMpop(numkeys, rest)
      _invalid -> {:reply, {:error, "ERR numkeys should be greater than 0"}}
    end
  end

  defp splitMpop(numkeys, rest) when length(rest) > numkeys do
    {keys, [directionRaw | tail]} = Enum.split(rest, numkeys)

    case parseSide(directionRaw) do
      {:ok, side} -> mpopCount(keys, side, tail)
      :error -> {:reply, Reply.syntax()}
    end
  end

  defp splitMpop(_numkeys, _rest), do: {:reply, Reply.syntax()}

  defp mpopCount(keys, side, []), do: {:ok, keys, side, 1}

  defp mpopCount(keys, side, [countToken, countRaw]) do
    if String.upcase(countToken) == "COUNT" do
      case Integer.parse(countRaw) do
        {count, ""} when count > 0 -> {:ok, keys, side, count}
        _invalid -> {:reply, {:error, "ERR count should be greater than 0"}}
      end
    else
      {:reply, Reply.syntax()}
    end
  end

  defp mpopCount(_keys, _side, _tail), do: {:reply, Reply.syntax()}
end
