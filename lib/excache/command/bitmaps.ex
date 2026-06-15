defmodule Excache.Command.Bitmaps do
  @moduledoc false

  alias Excache.Store
  alias Excache.Command.Reply

  def dispatch("SETBIT", [key, offsetRaw, valueRaw], state) do
    with {offset, ""} when offset >= 0 <- Integer.parse(offsetRaw),
         value when value in [0, 1] <- parseBitValue(valueRaw) do
      resultReply(Store.setbit(key, offset, value), state)
    else
      _ -> reply({:error, "ERR bit offset is not an integer or out of range"}, state)
    end
  end

  def dispatch("GETBIT", [key, offsetRaw], state) do
    case Integer.parse(offsetRaw) do
      {offset, ""} when offset >= 0 -> resultReply(Store.getbit(key, offset), state)
      _ -> reply({:error, "ERR bit offset is not an integer or out of range"}, state)
    end
  end

  def dispatch("BITCOUNT", [key], state), do: resultReply(Store.bitcount(key, :whole), state)

  def dispatch("BITCOUNT", [key, startRaw, endRaw | rest], state) do
    with {:ok, unit} <- bitcountUnit(rest),
         {start, ""} <- Integer.parse(startRaw),
         {stop, ""} <- Integer.parse(endRaw) do
      resultReply(Store.bitcount(key, {unit, start, stop}), state)
    else
      _ -> reply(Reply.syntax(), state)
    end
  end

  def dispatch("BITPOS", [key, bitRaw], state) do
    case parseBitValue(bitRaw) do
      bit when bit in [0, 1] -> resultReply(Store.bitpos(key, bit, :whole, false), state)
      _ -> reply({:error, "ERR The bit argument must be 1 or 0."}, state)
    end
  end

  def dispatch("BITPOS", [key, bitRaw, startRaw | rest], state) do
    with bit when bit in [0, 1] <- parseBitValue(bitRaw),
         {start, ""} <- Integer.parse(startRaw),
         {:ok, range, endGiven} <- bitposRange(start, rest) do
      resultReply(Store.bitpos(key, bit, range, endGiven), state)
    else
      _ -> reply(Reply.syntax(), state)
    end
  end

  def dispatch("BITOP", [opRaw, dest | [_ | _] = sources], state) do
    case parseBitop(opRaw) do
      {:ok, :not} when length(sources) != 1 ->
        reply({:error, "ERR BITOP NOT must be called with a single source key."}, state)

      {:ok, operation} ->
        resultReply(Store.bitop(operation, dest, sources), state)

      :error ->
        reply(Reply.syntax(), state)
    end
  end

  def dispatch("BITFIELD", [key | operations], state) do
    case parseBitfield(operations) do
      {:ok, parsed, writes?} ->
        case Store.bitfield(key, parsed, writes?) do
          {:ok, results} ->
            reply({:array, Enum.map(results, &bitfieldResult/1)}, state)

          {:error, reason} ->
            reply(bitError(reason), state)
        end

      :error ->
        reply(Reply.syntax(), state)
    end
  end

  def dispatch(_name, _args, _state), do: :unhandled

  defp parseBitValue("0"), do: 0
  defp parseBitValue("1"), do: 1
  defp parseBitValue(_), do: :error

  defp bitcountUnit([]), do: {:ok, :byte}
  defp bitcountUnit([unit]) when unit in ["BYTE", "byte"], do: {:ok, :byte}
  defp bitcountUnit([unit]) when unit in ["BIT", "bit"], do: {:ok, :bit}
  defp bitcountUnit(_), do: :error

  defp bitposRange(start, []), do: {:ok, {:byte, start, -1}, false}

  defp bitposRange(start, [endRaw]) do
    case Integer.parse(endRaw) do
      {stop, ""} -> {:ok, {:byte, start, stop}, true}
      _ -> :error
    end
  end

  defp bitposRange(start, [endRaw, unit]) do
    with {stop, ""} <- Integer.parse(endRaw),
         {:ok, kind} <- posUnit(unit) do
      {:ok, {kind, start, stop}, true}
    else
      _ -> :error
    end
  end

  defp bitposRange(_start, _rest), do: :error

  defp posUnit(unit) when unit in ["BYTE", "byte"], do: {:ok, :byte}
  defp posUnit(unit) when unit in ["BIT", "bit"], do: {:ok, :bit}
  defp posUnit(_), do: :error

  defp parseBitop(op) do
    case String.upcase(op) do
      "AND" -> {:ok, :and}
      "OR" -> {:ok, :or}
      "XOR" -> {:ok, :xor}
      "NOT" -> {:ok, :not}
      _ -> :error
    end
  end

  defp parseBitfield(tokens), do: parseBitfield(tokens, :wrap, [], false)

  defp parseBitfield([], _overflow, acc, writes?), do: {:ok, Enum.reverse(acc), writes?}

  defp parseBitfield([token | rest], overflow, acc, writes?) do
    case String.upcase(token) do
      "OVERFLOW" ->
        case rest do
          [modeRaw | remaining] ->
            case overflowMode(modeRaw) do
              {:ok, mode} -> parseBitfield(remaining, mode, acc, writes?)
              :error -> :error
            end

          [] ->
            :error
        end

      "GET" ->
        case rest do
          [typeRaw, offsetRaw | remaining] ->
            with {:ok, signed, bits} <- parseType(typeRaw),
                 {:ok, offset} <- parseOffset(offsetRaw, bits) do
              parseBitfield(remaining, overflow, [{:get, signed, bits, offset} | acc], writes?)
            else
              _ -> :error
            end

          _ ->
            :error
        end

      "SET" ->
        case rest do
          [typeRaw, offsetRaw, valueRaw | remaining] ->
            with {:ok, signed, bits} <- parseType(typeRaw),
                 {:ok, offset} <- parseOffset(offsetRaw, bits),
                 {value, ""} <- Integer.parse(valueRaw) do
              parseBitfield(
                remaining,
                overflow,
                [{:set, signed, bits, offset, value, overflow} | acc],
                true
              )
            else
              _ -> :error
            end

          _ ->
            :error
        end

      "INCRBY" ->
        case rest do
          [typeRaw, offsetRaw, valueRaw | remaining] ->
            with {:ok, signed, bits} <- parseType(typeRaw),
                 {:ok, offset} <- parseOffset(offsetRaw, bits),
                 {delta, ""} <- Integer.parse(valueRaw) do
              parseBitfield(
                remaining,
                overflow,
                [{:incrby, signed, bits, offset, delta, overflow} | acc],
                true
              )
            else
              _ -> :error
            end

          _ ->
            :error
        end

      _ ->
        :error
    end
  end

  defp overflowMode(mode) do
    case String.upcase(mode) do
      "WRAP" -> {:ok, :wrap}
      "SAT" -> {:ok, :sat}
      "FAIL" -> {:ok, :fail}
      _ -> :error
    end
  end

  defp parseType(<<prefix, digits::binary>>) when prefix in [?i, ?I, ?u, ?U] do
    case Integer.parse(digits) do
      {bits, ""} when bits >= 1 and bits <= 64 -> {:ok, prefix in [?i, ?I], bits}
      _ -> :error
    end
  end

  defp parseType(_), do: :error

  defp parseOffset(<<"#", digits::binary>>, bits) do
    case Integer.parse(digits) do
      {multiple, ""} when multiple >= 0 -> {:ok, multiple * bits}
      _ -> :error
    end
  end

  defp parseOffset(raw, _bits) do
    case Integer.parse(raw) do
      {offset, ""} when offset >= 0 -> {:ok, offset}
      _ -> :error
    end
  end

  defp bitfieldResult(nil), do: {:bulk, nil}
  defp bitfieldResult(value), do: {:integer, value}

  defp resultReply({:ok, value}, state), do: reply({:integer, value}, state)
  defp resultReply({:error, reason}, state), do: reply(bitError(reason), state)

  defp bitError(:wrongtype), do: Reply.wrongtype()
  defp bitError(other), do: {:error, "ERR #{other}"}

  defp reply(term, state), do: {[term], state}
end
