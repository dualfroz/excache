defmodule Excache.Command.Introspection.Client do
  @moduledoc false

  alias Excache.Clients
  alias Excache.Command.Reply

  @help [
    "CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "ID",
    "    Return the client id of the current connection.",
    "INFO",
    "    Return information about the current connection.",
    "GETNAME",
    "    Return the name of the current connection.",
    "SETNAME <name>",
    "    Assign the name <name> to the current connection.",
    "LIST [ID <id> [<id> ...]] [TYPE normal|master|replica|pubsub]",
    "    Return one line of information per connection.",
    "KILL <ip:port>",
    "    Close the connection reachable at <ip:port>.",
    "KILL <filter> <value> [<filter> <value> ...]",
    "    Close connections matching ID, ADDR, LADDR, TYPE, MAXAGE or SKIPME.",
    "HELP",
    "    Print this help."
  ]

  @killFilters ~w(ID ADDR LADDR TYPE MAXAGE SKIPME)
  @clientTypes ~w(normal master replica pubsub)

  def dispatch("ID", [], state),
    do: withClient(state, fn client -> {:integer, client.id} end)

  def dispatch("INFO", [], state),
    do: withClient(state, fn client -> {:bulk, line(client, now())} end)

  def dispatch("GETNAME", [], state),
    do: withClient(state, fn client -> {:bulk, client.name} end)

  def dispatch("SETNAME", [name], state) do
    if validName?(name) do
      withClient(state, fn client ->
        :ok = Clients.setName(client.pid, name)
        {:simple, "OK"}
      end)
    else
      {[{:error, "ERR Client names cannot contain spaces, newlines or special characters."}],
       state}
    end
  end

  def dispatch("LIST", args, state) do
    case listFilter(args) do
      {:ok, matcher} ->
        moment = now()

        text =
          Clients.snapshot().clients
          |> Enum.filter(matcher)
          |> Enum.map_join("", fn client -> line(client, moment) <> "\n" end)

        {[{:bulk, text}], state}

      :error ->
        {[Reply.syntax()], state}
    end
  end

  def dispatch("KILL", [address], state) do
    killed = Clients.kill(fn client -> client.addr == address end, self())

    if killed > 0 do
      {[{:simple, "OK"}], state}
    else
      {[{:error, "ERR No such client address in the client list"}], state}
    end
  end

  def dispatch("KILL", [_, _ | _] = args, state) do
    case killFilter(args) do
      {:ok, matcher} -> {[{:integer, Clients.kill(matcher, self())}], state}
      :error -> {[Reply.syntax()], state}
    end
  end

  def dispatch("HELP", [], state),
    do: {[{:array, Enum.map(@help, fn text -> {:simple, text} end)}], state}

  def dispatch(subcommand, _args, state) do
    {[
       {:error,
        "ERR Unknown subcommand or wrong number of arguments for '#{subcommand}'. " <>
          "Try CLIENT HELP."}
     ], state}
  end

  defp line(client, moment) do
    Enum.join(
      [
        "id=#{client.id}",
        "addr=#{client.addr}",
        "laddr=#{client.laddr}",
        "name=#{client.name}",
        "age=#{elapsed(moment, client.connectedAt)}",
        "idle=#{elapsed(moment, client.lastActiveAt)}",
        "flags=N",
        "db=0",
        "sub=#{Clients.subscriptionCount(client.pid)}",
        "psub=0",
        "tot-net-in=#{client.netIn}",
        "tot-net-out=#{client.netOut}"
      ],
      " "
    )
  end

  defp withClient(state, build) do
    case Clients.find(self()) do
      nil -> {[{:error, "ERR CLIENT is only available on a server connection"}], state}
      client -> {[build.(client)], state}
    end
  end

  defp listFilter([]), do: {:ok, fn _client -> true end}

  defp listFilter([keyword | [_ | _] = rest]) do
    case String.upcase(keyword) do
      "ID" -> idFilter(rest)
      "TYPE" -> typeFilter(rest)
      _other -> :error
    end
  end

  defp listFilter(_args), do: :error

  defp idFilter(raw) do
    ids = Enum.map(raw, &parseId/1)

    if :error in ids do
      :error
    else
      wanted = MapSet.new(ids)
      {:ok, fn client -> MapSet.member?(wanted, client.id) end}
    end
  end

  defp typeFilter([type]) do
    case String.downcase(type) do
      "normal" -> {:ok, fn client -> Clients.subscriptionCount(client.pid) == 0 end}
      "pubsub" -> {:ok, fn client -> Clients.subscriptionCount(client.pid) > 0 end}
      "master" -> {:ok, fn _client -> false end}
      "replica" -> {:ok, fn _client -> false end}
      _other -> :error
    end
  end

  defp typeFilter(_raw), do: :error

  defp killFilter(args) do
    case parseFilters(args, %{skipme: true}) do
      {:ok, filters} -> {:ok, buildMatcher(filters)}
      :error -> :error
    end
  end

  defp parseFilters([], filters), do: {:ok, filters}

  defp parseFilters([keyword, value | rest], filters) do
    upcased = String.upcase(keyword)

    if upcased in @killFilters do
      case addFilter(upcased, value, filters) do
        {:ok, updated} -> parseFilters(rest, updated)
        :error -> :error
      end
    else
      :error
    end
  end

  defp parseFilters(_args, _filters), do: :error

  defp addFilter("ID", value, filters) do
    case parseId(value) do
      :error -> :error
      id -> {:ok, Map.put(filters, :id, id)}
    end
  end

  defp addFilter("ADDR", value, filters), do: {:ok, Map.put(filters, :addr, value)}
  defp addFilter("LADDR", value, filters), do: {:ok, Map.put(filters, :laddr, value)}

  defp addFilter("TYPE", value, filters) do
    lowered = String.downcase(value)
    if lowered in @clientTypes, do: {:ok, Map.put(filters, :type, lowered)}, else: :error
  end

  defp addFilter("MAXAGE", value, filters) do
    case Integer.parse(value) do
      {seconds, ""} when seconds >= 0 -> {:ok, Map.put(filters, :maxage, seconds)}
      _invalid -> :error
    end
  end

  defp addFilter("SKIPME", value, filters) do
    case String.downcase(value) do
      "yes" -> {:ok, Map.put(filters, :skipme, true)}
      "no" -> {:ok, Map.put(filters, :skipme, false)}
      _other -> :error
    end
  end

  defp buildMatcher(filters) do
    caller = self()
    moment = now()

    fn client ->
      matchesId?(filters, client) and matchesAddr?(filters, client) and
        matchesLaddr?(filters, client) and matchesType?(filters, client) and
        matchesAge?(filters, client, moment) and matchesSkipme?(filters, client, caller)
    end
  end

  defp matchesId?(%{id: id}, client), do: client.id == id
  defp matchesId?(_filters, _client), do: true

  defp matchesAddr?(%{addr: addr}, client), do: client.addr == addr
  defp matchesAddr?(_filters, _client), do: true

  defp matchesLaddr?(%{laddr: laddr}, client), do: client.laddr == laddr
  defp matchesLaddr?(_filters, _client), do: true

  defp matchesType?(%{type: "normal"}, client), do: Clients.subscriptionCount(client.pid) == 0
  defp matchesType?(%{type: "pubsub"}, client), do: Clients.subscriptionCount(client.pid) > 0
  defp matchesType?(%{type: _other}, _client), do: false
  defp matchesType?(_filters, _client), do: true

  defp matchesAge?(%{maxage: maxage}, client, moment),
    do: elapsed(moment, client.connectedAt) >= maxage

  defp matchesAge?(_filters, _client, _moment), do: true

  defp matchesSkipme?(%{skipme: true}, client, caller), do: client.pid != caller
  defp matchesSkipme?(_filters, _client, _caller), do: true

  defp parseId(raw) do
    case Integer.parse(raw) do
      {id, ""} when id > 0 -> id
      _invalid -> :error
    end
  end

  defp validName?(name), do: not String.contains?(name, [" ", "\n", "\r"])

  defp elapsed(moment, since), do: div(moment - since, 1000)

  defp now, do: System.system_time(:millisecond)
end
