defmodule Excache.Command.Introspection do
  @moduledoc false

  alias Excache.Command.Introspection.Catalog
  alias Excache.Command.Introspection.Client
  alias Excache.Command.Introspection.Info

  @names ~w(INFO CLIENT COMMAND)

  def commandNames, do: @names

  def dispatch("INFO", args, state), do: {[{:bulk, Info.render(args)}], state}

  def dispatch("CLIENT", [subcommand | rest], state),
    do: Client.dispatch(String.upcase(subcommand), rest, state)

  def dispatch("CLIENT", [], state), do: {[wrongArity("client")], state}

  def dispatch("COMMAND", [], state), do: {[fullTable()], state}

  def dispatch("COMMAND", [subcommand | rest], state),
    do: command(String.upcase(subcommand), rest, state)

  defp command("COUNT", [], state), do: {[{:integer, Catalog.count()}], state}

  defp command("LIST", [], state) do
    {[{:array, Enum.map(Catalog.names(), fn name -> {:bulk, String.downcase(name)} end)}], state}
  end

  defp command("INFO", [], state), do: {[fullTable()], state}

  defp command("INFO", names, state) do
    {[{:array, Enum.map(names, fn name -> Catalog.infoTerm(String.upcase(name)) end)}], state}
  end

  defp command("DOCS", [], state) do
    {[{:array, Enum.flat_map(Catalog.names(), &docsEntry/1)}], state}
  end

  defp command("DOCS", names, state) do
    entries =
      names
      |> Enum.map(&String.upcase/1)
      |> Enum.filter(&Catalog.known?/1)
      |> Enum.flat_map(&docsEntry/1)

    {[{:array, entries}], state}
  end

  defp command("GETKEYS", [name | args], state) do
    {[getkeys(String.upcase(name), args)], state}
  end

  defp command("GETKEYS", [], state), do: {[wrongArity("command|getkeys")], state}

  defp command(subcommand, _args, state) do
    {[
       {:error,
        "ERR Unknown subcommand or wrong number of arguments for '#{subcommand}'. " <>
          "Try COMMAND HELP."}
     ], state}
  end

  defp fullTable, do: {:array, Enum.map(Catalog.names(), &Catalog.infoTerm/1)}

  defp docsEntry(name), do: [{:bulk, String.downcase(name)}, Catalog.docsTerm(name)]

  defp getkeys(name, args) do
    cond do
      not Catalog.known?(name) ->
        {:error, "ERR Invalid command specified"}

      not Catalog.arityOk?(name, length(args) + 1) ->
        {:error, "ERR Invalid number of arguments specified for command"}

      true ->
        extractKeys(name, args)
    end
  end

  defp extractKeys(name, args) do
    case Catalog.keysFor(name, args) do
      {:ok, []} -> {:error, "ERR The command has no key arguments"}
      {:ok, keys} -> {:array, Enum.map(keys, fn key -> {:bulk, key} end)}
      :error -> {:error, "ERR Invalid arguments specified for command"}
    end
  end

  defp wrongArity(name),
    do: {:error, "ERR wrong number of arguments for '#{name}' command"}
end
