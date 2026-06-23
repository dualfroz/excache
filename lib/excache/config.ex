defmodule Excache.Config do
  @moduledoc false

  alias Excache.Aof
  alias Excache.Eviction
  alias Excache.Eviction.Runner
  alias Excache.Persistence

  @defaults %{
    maxmemory: 0,
    maxmemoryPolicy: :noeviction,
    maxmemorySamples: 5,
    lfuLogFactor: 10,
    lfuDecayTime: 1,
    port: 6379,
    snapshot_path: nil,
    aof_path: nil
  }

  @parameters %{
    "maxmemory" => %{env: :maxmemory, type: :bytes, mutable: true, runtime: true},
    "maxmemory-policy" => %{env: :maxmemoryPolicy, type: :policy, mutable: true, runtime: true},
    "maxmemory-samples" => %{
      env: :maxmemorySamples,
      type: {:bounded, 1, 64},
      mutable: true,
      runtime: true
    },
    "lfu-log-factor" => %{
      env: :lfuLogFactor,
      type: {:bounded, 0, 255},
      mutable: true,
      runtime: true
    },
    "lfu-decay-time" => %{
      env: :lfuDecayTime,
      type: {:bounded, 0, 1440},
      mutable: true,
      runtime: true
    },
    "port" => %{env: :port, type: {:bounded, 0, 65_535}, mutable: false, runtime: false},
    "snapshot_path" => %{env: :snapshot_path, type: :path, mutable: true, runtime: false},
    "aof_path" => %{env: :aof_path, type: :path, mutable: false, runtime: false}
  }

  @trackedPolicies [:allkeysLru, :allkeysLfu, :volatileLru, :volatileLfu]

  @units %{
    "" => 1,
    "b" => 1,
    "k" => 1000,
    "kb" => 1024,
    "m" => 1_000_000,
    "mb" => 1_048_576,
    "g" => 1_000_000_000,
    "gb" => 1_073_741_824
  }

  def names, do: @parameters |> Map.keys() |> Enum.sort()

  def get(name) do
    case Map.fetch(@parameters, String.downcase(name)) do
      {:ok, spec} -> {:ok, readSpec(spec)}
      :error -> :error
    end
  end

  def set(name, raw) do
    case prepare(name, raw) do
      {:ok, change} -> commit(change)
      {:error, reason} -> {:error, reason}
    end
  end

  def setMany(pairs) do
    pairs
    |> Enum.reduce_while({:ok, []}, fn {name, raw}, {:ok, acc} ->
      case prepare(name, raw) do
        {:ok, change} -> {:cont, {:ok, [change | acc]}}
        {:error, reason} -> {:halt, {:error, name, reason}}
      end
    end)
    |> case do
      {:ok, changes} ->
        changes |> Enum.reverse() |> Enum.each(&commit/1)
        :ok

      failure ->
        failure
    end
  end

  def getGlob(pattern) do
    chars = pattern |> String.downcase() |> String.to_charlist()

    for name <- names(), matches?(chars, String.to_charlist(name)) do
      {:ok, value} = get(name)
      {name, format(value)}
    end
  end

  def reset do
    for {_name, spec} <- @parameters, spec.runtime do
      Application.delete_env(:excache, spec.env)
    end

    Runner.invalidate()
    Runner.reloadTracker()
    :ok
  end

  defp format(nil), do: ""
  defp format(value) when is_integer(value), do: Integer.to_string(value)
  defp format(value) when is_binary(value), do: value
  defp format(policy) when is_atom(policy), do: Eviction.policyName(policy)

  def maxmemory, do: readEnv(:maxmemory)
  def policy, do: readEnv(:maxmemoryPolicy)
  def maxmemorySamples, do: readEnv(:maxmemorySamples)
  def lfuLogFactor, do: readEnv(:lfuLogFactor)
  def lfuDecayTime, do: readEnv(:lfuDecayTime)

  def tracksAccess?, do: policy() in @trackedPolicies

  defp hint(:bytes), do: "argument must be a memory value like 512, 64kb, 100mb or 1gb"

  defp hint(:policy),
    do:
      "argument must be one of the following: " <>
        Enum.map_join(Eviction.policies(), ", ", &Eviction.policyName/1)

  defp hint({:bounded, low, high}), do: "argument must be an integer between #{low} and #{high}"
  defp hint(:path), do: "argument must be a non empty path"

  defp prepare(name, raw) do
    case Map.fetch(@parameters, String.downcase(name)) do
      :error ->
        {:error, :unknown}

      {:ok, %{mutable: false}} ->
        {:error, :immutable}

      {:ok, spec} ->
        case parse(spec.type, raw) do
          {:ok, value} -> {:ok, {spec, value}}
          :error -> {:error, {:invalid, hint(spec.type)}}
        end
    end
  end

  defp commit({spec, value}) do
    previous = readSpec(spec)
    Application.put_env(:excache, spec.env, value)
    if value != previous, do: applyChange(spec)
    :ok
  end

  defp applyChange(%{env: env}) when env in [:lfuLogFactor, :lfuDecayTime],
    do: Runner.reloadTracker()

  defp applyChange(%{env: env}) when env in [:maxmemory, :maxmemoryPolicy, :maxmemorySamples],
    do: Runner.invalidate()

  defp applyChange(_spec), do: :ok

  defp parse(:bytes, raw), do: parseBytes(raw)
  defp parse(:policy, raw), do: Eviction.parsePolicy(String.downcase(raw))

  defp parse({:bounded, low, high}, raw) do
    case Integer.parse(raw) do
      {value, ""} when value >= low and value <= high -> {:ok, value}
      _ -> :error
    end
  end

  defp parse(:path, ""), do: :error
  defp parse(:path, raw), do: {:ok, raw}

  defp parseBytes(raw) do
    {digits, unit} =
      raw
      |> String.downcase()
      |> String.to_charlist()
      |> Enum.split_while(&(&1 in ?0..?9))

    with {:ok, factor} <- Map.fetch(@units, List.to_string(unit)),
         {amount, ""} <- Integer.parse(List.to_string(digits)) do
      {:ok, amount * factor}
    else
      _ -> :error
    end
  end

  defp readSpec(%{env: :snapshot_path}), do: Persistence.snapshot_path()
  defp readSpec(%{env: :aof_path}), do: Aof.path()
  defp readSpec(%{env: env}), do: readEnv(env)

  defp readEnv(env), do: Application.get_env(:excache, env, @defaults[env])

  defp matches?([], name), do: name == []

  defp matches?([?* | pattern], name),
    do: matches?(pattern, name) or (name != [] and matches?([?* | pattern], tl(name)))

  defp matches?([?? | pattern], [_char | name]), do: matches?(pattern, name)
  defp matches?([char | pattern], [char | name]), do: matches?(pattern, name)
  defp matches?(_pattern, _name), do: false
end
