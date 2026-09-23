defmodule Excache.MixProject do
  use Mix.Project

  @version "0.1.0"
  @sourceUrl "https://github.com/dualfroz/excache"

  def project do
    [
      app: :excache,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Redis-compatible in-memory data store server written in Elixir.",
      package: package(),
      name: "Excache",
      source_url: @sourceUrl
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  def application do
    [
      extra_applications: [:logger],
      mod: {Excache.Application, []}
    ]
  end

  defp deps do
    []
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @sourceUrl},
      files: ~w(lib config mix.exs .formatter.exs README.md LICENSE)
    ]
  end
end
