defmodule NamedSQL.MixProject do
  use Mix.Project

  def project do
    [
      app: :named_sql,
      version: "0.1.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      description: "SQL-first named parameters for Ecto.Repo.query/3 with compile-time validation",
      package: package(),
      deps: deps(),
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/dadarakt/named_sql"
      }
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.32", only: :dev, runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
