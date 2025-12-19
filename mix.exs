defmodule CloudCache.MixProject do
  use Mix.Project

  def project do
    [
      app: :cloud_cache,
      version: "0.1.0",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        doctor: :test,
        coverage: :test,
        dialyzer: :test,
        coveralls: :test,
        "coveralls.lcov": :test,
        "coveralls.json": :test,
        "coveralls.html": :test,
        "coveralls.detail": :test,
        "coveralls.post": :test
      ],
      dialyzer: [
        plt_add_apps: [:ex_unit, :mix],
        plt_ignore_apps: [],
        plt_local_path: "../dialyzer",
        plt_core_path: "../dialyzer",
        list_unused_filters: true,
        ignore_warnings: ".dialyzer-ignore.exs",
        flags: [:unmatched_returns, :no_improper_lists]
      ]
    ]
  end

  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {CloudCache.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_aws, "~> 2.0"},
      {:ex_aws_s3, "~> 2.0"},
      {:configparser_ex, ">= 0.0.0"},
      {:sweet_xml, ">= 0.0.0"},
      {:proper_case, "~> 1.0"},
      {:timex, "~> 3.0"},
      {:jason, "~> 1.0"},
      {:req, "~> 0.5"},
      {:error_message, ">= 0.0.0"},
      {:sandbox_registry, ">= 0.0.0"}
    ]
  end
end
