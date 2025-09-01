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

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_aws, "~> 2.0", optional: true},
      {:ex_aws_s3, "~> 2.0", optional: true},
      {:sweet_xml, ">= 0.0.0", optional: true},
      {:proper_case, "~> 1.0", optional: true},
      {:timex, "~> 3.0", optional: true},
      {:jason, "~> 1.0", optional: true},
      {:req, "~> 0.5", optional: true},
      {:error_message, ">= 0.0.0", optional: true},
      {:sandbox_registry, ">= 0.0.0", only: :test, runtime: false}
    ]
  end
end
