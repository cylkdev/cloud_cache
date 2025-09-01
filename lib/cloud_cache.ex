defmodule CloudCache do
  @moduledoc """
  Documentation for `CloudCache`.

  Getting Started:

  Create an adapter:

      defmodule MyApp.S3Endpoint do
        use CloudCache.Endpoint,
          adapter: CloudCache.Adapters.S3,
          options: []
      end

  You can add start the cache adapter:

      CloudCache.start_link(caches: [MyApp.S3Endpoint])

  You can also create a container:

      defmodule MyApp.S3Container do
        use CloudCache.Container,
          endpoint: MyApp.S3Endpoint,
          source: "test-bucket",
          options: [region: "us-west-1"]
      end

  You can start the containers as well through CloudCache.start_link().
  When the containers are used each unique adapter configuration is
  started automatically.

      CloudCache.start_link(containers: [MyApp.S3Container])
  """
  def start_link(opts \\ []) do
    CloudCache.Supervisor.start_link(opts)
  end
end
