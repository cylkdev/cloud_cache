defmodule CloudCache.Container do
  alias CloudCache.Endpoint

  @callback endpoint :: module()
  @callback source :: binary()
  @callback options :: keyword()

  def app(container), do: container.app()

  def endpoint(container), do: container.endpoint()

  def source(container), do: container.source()

  def options(container), do: container.options()

  def describe_object(container, key, opts \\ []) do
    container
    |> endpoint()
    |> Endpoint.describe_object(source(container), key, with_opts(opts, container))
  end

  defp with_opts(opts, container) do
    container |> options() |> Keyword.merge(opts)
  end

  defmacro __using__(opts \\ []) do
    quote do
      opts = unquote(opts)

      alias CloudCache.Container

      @behaviour CloudCache.Container

      @app opts[:app]
      @endpoint Keyword.fetch!(opts, :endpoint)
      @source Keyword.fetch!(opts, :source)
      @options opts[:options] || []

      @impl true
      def app, do: @app

      @impl true
      def endpoint, do: @endpoint

      @impl true
      def source, do: @source

      @impl true
      def options, do: @options

      def describe_object(container, key, opts \\ []) do
        Container.describe_object(__MODULE__, key, opts)
      end
    end
  end
end
