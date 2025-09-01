defmodule CloudCache.Container do
  alias CloudCache.Endpoint

  def endpoint(impl), do: impl.endpoint()

  def region(impl), do: impl.region()

  def source(impl), do: impl.source()

  def options(impl), do: impl.options()

  def describe_object(impl, key, opts \\ []) do
    opts = impl |> options() |> Keyword.merge(opts)

    impl
    |> endpoint()
    |> Endpoint.describe_object(source(impl), key, opts)
  end

  defmacro __using__(opts \\ []) do
    quote do
      opts = unquote(opts)

      alias CloudCache.Container

      @endpoint Keyword.fetch!(opts, :endpoint)
      @region Keyword.fetch!(opts, :region)
      @source Keyword.fetch!(opts, :source)
      @options opts[:options] || []

      def endpoint, do: @endpoint

      def region, do: @region

      def source, do: @source

      def options, do: @options

      def describe_object(impl, key, opts \\ []) do
        Container.describe_object(__MODULE__, key, opts)
      end
    end
  end
end
