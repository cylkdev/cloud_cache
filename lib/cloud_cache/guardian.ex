defmodule CloudCache.Guardian do
  use Supervisor

  alias CloudCache.{Endpoint, Container}

  @default_name __MODULE__

  @default_options [
    name: @default_name
  ]

  @supervisor_options_keys [:name, :strategy, :max_restarts, :max_seconds]

  @doc """
  Starts the supervisor and its children.
  """
  def start_link(opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    supervisor_opts =
      opts
      |> Keyword.take(@supervisor_options_keys)
      |> Keyword.put(:name, opts[:name] || @default_name)

    init_opts = Keyword.drop(opts, @supervisor_options_keys)

    Supervisor.start_link(__MODULE__, init_opts, supervisor_opts)
  end

  def child_spec(opts) do
    opts =
      @default_options
      |> Keyword.merge(opts)
      |> Keyword.put(:name, opts[:name] || @default_name)

    %{
      id: opts[:name],
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @impl true
  def init(opts) do
    container_children = container_children(opts[:containers] || [])
    cache_children = cache_children(opts[:caches] || [])
    children = dedup(cache_children ++ container_children)

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp dedup(entries) do
    entries
    |> Enum.reduce(MapSet.new(), fn entry, set -> MapSet.put(set, entry) end)
    |> MapSet.to_list()
  end

  @doc false
  def container_children(containers) do
    containers
    |> Enum.map(fn mod ->
      endpoint = Container.endpoint(mod)
      container_opts = Container.options(mod)

      {endpoint, container_opts}
    end)
    |> cache_children()
  end

  @doc false
  def cache_children(caches) do
    caches
    |> Enum.map(fn
      {endpoint, child_opts} ->
        adapter = Endpoint.adapter(endpoint)

        adapter_opts =
          endpoint
          |> Endpoint.options()
          |> Keyword.merge(child_opts)

        {adapter, adapter_opts}

      endpoint ->
        adapter = Endpoint.adapter(endpoint)
        adapter_opts = Endpoint.options(endpoint)

        {adapter, adapter_opts}
    end)
    |> Enum.reduce([], fn {adapter, adapter_opts}, acc ->
      if supervisor_children_exported?(adapter) do
        [adapter.supervisor_children(adapter_opts) | acc]
      else
        acc
      end
    end)
    |> Enum.reverse()
    |> List.flatten()
  end

  defp supervisor_children_exported?(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :supervisor_children, 1)
  end
end
