defmodule CloudCache do
  @moduledoc """
  CloudCache is a flexible and pluggable caching layer for cloud storage
  operations, designed to simplify interactions with cloud providers
  like AWS S3.

  ## Usage

  CloudCache automatically starts the configured caches when the application
  starts. This means if you're using the default adapter, `CloudCache.Adapters.S3`,
  you don't need to do anything else.

  If you want to start the caches manually then first set `auto_start` to `false`
  in your config:

  ```elixir
  config :cloud_cache, auto_start: false
  ```

  Then you can start `CloudCache` manually:

  ```elixir
  CloudCache.start_link([CloudCache.Adapters.S3])
  ```

  You can also start `CloudCache` automatically by adding it to your application's
  supervision tree:

  ```elixir
  children = [
    # Starts an instance with a single cache adapter
    {CloudCache, [CloudCache.Adapters.S3]},

    # Starts an instance with a cache adapter and additional options
    {CloudCache, {[CloudCache.Adapters.S3], [timeout: 5_000]}},

    # Starts an instance with a custom name, cache adapter, and additional options
    {CloudCache, {:my_cache_name, [CloudCache.Adapters.S3], [timeout: 5_000]}},

    # Uses a keyword list for configuration, including name, caches, and options
    {CloudCache, [name: :my_cache_name, caches: [CloudCache.Adapters.S3], timeout: 5_000]}
  ]

  Supervisor.start_link(children, strategy: :one_for_one)
  ```

  ## Examples

  ### Listing Objects in a Bucket

  ```elixir
  CloudCache.list_objects("my-bucket")
  ```

  ### Uploading an Object

  ```elixir
  CloudCache.put_object("my-bucket", "path/to/object.txt", "file content")
  ```

  ### Downloading an Object

  ```elixir
  {:ok, content} = CloudCache.get_object("my-bucket", "path/to/object.txt")
  ```

  ### Generating a Pre-Signed URL

  ```elixir
  {:ok, url} = CloudCache.presign_url("my-bucket", :post, "path/to/object.txt", expires_in: 3600)
  ```

  ## Adapters

  `CloudCache` supports pluggable adapters. Each adapter must implement the required
  behavior for interacting with the cloud storage provider. The default adapter is
  `CloudCache.Adapters.S3`.

  For more details, refer to the adapter documentation.
  """
  use Supervisor

  alias CloudCache.Adapter

  @registry CloudCache.Registry
  @instances :instances
  @default_adapter CloudCache.Adapters.S3
  @default_name __MODULE__

  @doc """
  Returns a list pids of all running instances.

  ### Examples

      iex> CloudCache.instances()
  """
  def instances do
    @registry
    |> Registry.lookup(@instances)
    |> Enum.map(fn {pid, _value} -> pid end)
  end

  @doc """
  Returns the list of children for the given instance.

  ### Examples

      iex> CloudCache.which_children()
  """
  def which_children(name \\ @default_name) do
    Supervisor.which_children(name)
  end

  @doc """
  Returns the pid of the instance with the given name.

  ### Examples

      iex> CloudCache.whereis()
  """
  def whereis(name \\ @default_name) do
    Process.whereis(name)
  end

  @doc """
  Starts a new instance with the given caches and options.

  ### Examples

      iex> CloudCache.start_link([CloudCache.Adapters.S3])
  """
  def start_link(caches, opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, @default_name)
    Supervisor.start_link(__MODULE__, {name, caches}, Keyword.put(opts, :name, name))
  end

  @doc """
  Returns a child specification for the given caches and options.
  """
  def child_spec({caches, opts}) do
    %{
      id: {__MODULE__, opts[:id] || opts[:name] || opts[:key] || opts[:default]},
      start: {__MODULE__, :start_link, [caches, opts]},
      type: :supervisor,
      restart: Keyword.get(opts, :restart, :permanent),
      shutdown: Keyword.get(opts, :shutdown, 5_000)
    }
  end

  def child_spec(list) do
    cond do
      list === [] -> child_spec({[], []})
      Keyword.keyword?(list) -> list |> Keyword.pop(:caches, []) |> child_spec()
      true -> child_spec({list, []})
    end
  end

  @impl true
  def init({name, caches}) do
    children = collect_children(caches)

    case Registry.register(@registry, @instances, nil) do
      {:ok, _} -> Supervisor.init(children, strategy: :one_for_one)
      {:error, {:already_registered, _}} -> raise "instance already started: #{inspect(name)}"
    end
  end

  defp collect_children(caches) do
    caches
    |> Enum.map(fn
      {adapter, child_spec_args} -> {adapter, child_spec_args}
      adapter -> {adapter, []}
    end)
    |> Enum.reduce([], fn {adapter, child_spec_args}, acc ->
      [adapter.child_spec(child_spec_args) | acc]
    end)
    |> Enum.reverse()
  end

  # Non-Multipart Upload API

  def presign(bucket, http_method, object, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.presign(bucket, http_method, object, opts)
  end

  def presign_post(bucket, object, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.presign_post(bucket, object, opts)
  end

  def list_buckets(opts \\ []) do
    opts
    |> adapter()
    |> Adapter.list_buckets(opts)
  end

  def create_bucket(bucket, region, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.create_bucket(bucket, region, opts)
  end

  def list_objects(bucket, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.list_objects(bucket, opts)
  end

  def head_object(bucket, object, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.head_object(bucket, object, opts)
  end

  def delete_object(bucket, object, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.delete_object(bucket, object, opts)
  end

  def get_object(bucket, object, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.get_object(bucket, object, opts)
  end

  def put_object(bucket, object, body, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.put_object(bucket, object, body, opts)
  end

  def copy_object(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.copy_object(dest_bucket, dest_object, src_bucket, src_object, opts)
  end

  # Multipart Upload API

  def presign_part(bucket, object, upload_id, part_number, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.presign_part(bucket, object, upload_id, part_number, opts)
  end

  def upload_part(bucket, object, upload_id, part_number, body, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.upload_part(bucket, object, upload_id, part_number, body, opts)
  end

  def list_parts(bucket, object, upload_id, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.list_parts(bucket, object, upload_id, opts)
  end

  def complete_multipart_upload(bucket, object, upload_id, parts, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.complete_multipart_upload(bucket, object, upload_id, parts, opts)
  end

  def abort_multipart_upload(bucket, object, upload_id, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.abort_multipart_upload(bucket, object, upload_id, opts)
  end

  def create_multipart_upload(bucket, object, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.create_multipart_upload(bucket, object, opts)
  end

  def copy_object_multipart(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.copy_object_multipart(dest_bucket, dest_object, src_bucket, src_object, opts)
  end

  def copy_parts(
        dest_bucket,
        dest_object,
        src_bucket,
        src_object,
        upload_id,
        content_length,
        opts \\ []
      ) do
    opts
    |> adapter()
    |> Adapter.copy_parts(
      dest_bucket,
      dest_object,
      src_bucket,
      src_object,
      upload_id,
      content_length,
      opts
    )
  end

  def copy_part(
        dest_bucket,
        dest_object,
        src_bucket,
        src_object,
        upload_id,
        part_number,
        src_range,
        opts \\ []
      ) do
    opts
    |> adapter()
    |> Adapter.copy_part(
      dest_bucket,
      dest_object,
      src_bucket,
      src_object,
      upload_id,
      part_number,
      src_range,
      opts
    )
  end

  defp adapter(opts) do
    opts[:cloud_cache][:adapter] || @default_adapter
  end
end
