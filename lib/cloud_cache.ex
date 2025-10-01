defmodule CloudCache do
  @moduledoc """
  CloudCache is a flexible and pluggable caching layer for cloud storage operations,
  designed to simplify interactions with cloud providers like AWS S3.

  ## Usage

  Starting `CloudCache` is easy, just choose your adapter and run:

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
  {:ok, url} = CloudCache.pre_sign("my-bucket", "path/to/object.txt", expires_in: 3600)
  ```

  ## Adapters

  `CloudCache` supports pluggable adapters. Each adapter must implement the required
  behavior for interacting with the cloud storage provider. The default adapter is
  `CloudCache.Adapters.S3`.

  For more details, refer to the adapter documentation.
  """
  use Supervisor

  alias CloudCache.Adapter

  @default_adapter CloudCache.Adapters.S3
  @default_name __MODULE__

  def start_link(name \\ @default_name, caches, opts \\ []) do
    Supervisor.start_link(__MODULE__, caches, Keyword.put(opts, :name, name))
  end

  def child_spec({name, caches, opts}) do
    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [name, caches, opts]},
      type: :supervisor,
      restart: Keyword.get(opts, :restart, :permanent),
      shutdown: Keyword.get(opts, :shutdown, 5_000)
    }
  end

  def child_spec({caches, opts}) do
    child_spec({@default_name, caches, opts})
  end

  def child_spec(list) do
    if Keyword.keyword?(list) do
      name = list[:name] || @default_adapter
      caches = list[:caches] || []
      opts = Keyword.drop(list, [:name, :caches])
      child_spec({name, caches, opts})
    else
      child_spec({@default_name, list, []})
    end
  end

  @impl true
  def init(caches) do
    caches
    |> Kernel.++(CloudCache.Config.caches())
    |> List.flatten()
    |> Enum.flat_map(fn
      {adapter, args} -> Adapter.supervisor_child_spec(adapter, args)
      adapter -> Adapter.supervisor_child_spec(adapter, [])
    end)
    |> Enum.reduce(MapSet.new(), fn entry, set -> MapSet.put(set, entry) end)
    |> MapSet.to_list()
    |> Supervisor.init(strategy: :one_for_one)
  end

  # Non-Multipart Upload API

  def list_buckets(opts \\ []) do
    opts
    |> adapter()
    |> Adapter.list_buckets(opts)
  end

  def head_object(bucket, object, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.head_object(bucket, object, opts)
  end

  def pre_sign(bucket, object, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.pre_sign(bucket, object, opts)
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

  def list_objects(bucket, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.list_objects(bucket, opts)
  end

  # Multipart Upload API

  def pre_sign_part(bucket, object, upload_id, part_number, opts \\ []) do
    opts
    |> adapter()
    |> Adapter.pre_sign_part(bucket, object, upload_id, part_number, opts)
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
    opts[:cloud_cache] || @default_adapter
  end
end
