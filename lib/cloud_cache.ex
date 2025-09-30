defmodule CloudCache do
  @moduledoc """
  {CloudCache, [{MyApp.CacheA, []}, MyApp.CacheB]}
  CloudCache.list_objects("test-bucket", adapter: MyApp.CacheA, s3: [access_key: "A", secret_access_key: "B"])
  """
  use Supervisor

  @default_adapter CloudCache.Adapters.S3
  @default_name __MODULE__
  def start_link(caches, opts \\ []) do
    Supervisor.start_link(__MODULE__, caches, Keyword.put_new(opts, :name, @default_name))
  end

  def child_spec({caches, child_opts, start_opts}) do
    Supervisor.child_spec({__MODULE__, [caches, start_opts]}, child_opts)
  end

  def child_spec({caches, opts}) do
    child_opts = Keyword.get(opts, :supervisor, [])
    start_opts = Keyword.drop(opts, [:caches, :supervisor])

    child_spec({caches, child_opts, start_opts})
  end

  def child_spec(opts) do
    caches = Keyword.get(opts, :caches, [])
    child_opts = Keyword.get(opts, :supervisor, [])
    start_opts = Keyword.drop(opts, [:caches, :supervisor])

    child_spec({caches, child_opts, start_opts})
  end

  @impl true
  def init(caches) do
    caches
    |> Kernel.++(CloudCache.Config.caches())
    |> List.flatten()
    |> Enum.map(fn
      {module, args, opts} ->
        Supervisor.child_spec({module, args}, opts)

      {module, args} ->
        Supervisor.child_spec({module, args}, [])

      module ->
        Supervisor.child_spec(module, [])
    end)
    |> Enum.reduce(MapSet.new(), fn entry, set -> MapSet.put(set, entry) end)
    |> MapSet.to_list()
    |> Supervisor.init(strategy: :one_for_one)
  end

  # Non-Multipart Upload API

  def list_buckets(opts \\ []) do
    adapter(opts).list_buckets(opts)
  end

  def head_object(bucket, object, opts \\ []) do
    adapter(opts).head_object(bucket, object, opts)
  end

  def pre_sign(bucket, object, opts \\ []) do
    adapter(opts).pre_sign(bucket, object, opts)
  end

  def get_object(bucket, object, opts \\ []) do
    adapter(opts).get_object(bucket, object, opts)
  end

  def put_object(bucket, object, body, opts \\ []) do
    adapter(opts).put_object(bucket, object, body, opts)
  end

  def copy_object(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    adapter(opts).copy_object(dest_bucket, dest_object, src_bucket, src_object, opts)
  end

  def list_objects(bucket, opts \\ []) do
    adapter(opts).list_objects(bucket, opts)
  end

  # Multipart Upload API

  def pre_sign_part(bucket, object, upload_id, part_number, opts \\ []) do
    adapter(opts).pre_sign_part(bucket, object, upload_id, part_number, opts)
  end

  def upload_part(bucket, object, upload_id, part_number, body, opts \\ []) do
    adapter(opts).upload_part(bucket, object, upload_id, part_number, body, opts)
  end

  def list_parts(bucket, object, upload_id, opts \\ []) do
    adapter(opts).list_parts(bucket, object, upload_id, opts)
  end

  def complete_multipart_upload(bucket, object, upload_id, parts, opts \\ []) do
    adapter(opts).complete_multipart_upload(bucket, object, upload_id, parts, opts)
  end

  def abort_multipart_upload(bucket, object, upload_id, opts \\ []) do
    adapter(opts).abort_multipart_upload(bucket, object, upload_id, opts)
  end

  def create_multipart_upload(bucket, object, opts \\ []) do
    adapter(opts).create_multipart_upload(bucket, object, opts)
  end

  def copy_object_multipart(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    adapter(opts).copy_object_multipart(dest_bucket, dest_object, src_bucket, src_object, opts)
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
    adapter(opts).copy_parts(
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
    adapter(opts).copy_part(
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
