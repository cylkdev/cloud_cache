defmodule CloudCache do
  @moduledoc """
  Documentation for `CloudCache`.

  Getting Started:

  Create a module:

  defmodule MyApp.S3Cache do
    use CloudCache,
      adapter: CloudCache.S3,
      options: []
  end

  CloudCache.start_link(caches: [MyApp.S3Cache])
  """
  use Supervisor

  @supervisor_option_keys [:name, :strategy, :max_restarts, :max_seconds]

  @doc """
  Starts the supervisor and its children.
  """
  def start_link(opts \\ []) do
    supervisor_opts =
      opts
      |> Keyword.take(@supervisor_option_keys)
      |> Keyword.put_new(:name, __MODULE__)

    init_opts = Keyword.drop(opts, @supervisor_option_keys)

    Supervisor.start_link(__MODULE__, init_opts, supervisor_opts)
  end

  @impl true
  def init(opts) do
    children =
      opts
      |> Keyword.get(:caches, [])
      |> Enum.filter(fn
        {mod, _} -> function_exported?(mod, :child_spec, 1)
        mod -> function_exported?(mod, :child_spec, 1)
      end)

    # one_for_one means if a child dies, only that child is restarted
    Supervisor.init(children, strategy: :one_for_one)
  end

  # Cache API

  def describe_object(adapter, bucket, object, opts) do
    adapter.describe_object(bucket, object, opts)
  end

  def pre_sign(adapter, bucket, object, opts) do
    adapter.pre_sign(bucket, object, opts)
  end

  def list_parts(adapter, bucket, object, upload_id, opts) do
    adapter.list_parts(bucket, object, upload_id, opts)
  end

  def pre_sign_part(adapter, bucket, object, upload_id, part_number, opts) do
    adapter.pre_sign_part(bucket, object, upload_id, part_number, opts)
  end

  def copy_part(
        adapter,
        dest_bucket,
        dest_object,
        src_bucket,
        src_object,
        upload_id,
        part_number,
        range,
        opts
      ) do
    adapter.copy_part(
      dest_bucket,
      dest_object,
      src_bucket,
      src_object,
      upload_id,
      part_number,
      range,
      opts
    )
  end

  def complete_multipart_upload(adapter, bucket, object, upload_id, parts, opts) do
    adapter.complete_multipart_upload(bucket, object, upload_id, parts, opts)
  end

  def abort_multipart_upload(adapter, bucket, object, upload_id, opts) do
    adapter.abort_multipart_upload(bucket, object, upload_id, opts)
  end

  def create_multipart_upload(adapter, bucket, object, opts) do
    adapter.create_multipart_upload(bucket, object, opts)
  end

  defmacro __using__(opts) do
    quote do
      opts = unquote(opts)

      @adapter opts[:adapter]
      @options opts[:options] || []

      def adapter, do: @adapter

      def options, do: @options

      def describe_object(bucket, object, opts) do
        opts = Keyword.merge(@options, opts)

        CloudCache.describe_object(@adapter, bucket, object, opts)
      end

      def pre_sign(bucket, object, opts) do
        opts = Keyword.merge(@options, opts)

        CloudCache.pre_sign(@adapter, bucket, object, opts)
      end

      def list_parts(bucket, object, upload_id, opts) do
        opts = Keyword.merge(@options, opts)

        CloudCache.list_parts(@adapter, bucket, object, upload_id, opts)
      end

      def pre_sign_part(bucket, object, upload_id, part_number, opts) do
        opts = Keyword.merge(@options, opts)

        CloudCache.pre_sign_part(@adapter, bucket, object, upload_id, part_number, opts)
      end

      def copy_part(
            dest_bucket,
            dest_object,
            src_bucket,
            src_object,
            upload_id,
            part_number,
            range,
            opts
          ) do
        opts = Keyword.merge(@options, opts)

        CloudCache.copy_part(
          @adapter,
          dest_bucket,
          dest_object,
          src_bucket,
          src_object,
          upload_id,
          part_number,
          range,
          opts
        )
      end

      def complete_multipart_upload(bucket, object, upload_id, parts, opts) do
        opts = Keyword.merge(@options, opts)

        CloudCache.complete_multipart_upload(@adapter, bucket, object, upload_id, parts, opts)
      end

      def abort_multipart_upload(bucket, object, upload_id, opts) do
        opts = Keyword.merge(@options, opts)

        CloudCache.abort_multipart_upload(@adapter, bucket, object, upload_id, opts)
      end

      def create_multipart_upload(bucket, object, opts) do
        opts = Keyword.merge(@options, opts)

        CloudCache.create_multipart_upload(@adapter, bucket, object, opts)
      end
    end
  end
end
