defmodule CloudCache do
  @moduledoc """
  Documentation for `CloudCache`.

  Getting Started:

  Create a module:

  defmodule MyApp.S3Cache do
    use CloudCache,
      adapter: CloudCache.Adapters.S3,
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
        {mod, _} -> function_exported?(mod, :supervisor_children, 1)
        mod -> function_exported?(mod, :supervisor_children, 1)
      end)
      |> Enum.map(fn
        {mod, child_opts} -> mod.supervisor_children(Keyword.merge(opts, child_opts))
        mod -> mod.supervisor_children(opts)
      end)
      |> List.flatten()

    Supervisor.init(children, strategy: :one_for_one)
  end

  defmacro __using__(opts) do
    quote do
      opts = unquote(opts)

      alias CloudCache.Adapter

      @adapter opts[:adapter]
      @options opts[:options] || []

      def adapter, do: @adapter

      def options, do: @options

      def describe_object(bucket, object, opts) do
        opts = Keyword.merge(@options, opts)

        Adapter.describe_object(@adapter, bucket, object, opts)
      end

      def pre_sign(bucket, object, opts) do
        opts = Keyword.merge(@options, opts)

        Adapter.pre_sign(@adapter, bucket, object, opts)
      end

      def pre_sign_part(bucket, object, upload_id, part_number, opts) do
        opts = Keyword.merge(@options, opts)

        Adapter.pre_sign_part(@adapter, bucket, object, upload_id, part_number, opts)
      end

      def list_parts(bucket, object, upload_id, opts) do
        opts = Keyword.merge(@options, opts)

        Adapter.list_parts(@adapter, bucket, object, upload_id, opts)
      end

      def copy_object_multipart(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
        opts = Keyword.merge(@options, opts)

        Adapter.copy_object_multipart(
          @adapter,
          dest_bucket,
          dest_object,
          src_bucket,
          src_object,
          opts
        )
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
        opts = Keyword.merge(@options, opts)

        Adapter.copy_parts(
          @adapter,
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
            range,
            opts
          ) do
        opts = Keyword.merge(@options, opts)

        Adapter.copy_part(
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

        Adapter.copy_part(
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

        Adapter.complete_multipart_upload(@adapter, bucket, object, upload_id, parts, opts)
      end

      def abort_multipart_upload(bucket, object, upload_id, opts) do
        opts = Keyword.merge(@options, opts)

        Adapter.abort_multipart_upload(@adapter, bucket, object, upload_id, opts)
      end

      def create_multipart_upload(bucket, object, opts) do
        opts = Keyword.merge(@options, opts)

        Adapter.create_multipart_upload(@adapter, bucket, object, opts)
      end
    end
  end
end
