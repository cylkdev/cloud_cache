defmodule CloudCache.Endpoint do
  @type adapter :: module()
  @type bucket :: binary()
  @type object :: binary()
  @type upload_id :: binary()
  @type part_number :: pos_integer()
  @type etag :: binary()
  @type content_length :: pos_integer()
  @type body :: term()
  @type options :: keyword()

  @callback adapter :: adapter()

  @callback options :: options()

  @callback head_object(
              bucket :: bucket(),
              object :: object(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback copy_object(
              dest_bucket :: bucket(),
              dest_object :: object(),
              src_bucket :: bucket(),
              src_object :: object(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback pre_sign(
              bucket :: bucket(),
              object :: object(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback pre_sign_part(
              bucket :: bucket(),
              object :: object(),
              upload_id :: upload_id(),
              part_number :: part_number(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback create_multipart_upload(
              bucket :: bucket(),
              object :: object(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback upload_part(
              bucket :: bucket(),
              object :: object(),
              upload_id :: upload_id(),
              part_number :: part_number(),
              body :: body(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback list_parts(
              bucket :: bucket(),
              object :: object(),
              upload_id :: upload_id(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback complete_multipart_upload(
              bucket :: bucket(),
              object :: object(),
              upload_id :: upload_id(),
              parts :: [{part_number(), etag()}],
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback abort_multipart_upload(
              bucket :: bucket(),
              object :: object(),
              upload_id :: upload_id(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback copy_object_multipart(
              dest_bucket :: bucket(),
              dest_object :: object(),
              src_bucket :: bucket(),
              src_object :: object(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback copy_parts(
              dest_bucket :: bucket(),
              dest_object :: object(),
              src_bucket :: bucket(),
              src_object :: object(),
              upload_id :: upload_id(),
              content_length :: content_length(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback copy_part(
              dest_bucket :: bucket(),
              dest_object :: object(),
              src_bucket :: bucket(),
              src_object :: object(),
              upload_id :: upload_id(),
              part_number :: part_number(),
              src_range :: Range.t(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  def adapter(impl), do: impl.adapter()

  def options(impl), do: impl.options()

  # Non-Multipart Upload API

  def head_object(impl, bucket, object, opts \\ []) do
    impl.head_object(bucket, object, opts)
  end

  def pre_sign(impl, bucket, object, opts \\ []) do
    impl.pre_sign(bucket, object, opts)
  end

  def copy_object(impl, dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    impl.copy_object(dest_bucket, dest_object, src_bucket, src_object, opts)
  end

  # Multipart Upload API

  def pre_sign_part(impl, bucket, object, upload_id, part_number, opts \\ []) do
    impl.pre_sign_part(bucket, object, upload_id, part_number, opts)
  end

  def upload_part(impl, bucket, object, upload_id, part_number, body, opts \\ []) do
    impl.upload_part(bucket, object, upload_id, part_number, body, opts)
  end

  def list_parts(impl, bucket, object, upload_id, opts \\ []) do
    impl.list_parts(bucket, object, upload_id, opts)
  end

  def complete_multipart_upload(impl, bucket, object, upload_id, parts, opts \\ []) do
    impl.complete_multipart_upload(bucket, object, upload_id, parts, opts)
  end

  def abort_multipart_upload(impl, bucket, object, upload_id, opts \\ []) do
    impl.abort_multipart_upload(bucket, object, upload_id, opts)
  end

  def create_multipart_upload(impl, bucket, object, opts \\ []) do
    impl.create_multipart_upload(bucket, object, opts)
  end

  def copy_object_multipart(impl, dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    impl.copy_object_multipart(dest_bucket, dest_object, src_bucket, src_object, opts)
  end

  def copy_parts(
        impl,
        dest_bucket,
        dest_object,
        src_bucket,
        src_object,
        upload_id,
        content_length,
        opts \\ []
      ) do
    impl.copy_parts(
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
        impl,
        dest_bucket,
        dest_object,
        src_bucket,
        src_object,
        upload_id,
        part_number,
        src_range,
        opts \\ []
      ) do
    impl.copy_part(
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

  defmacro __using__(opts) do
    quote do
      opts = unquote(opts)

      alias CloudCache.Adapter

      @behaviour CloudCache.Endpoint

      @adapter Keyword.fetch!(opts, :adapter)
      @options opts[:options] || []

      @impl true
      def adapter, do: @adapter

      @impl true
      def options, do: @options

      @impl true
      def pre_sign(bucket, object, opts) do
        opts = Keyword.merge(@options, opts)

        Adapter.pre_sign(@adapter, bucket, object, opts)
      end

      @impl true
      def pre_sign_part(bucket, object, upload_id, part_number, opts) do
        opts = Keyword.merge(@options, opts)

        Adapter.pre_sign_part(@adapter, bucket, object, upload_id, part_number, opts)
      end

      @impl true
      def copy_object(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
        opts = Keyword.merge(@options, opts)

        Adapter.copy_object(@adapter, dest_bucket, dest_object, src_bucket, src_object, opts)
      end

      @impl true
      def head_object(bucket, object, opts) do
        opts = Keyword.merge(@options, opts)

        Adapter.head_object(@adapter, bucket, object, opts)
      end

      @impl true
      def upload_part(bucket, object, upload_id, part_number, body, opts \\ []) do
        opts = Keyword.merge(@options, opts)

        Adapter.upload_part(@adapter, bucket, object, upload_id, part_number, body, opts)
      end

      @impl true
      def list_parts(bucket, object, upload_id, opts) do
        opts = Keyword.merge(@options, opts)

        Adapter.list_parts(@adapter, bucket, object, upload_id, opts)
      end

      @impl true
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

      @impl true
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

      @impl true
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

      @impl true
      def complete_multipart_upload(bucket, object, upload_id, parts, opts) do
        opts = Keyword.merge(@options, opts)

        Adapter.complete_multipart_upload(@adapter, bucket, object, upload_id, parts, opts)
      end

      @impl true
      def abort_multipart_upload(bucket, object, upload_id, opts) do
        opts = Keyword.merge(@options, opts)

        Adapter.abort_multipart_upload(@adapter, bucket, object, upload_id, opts)
      end

      @impl true
      def create_multipart_upload(bucket, object, opts) do
        opts = Keyword.merge(@options, opts)

        Adapter.create_multipart_upload(@adapter, bucket, object, opts)
      end
    end
  end
end
