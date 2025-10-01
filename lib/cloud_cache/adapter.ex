defmodule CloudCache.Adapter do
  @type bucket :: binary()
  @type object :: binary()
  @type upload_id :: binary()
  @type part_number :: pos_integer()
  @type etag :: binary()
  @type content_length :: pos_integer()
  @type body :: term()
  @type options :: keyword()

  @callback supervisor_children(opts :: options()) :: list()

  @callback list_buckets(opts :: options()) :: {:ok, term()} | {:error, term()}

  @callback head_object(
              bucket :: bucket(),
              object :: object(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback get_object(
              bucket :: bucket(),
              object :: object(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback put_object(
              bucket :: bucket(),
              object :: object(),
              body :: any(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback copy_object(
              dest_bucket :: bucket(),
              dest_object :: object(),
              src_bucket :: bucket(),
              src_object :: object(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback list_objects(
              bucket :: bucket(),
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

  @callback list_multipart_uploads(
              bucket :: bucket(),
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

  @optional_callbacks supervisor_children: 1

  # Non-Multipart Upload API

  def head_object(adapter, bucket, object, opts \\ []) do
    adapter.head_object(bucket, object, opts)
  end

  def pre_sign(adapter, bucket, object, opts \\ []) do
    adapter.pre_sign(bucket, object, opts)
  end

  def get_object(adapter, bucket, object, body, opts \\ []) do
    adapter.get_object(bucket, object, body, opts)
  end

  def put_object(adapter, bucket, object, body, opts \\ []) do
    adapter.put_object(bucket, object, body, opts)
  end

  def copy_object(adapter, dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    adapter.copy_object(dest_bucket, dest_object, src_bucket, src_object, opts)
  end

  def list_objects(adapter, bucket, opts \\ []) do
    adapter.list_objects(bucket, opts)
  end

  # Multipart Upload API

  def pre_sign_part(adapter, bucket, object, upload_id, part_number, opts \\ []) do
    adapter.pre_sign_part(bucket, object, upload_id, part_number, opts)
  end

  def upload_part(adapter, bucket, object, upload_id, part_number, body, opts \\ []) do
    adapter.upload_part(bucket, object, upload_id, part_number, body, opts)
  end

  def list_parts(adapter, bucket, object, upload_id, opts \\ []) do
    adapter.list_parts(bucket, object, upload_id, opts)
  end

  def list_multipart_uploads(adapter, bucket, opts \\ []) do
    adapter.list_multipart_uploads(bucket, opts)
  end

  def complete_multipart_upload(adapter, bucket, object, upload_id, parts, opts \\ []) do
    adapter.complete_multipart_upload(bucket, object, upload_id, parts, opts)
  end

  def abort_multipart_upload(adapter, bucket, object, upload_id, opts \\ []) do
    adapter.abort_multipart_upload(bucket, object, upload_id, opts)
  end

  def create_multipart_upload(adapter, bucket, object, opts \\ []) do
    adapter.create_multipart_upload(bucket, object, opts)
  end

  def copy_object_multipart(adapter, dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    adapter.copy_object_multipart(dest_bucket, dest_object, src_bucket, src_object, opts)
  end

  def copy_parts(
        adapter,
        dest_bucket,
        dest_object,
        src_bucket,
        src_object,
        upload_id,
        content_length,
        opts \\ []
      ) do
    adapter.copy_parts(
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
        adapter,
        dest_bucket,
        dest_object,
        src_bucket,
        src_object,
        upload_id,
        part_number,
        src_range,
        opts \\ []
      ) do
    adapter.copy_part(
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
end
