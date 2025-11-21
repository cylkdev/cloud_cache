defmodule CloudCache.Adapter do
  @type bucket :: binary()
  @type key :: binary()
  @type upload_id :: binary()
  @type part_number :: pos_integer()
  @type etag :: binary()
  @type content_length :: pos_integer()
  @type body :: term()
  @type options :: keyword()
  @type http_method :: atom()

  @callback pre_sign(
              bucket :: bucket(),
              http_method :: http_method(),
              key :: key(),
              opts :: options()
            ) :: map()

  @callback pre_sign_post(bucket :: bucket(), key :: key(), opts :: options()) :: map()

  @callback pre_sign_part(
              bucket :: bucket(),
              key :: key(),
              upload_id :: upload_id(),
              part_number :: part_number(),
              opts :: options()
            ) :: map()

  @callback list_buckets(opts :: options()) :: {:ok, term()} | {:error, term()}

  @callback create_bucket(bucket :: bucket(), region :: binary(), opts :: options()) ::
              {:ok, term()} | {:error, term()}

  @callback head_object(
              bucket :: bucket(),
              key :: key(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback delete_object(
              bucket :: bucket(),
              key :: key(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback get_object(
              bucket :: bucket(),
              key :: key(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback put_object(
              bucket :: bucket(),
              key :: key(),
              body :: any(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback copy_object(
              dest_bucket :: bucket(),
              dest_key :: key(),
              src_bucket :: bucket(),
              src_key :: key(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback list_objects(
              bucket :: bucket(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback create_multipart_upload(
              bucket :: bucket(),
              key :: key(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback upload_part(
              bucket :: bucket(),
              key :: key(),
              upload_id :: upload_id(),
              part_number :: part_number(),
              body :: body(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback list_parts(
              bucket :: bucket(),
              key :: key(),
              upload_id :: upload_id(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback complete_multipart_upload(
              bucket :: bucket(),
              key :: key(),
              upload_id :: upload_id(),
              parts :: [{part_number(), etag()}],
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback abort_multipart_upload(
              bucket :: bucket(),
              key :: key(),
              upload_id :: upload_id(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback copy_object_multipart(
              dest_bucket :: bucket(),
              dest_key :: key(),
              src_bucket :: bucket(),
              src_key :: key(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback copy_parts(
              dest_bucket :: bucket(),
              dest_key :: key(),
              src_bucket :: bucket(),
              src_key :: key(),
              upload_id :: upload_id(),
              content_length :: content_length(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  @callback copy_part(
              dest_bucket :: bucket(),
              dest_key :: key(),
              src_bucket :: bucket(),
              src_key :: key(),
              upload_id :: upload_id(),
              part_number :: part_number(),
              src_range :: Range.t(),
              opts :: options()
            ) :: {:ok, term()} | {:error, term()}

  def pre_sign(adapter, bucket, http_method, key, opts \\ []) do
    adapter.pre_sign(bucket, http_method, key, opts)
  end

  def pre_sign_post(adapter, bucket, key, opts \\ []) do
    adapter.pre_sign_post(bucket, key, opts)
  end

  def list_buckets(adapter, opts \\ []) do
    adapter.list_buckets(opts)
  end

  def create_bucket(adapter, bucket, region, opts \\ []) do
    adapter.create_bucket(bucket, region, opts)
  end

  def head_object(adapter, bucket, key, opts \\ []) do
    adapter.head_object(bucket, key, opts)
  end

  def delete_object(adapter, bucket, key, opts \\ []) do
    adapter.delete_object(bucket, key, opts)
  end

  def get_object(adapter, bucket, key, opts \\ []) do
    adapter.get_object(bucket, key, opts)
  end

  def put_object(adapter, bucket, key, body, opts \\ []) do
    adapter.put_object(bucket, key, body, opts)
  end

  def copy_object(adapter, dest_bucket, dest_key, src_bucket, src_key, opts \\ []) do
    adapter.copy_object(dest_bucket, dest_key, src_bucket, src_key, opts)
  end

  def list_objects(adapter, bucket, opts \\ []) do
    adapter.list_objects(bucket, opts)
  end

  # Multipart Upload API

  def pre_sign_part(adapter, bucket, key, upload_id, part_number, opts \\ []) do
    adapter.pre_sign_part(bucket, key, upload_id, part_number, opts)
  end

  def upload_part(adapter, bucket, key, upload_id, part_number, body, opts \\ []) do
    adapter.upload_part(bucket, key, upload_id, part_number, body, opts)
  end

  def list_parts(adapter, bucket, key, upload_id, opts \\ []) do
    adapter.list_parts(bucket, key, upload_id, opts)
  end

  def complete_multipart_upload(adapter, bucket, key, upload_id, parts, opts \\ []) do
    adapter.complete_multipart_upload(bucket, key, upload_id, parts, opts)
  end

  def abort_multipart_upload(adapter, bucket, key, upload_id, opts \\ []) do
    adapter.abort_multipart_upload(bucket, key, upload_id, opts)
  end

  def create_multipart_upload(adapter, bucket, key, opts \\ []) do
    adapter.create_multipart_upload(bucket, key, opts)
  end

  def copy_object_multipart(adapter, dest_bucket, dest_key, src_bucket, src_key, opts \\ []) do
    adapter.copy_object_multipart(dest_bucket, dest_key, src_bucket, src_key, opts)
  end

  def copy_parts(
        adapter,
        dest_bucket,
        dest_key,
        src_bucket,
        src_key,
        upload_id,
        content_length,
        opts \\ []
      ) do
    adapter.copy_parts(
      dest_bucket,
      dest_key,
      src_bucket,
      src_key,
      upload_id,
      content_length,
      opts
    )
  end

  def copy_part(
        adapter,
        dest_bucket,
        dest_key,
        src_bucket,
        src_key,
        upload_id,
        part_number,
        src_range,
        opts \\ []
      ) do
    adapter.copy_part(
      dest_bucket,
      dest_key,
      src_bucket,
      src_key,
      upload_id,
      part_number,
      src_range,
      opts
    )
  end
end
