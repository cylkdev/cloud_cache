defmodule CloudCache.Adapters.S3Test do
  use ExUnit.Case, async: true
  alias CloudCache.Adapters.S3
  alias CloudCache.Adapters.S3.Testing

  @bucket "test-bucket"
  @local_stack_opts [s3: [sandbox_enabled: false, local_stack: true]]

  describe "list_buckets/3" do
    test "returns all buckets" do
      assert {:ok, buckets} = S3.list_buckets(@local_stack_opts)
      assert Enum.any?(buckets, fn bucket -> bucket.name === @bucket end)
    end
  end

  describe "head_object/3" do
    test "returns object metadata on success" do
      dest_object = "test_#{:erlang.unique_integer()}.txt"

      assert {:ok, _} = S3.put_object(@bucket, dest_object, "content", @local_stack_opts)

      assert {:ok,
              %{
                content_length: content_length,
                content_type: content_type,
                etag: etag,
                last_modified: last_modified
              }} = S3.head_object(@bucket, dest_object, @local_stack_opts)

      assert content_length >= 0
      assert content_type
      assert is_binary(etag)
      assert last_modified
    end

    test "returns not_found error if object does not exist" do
      assert {:error,
              %ErrorMessage{
                code: :not_found,
                message: "object not found",
                details: %{
                  bucket: @bucket,
                  object: "nonexistent-object"
                }
              }} =
               S3.head_object(@bucket, "nonexistent-object", @local_stack_opts)
    end
  end

  describe "get_object/3" do
    test "can download object" do
      dest_object = "test_#{:erlang.unique_integer()}.txt"
      assert {:ok, _} = S3.put_object(@bucket, dest_object, "content", @local_stack_opts)
      assert {:ok, "content"} = S3.get_object(@bucket, dest_object, @local_stack_opts)
    end

    test "can download object range" do
      dest_object = "test_#{:erlang.unique_integer()}.txt"
      assert {:ok, _} = S3.put_object(@bucket, dest_object, "content", @local_stack_opts)

      start_byte = 0
      end_byte = 2

      assert {:ok, "con"} =
               S3.get_object(
                 @bucket,
                 dest_object,
                 Keyword.put(@local_stack_opts, :range, "bytes=#{start_byte}-#{end_byte}")
               )
    end
  end

  describe "put_object/4" do
    test "adds object to store" do
      dest_object = "test_#{:erlang.unique_integer()}.txt"

      assert {:ok,
              %{
                # 0,
                content_length: _,
                # "9a0364b9e99bb480dd25e1f0284c8555",
                etag: _,
                # ~U[2025-09-30 19:23:00Z],
                date: _,
                # "TwistedWeb/24.3.0",
                server: _,
                # "lhVdXyVQTGE=",
                x_amz_checksum_crc64nvme: _,
                # "FULL_OBJECT",
                x_amz_checksum_type: _,
                # "s9lzHYrFp76ZVxRcpX9+5cjAnEH2ROuNkd2BHfIa6UkFVdtjf5mKR3/eTPFvsiP/XV/VLi31234=",
                x_amz_id_2: _,
                # "52ba8484-331c-48ed-8e39-43e370a7d962",
                x_amz_request_id: _,
                # "AES256"
                x_amz_server_side_encryption: _
              }} = S3.put_object(@bucket, dest_object, "content", @local_stack_opts)
    end
  end

  describe "copy_object/3" do
    test "returns object metadata on success" do
      src_object = "test_#{:erlang.unique_integer()}.txt"
      assert {:ok, _} = S3.put_object(@bucket, src_object, "content", @local_stack_opts)

      assert {:ok, _xml} =
               S3.copy_object(
                 @bucket,
                 "dest_#{src_object}",
                 @bucket,
                 src_object,
                 @local_stack_opts
               )
    end

    test "returns not_found error if object does not exist" do
      assert {:error,
              %ErrorMessage{
                code: :not_found,
                message: "object not found",
                details: %{
                  dest_bucket: @bucket,
                  dest_object: "test-object",
                  src_bucket: @bucket,
                  src_object: "nonexistent-object"
                }
              }} =
               S3.copy_object(
                 @bucket,
                 "test-object",
                 @bucket,
                 "nonexistent-object",
                 @local_stack_opts
               )
    end
  end

  describe "list_objects/2" do
    test "returns list of objects on success" do
      src_object = "test_#{:erlang.unique_integer()}.txt"

      assert {:ok, _} = S3.put_object(@bucket, src_object, "content", @local_stack_opts)

      assert {:ok, contents} = S3.list_objects(@bucket, @local_stack_opts)
      assert Enum.any?(contents, fn content -> content.key === src_object end)
    end
  end

  describe "presign/3" do
    test "returns a presigned URL and metadata on success" do
      assert %{
               key: "test-object",
               url: url,
               expires_in: 60,
               expires_at: %DateTime{}
             } = S3.presign_url(@bucket, :post, "test-object", @local_stack_opts)

      assert String.contains?(url, "test-bucket/test-object")
    end
  end

  describe "list_parts/4" do
    test "returns list of parts and count on success" do
      key = "test-object.txt"

      assert {:ok, %{upload_id: upload_id}} =
               S3.create_multipart_upload(@bucket, key, @local_stack_opts)

      content = (1_024 * 5) |> :crypto.strong_rand_bytes() |> Base.encode32(padding: false)

      assert {:ok, _} = S3.upload_part(@bucket, key, upload_id, 1, content, @local_stack_opts)

      assert {:ok, [%{part_number: 1, size: size, etag: etag}]} =
               S3.list_parts(@bucket, key, upload_id, @local_stack_opts)

      assert size >= 0
      assert is_binary(etag)
    end

    test "returns not_found error if object or upload ID is invalid" do
      assert {:error,
              %ErrorMessage{
                code: :not_found,
                message: "object not found",
                details: %{
                  bucket: @bucket,
                  object: "nonexistent-object",
                  upload_id: "nonexistent_upload_id"
                }
              }} =
               S3.list_parts(
                 @bucket,
                 "nonexistent-object",
                 "nonexistent_upload_id",
                 @local_stack_opts
               )
    end
  end

  describe "copy_object_multipart/5" do
    test "returns {:ok, payload} on success" do
      dest_object = "dest-test-object"

      src_object = "src-test-object"

      content = (100 * 1024 * 1024) |> :crypto.strong_rand_bytes() |> Base.encode64()

      assert {:ok, _} = S3.put_object(@bucket, src_object, content, @local_stack_opts)

      assert {:ok,
              %{
                location: _,
                key: _,
                bucket: _,
                etag: _
              }} =
               S3.copy_object_multipart(
                 @bucket,
                 dest_object,
                 @bucket,
                 src_object,
                 @local_stack_opts
               )
    end
  end

  describe "upload_part/6" do
    test "returns :ok on successful part copy" do
      dest_object = "test-object.txt"

      assert {:ok, %{upload_id: upload_id}} =
               S3.create_multipart_upload(@bucket, dest_object, @local_stack_opts)

      content = (1_024 * 5) |> :crypto.strong_rand_bytes() |> Base.encode32(padding: false)

      assert {:ok,
              %{
                content_length: _,
                etag: etag
              }} =
               S3.upload_part(
                 @bucket,
                 dest_object,
                 upload_id,
                 1,
                 content,
                 @local_stack_opts
               )

      assert is_binary(etag)
    end

    test "returns service_unavailable error on failure to copy part" do
      assert {:error,
              %ErrorMessage{
                code: :not_found,
                message: "object not found"
              }} =
               S3.upload_part(
                 @bucket,
                 "nonexistent-object",
                 "nonexistent_upload_id",
                 1,
                 "content",
                 @local_stack_opts
               )
    end
  end

  describe "copy_part/8" do
    test "returns :ok on successful part copy" do
      dest_object = "test-object.txt"
      src_object = "test-object.txt"

      content = (1_024 * 5) |> :crypto.strong_rand_bytes() |> Base.encode32(padding: false)

      content_byte_size = byte_size(content)

      assert {:ok, _} = S3.put_object(@bucket, src_object, content, @local_stack_opts)

      assert {:ok, %{upload_id: upload_id}} =
               S3.create_multipart_upload(@bucket, dest_object, @local_stack_opts)

      assert {:ok,
              %{
                last_modified: %DateTime{},
                etag: etag
              }} =
               S3.copy_part(
                 @bucket,
                 dest_object,
                 @bucket,
                 src_object,
                 upload_id,
                 1,
                 0..(content_byte_size - 1),
                 @local_stack_opts
               )

      assert is_binary(etag)
    end

    test "returns service_unavailable error on failure to copy part" do
      assert {:error,
              %ErrorMessage{
                code: :service_unavailable,
                message: "service temporarily unavailable"
              }} =
               S3.copy_part(
                 @bucket,
                 "nonexistent-object",
                 @bucket,
                 "nonexistent-object",
                 "nonexistent_upload_id",
                 1,
                 0..99,
                 @local_stack_opts
               )
    end
  end

  describe "create_multipart_upload/3" do
    test "returns upload information on success" do
      assert {:ok, %{bucket: @bucket, key: "test-object", upload_id: upload_id}} =
               S3.create_multipart_upload(@bucket, "test-object", @local_stack_opts)

      assert is_binary(upload_id)
    end

    test "returns service_unavailable error on failure to initiate upload" do
      assert {:error,
              %ErrorMessage{
                code: :service_unavailable,
                message: "service temporarily unavailable",
                details: %{
                  bucket: "nonexistent-bucket",
                  object: "test-object"
                }
              }} =
               S3.create_multipart_upload("nonexistent-bucket", "test-object", @local_stack_opts)
    end
  end

  describe "complete_multipart_upload/5" do
    test "returns file metadata on successful multipart upload completion" do
      dest_object = "test-object"

      assert {:ok, %{upload_id: upload_id}} =
               S3.create_multipart_upload(@bucket, dest_object, @local_stack_opts)

      content = (1_024 * 5) |> :crypto.strong_rand_bytes() |> Base.encode32(padding: false)

      assert {:ok, %{etag: etag}} =
               S3.upload_part(
                 @bucket,
                 dest_object,
                 upload_id,
                 1,
                 content,
                 @local_stack_opts
               )

      assert {:ok,
              %{
                etag: etag,
                bucket: @bucket,
                key: key,
                location: location
              }} =
               S3.complete_multipart_upload(
                 @bucket,
                 dest_object,
                 upload_id,
                 [{1, etag}],
                 @local_stack_opts
               )

      assert key === dest_object
      assert is_binary(etag)
      assert is_binary(location)
    end

    test "returns not_found error if the multipart upload is not found" do
      assert {:error,
              %ErrorMessage{
                code: :not_found,
                details: %{
                  bucket: @bucket,
                  object: "nonexistent-object",
                  upload_id: "upload_id"
                },
                message: "multipart upload not found."
              }} =
               S3.complete_multipart_upload(
                 @bucket,
                 "nonexistent-object",
                 "upload_id",
                 [{1, "etag"}],
                 @local_stack_opts
               )
    end
  end

  describe "abort_multipart_upload/4" do
    test "returns OK tuple on successful abort" do
      key = "test-object"

      assert {:ok, %{upload_id: upload_id}} =
               S3.create_multipart_upload(@bucket, key, @local_stack_opts)

      assert {:ok,
              %{
                status_code: 204,
                body: "",
                headers: %{
                  date: date,
                  server: server,
                  x_amz_id_2: x_amz_id_2,
                  x_amz_request_id: x_amz_request_id
                }
              }} = S3.abort_multipart_upload(@bucket, key, upload_id, @local_stack_opts)

      assert %DateTime{} = date
      assert is_binary(server)
      assert is_binary(x_amz_id_2)
      assert is_binary(x_amz_request_id)
    end

    test "returns service_unavailable error on failure to abort" do
      assert {:error,
              %ErrorMessage{
                code: :service_unavailable,
                message: "service temporarily unavailable"
              }} =
               S3.abort_multipart_upload(
                 @bucket,
                 "nonexistent-object",
                 "upload_id",
                 @local_stack_opts
               )
    end
  end
end
