defmodule CloudCache.Adapters.S3.Testing.S3SandboxTest do
  use ExUnit.Case, async: true
  alias CloudCache.Adapters.S3
  alias CloudCache.Adapters.S3.Testing.S3Sandbox

  @non_existent_bucket "non-existent-bucket"
  @non_existent_object "non-existent-object"
  @bucket "test-bucket"
  @object "test-object"
  @options [s3: [sandbox_enabled: true]]

  describe "head_object/3" do
    test "returns object metadata on success" do
      S3Sandbox.set_head_object_responses([
        {@bucket,
         fn ->
           {:ok,
            %{
              content_length: 123,
              content_type: "image/png",
              etag: "abcdef1234567890",
              last_modified: "Tue, 01 Jan 2021 00:00:00 GMT"
            }}
         end}
      ])

      assert {:ok,
              %{
                content_length: 123,
                content_type: "image/png",
                etag: "abcdef1234567890",
                last_modified: "Tue, 01 Jan 2021 00:00:00 GMT"
              }} = S3.head_object(@bucket, @object, @options)
    end

    test "returns not_found error if object does not exist" do
      S3Sandbox.set_head_object_responses([
        {@bucket,
         fn ->
           {:error,
            %ErrorMessage{
              code: :not_found,
              message: "object not found",
              details: %{
                bucket: @bucket,
                object: "nonexistent.txt"
              }
            }}
         end}
      ])

      assert {:error,
              %ErrorMessage{
                code: :not_found,
                message: "object not found",
                details: %{
                  bucket: @bucket,
                  object: "nonexistent.txt"
                }
              }} = S3.head_object(@bucket, "nonexistent.txt", @options)
    end
  end

  describe "put_object/4" do
    test "successfully puts an object in the sandbox" do
      S3Sandbox.set_put_object_responses([
        {~r|.*|, fn -> {:ok, %{
          content_length: "0",
          date: "Fri, 19 Sep 2025 18:40:13 GMT",
          etag: "9725d5a30c6130db8e169c4d9560ded7",
          server: "TwistedWeb/24.3.0",
          x_amz_checksum_crc64nvme: "GMhJTU/CB1I=",
          x_amz_checksum_type: "FULL_OBJECT",
          x_amz_id_2:
            "s9lzHYrFp76ZVxRcpX9+5cjAnEH2ROuNkd2BHfIa6UkFVdtjf5mKR3/eTPFvsiP/XV/VLi31234=",
          x_amz_request_id: "b255ad52-f548-4bb5-ab97-6dd2e5982a8d",
          x_amz_server_side_encryption: "AES256",
          x_localstack: "true"
        }} end}
      ])

      assert {:ok,
              %{
                content_length: "0",
                date: "Fri, 19 Sep 2025 18:40:13 GMT",
                etag: "9725d5a30c6130db8e169c4d9560ded7",
                server: "TwistedWeb/24.3.0",
                x_amz_checksum_crc64nvme: "GMhJTU/CB1I=",
                x_amz_checksum_type: "FULL_OBJECT",
                x_amz_id_2:
                  "s9lzHYrFp76ZVxRcpX9+5cjAnEH2ROuNkd2BHfIa6UkFVdtjf5mKR3/eTPFvsiP/XV/VLi31234=",
                x_amz_request_id: "b255ad52-f548-4bb5-ab97-6dd2e5982a8d",
                x_amz_server_side_encryption: "AES256",
                x_localstack: "true"
              }} = S3.put_object(@bucket, @object, "test-content", @options)
    end

    test "returns an error when bucket is not found" do
      S3Sandbox.set_put_object_responses([
        {~r|.*|, fn -> {:error, %{message: "bucket not found"}} end}
      ])

      assert {:error, %{message: "bucket not found"}} =
               S3.put_object(@non_existent_bucket, @object, "test-content", @options)
    end
  end

  describe "list_objects/2" do
    test "returns list of objects on success" do
      S3Sandbox.set_list_objects_responses([
        {@bucket,
         fn ->
           {:ok,
            [
              %{
                key: "test-object",
                last_modified: ~U[2025-08-30 01:00:00.000000Z],
                etag: "etag"
              }
            ]}
         end}
      ])

      assert {:ok,
              [
                %{
                  key: "test-object",
                  last_modified: ~U[2025-08-30 01:00:00.000000Z],
                  etag: "etag"
                }
              ]} = S3.list_objects(@bucket, @options)
    end
  end

  describe "copy_object/3" do
    test "returns object metadata on success" do
      S3Sandbox.set_copy_object_responses([
        {@bucket,
         fn ->
           {:ok,
            %{
              last_modified: ~U[2025-08-30 01:00:00.000000Z],
              etag: "etag"
            }}
         end}
      ])

      assert {:ok,
              %{
                last_modified: ~U[2025-08-30 01:00:00.000000Z],
                etag: "etag"
              }} =
               S3.copy_object(@bucket, @object, @bucket, @object, @options)
    end

    test "returns not_found error if object does not exist" do
      S3Sandbox.set_copy_object_responses([
        {@bucket,
         fn ->
           {:error,
            %ErrorMessage{
              code: :not_found,
              message: "object not found",
              details: %{
                dest_bucket: @bucket,
                dest_object: @object,
                src_bucket: @bucket,
                src_object: @non_existent_object
              }
            }}
         end}
      ])

      assert {:error,
              %ErrorMessage{
                code: :not_found,
                message: "object not found",
                details: %{
                  dest_bucket: @bucket,
                  dest_object: @object,
                  src_bucket: @bucket,
                  src_object: @non_existent_object
                }
              }} =
               S3.copy_object(@bucket, @object, @bucket, @non_existent_object, @options)
    end
  end

  describe "pre_sign/3" do
    test "returns a presigned URL and metadata on success" do
      S3Sandbox.set_pre_sign_responses([
        {@bucket,
         fn object ->
           {:ok,
            %{
              key: object,
              url: "https://example.com/#{object}?signature=fake-signature",
              expires_in: 60,
              expires_at: ~U[2025-08-30 01:00:00.000000Z]
            }}
         end}
      ])

      assert {:ok,
              %{
                key: @object,
                url: "https://example.com/test-object?signature=fake-signature",
                expires_in: 60,
                expires_at: ~U[2025-08-30 01:00:00.000000Z]
              }} = S3.pre_sign(@bucket, @object, @options)
    end
  end

  describe "list_parts/4" do
    test "returns list of parts and count on success" do
      S3Sandbox.set_list_parts_responses([
        {@bucket,
         fn ->
           {:ok, [%{part_number: 1, size: 5_247_794, etag: "etag_123"}]}
         end}
      ])

      assert {:ok, [%{part_number: 1, size: 5_247_794, etag: "etag_123"}]} =
               S3.list_parts(@bucket, @object, "upload_id_123", @options)
    end

    test "returns not_found error if object or upload ID is invalid" do
      S3Sandbox.set_list_parts_responses([
        {@bucket,
         fn ->
           {:error,
            %ErrorMessage{
              code: :not_found,
              message: "object not found",
              details: %{
                bucket: @bucket,
                object: @object,
                upload_id: "upload_id"
              }
            }}
         end}
      ])

      assert {:error,
              %ErrorMessage{
                code: :not_found,
                message: "object not found",
                details: %{
                  bucket: @bucket,
                  object: @object,
                  upload_id: "upload_id"
                }
              }} =
               S3.list_parts(@bucket, @object, "upload_id", @options)
    end
  end

  describe "pre_sign_part/5" do
    test "returns a presigned URL for the given part on success" do
      S3Sandbox.set_pre_sign_part_responses([
        {@bucket,
         fn _object, _upload_id, part_number ->
           {:ok,
            %{
              key: @object,
              url: "https://example.com/test-object?partNumber=#{part_number}&signature=fake",
              expires_in: 120,
              expires_at: ~U[2025-08-30 01:00:00.000000Z]
            }}
         end}
      ])

      assert {:ok,
              %{
                key: @object,
                url: "https://example.com/test-object?partNumber=1&signature=fake",
                expires_in: 120,
                expires_at: ~U[2025-08-30 01:00:00.000000Z]
              }} = S3.pre_sign_part(@bucket, @object, "upload_id_123", 1, @options)
    end
  end

  describe "upload_part/6" do
    test "returns :ok on successful part copy" do
      S3Sandbox.set_upload_part_responses([
        {@bucket,
         fn ->
           {:ok,
            %{
              content_length: 0,
              etag: "etag"
            }}
         end}
      ])

      assert {:ok,
              %{
                content_length: 0,
                etag: "etag"
              }} =
               S3.upload_part(
                 @bucket,
                 @object,
                 "upload_id",
                 1,
                 "content",
                 @options
               )
    end

    test "returns service_unavailable error on failure to copy part" do
      S3Sandbox.set_upload_part_responses([
        {@bucket,
         fn ->
           {:error,
            %ErrorMessage{
              code: :not_found,
              message: "object not found",
              details: %{
                bucket: @bucket,
                object: @non_existent_object,
                upload_id: "upload_id",
                part_number: 1
              }
            }}
         end}
      ])

      assert {:error,
              %ErrorMessage{
                code: :not_found,
                message: "object not found"
              }} =
               S3.upload_part(
                 @bucket,
                 @non_existent_object,
                 "upload_id",
                 1,
                 "content",
                 @options
               )
    end
  end

  describe "copy_parts/8" do
    test "returns :ok on successful part copy" do
      S3Sandbox.set_copy_parts_responses([
        {@bucket,
         fn ->
           {:ok, [{1, "etag_1"}, {2, "etag_2"}]}
         end}
      ])

      assert {:ok, [{1, "etag_1"}, {2, "etag_2"}]} =
               S3.copy_parts(
                 @bucket,
                 @object,
                 @bucket,
                 @object,
                 "upload_id",
                 123,
                 @options
               )
    end

    test "returns service_unavailable error on failure to copy part" do
      S3Sandbox.set_copy_parts_responses([
        {@bucket,
         fn ->
           {:error,
            %ErrorMessage{code: :service_unavailable, message: "service temporarily unavailable"}}
         end}
      ])

      assert {:error,
              %ErrorMessage{
                code: :service_unavailable,
                message: "service temporarily unavailable"
              }} =
               S3.copy_parts(
                 @bucket,
                 @object,
                 @bucket,
                 @object,
                 "upload_id",
                 123,
                 @options
               )
    end
  end

  describe "copy_part/8" do
    test "returns :ok on successful part copy" do
      S3Sandbox.set_copy_part_responses([
        {@bucket,
         fn ->
           {:ok,
            %{
              last_modified: ~U[2025-08-30 01:00:00.000000Z],
              etag: "etag_123"
            }}
         end}
      ])

      assert {:ok,
              %{
                last_modified: ~U[2025-08-30 01:00:00.000000Z],
                etag: "etag_123"
              }} =
               S3.copy_part(
                 @bucket,
                 "dest.txt",
                 @bucket,
                 @object,
                 "upload_id_123",
                 1,
                 "bytes=0-99",
                 @options
               )
    end

    test "returns service_unavailable error on failure to copy part" do
      S3Sandbox.set_copy_part_responses([
        {@bucket,
         fn ->
           {:error,
            %ErrorMessage{code: :service_unavailable, message: "service temporarily unavailable"}}
         end}
      ])

      assert {:error,
              %ErrorMessage{
                code: :service_unavailable,
                message: "service temporarily unavailable"
              }} =
               S3.copy_part(
                 @bucket,
                 "dest.txt",
                 @bucket,
                 @object,
                 "upload_id_123",
                 2,
                 0..99,
                 @options
               )
    end
  end

  describe "create_multipart_upload/3" do
    test "returns upload information on success" do
      S3Sandbox.set_create_multipart_upload_responses([
        {@bucket,
         fn ->
           {:ok,
            %{
              bucket: @bucket,
              key: @object,
              upload_id: "upload_id_123"
            }}
         end}
      ])

      assert {:ok, %{bucket: @bucket, key: @object, upload_id: "upload_id_123"}} =
               S3.create_multipart_upload(@bucket, @object, @options)
    end

    test "returns service_unavailable error on failure to initiate upload" do
      S3Sandbox.set_create_multipart_upload_responses([
        {@bucket,
         fn ->
           {:error,
            %ErrorMessage{
              code: :service_unavailable,
              message: "service temporarily unavailable"
            }}
         end}
      ])

      assert {:error,
              %ErrorMessage{
                code: :service_unavailable,
                message: "service temporarily unavailable"
              }} =
               S3.create_multipart_upload(@bucket, @object, @options)
    end
  end

  describe "complete_multipart_upload/5" do
    test "returns file metadata on successful multipart upload completion" do
      S3Sandbox.set_complete_multipart_upload_responses([
        {@bucket,
         fn ->
           {:ok,
            %{
              last_modified: "Fri, 01 Jan 2021 00:00:00 GMT",
              content_length: 1000,
              etag: "final-etag",
              content_type: "binary/octet-stream"
            }}
         end}
      ])

      assert {:ok,
              %{
                last_modified: "Fri, 01 Jan 2021 00:00:00 GMT",
                content_length: 1000,
                etag: "final-etag",
                content_type: "binary/octet-stream"
              }} =
               S3.complete_multipart_upload(
                 @bucket,
                 @object,
                 "upload_id_123",
                 {"1", "etag_123"},
                 @options
               )
    end

    test "returns not_found error if the multipart upload is not found" do
      S3Sandbox.set_complete_multipart_upload_responses([
        {@bucket,
         fn ->
           {:error,
            %ErrorMessage{
              code: :service_unavailable,
              message: "service temporarily unavailable"
            }}
         end}
      ])

      assert {:error,
              %ErrorMessage{
                code: :service_unavailable,
                message: "service temporarily unavailable"
              }} =
               S3.complete_multipart_upload(
                 @bucket,
                 @object,
                 "bad_upload_id",
                 [{1, "etag"}],
                 @options
               )
    end
  end

  describe "abort_multipart_upload/4" do
    test "returns OK tuple on successful abort" do
      S3Sandbox.set_abort_multipart_upload_responses([
        {@bucket,
         fn ->
           {:ok,
            %{
              status_code: 204,
              body: "",
              headers: [
                {"x-amz-id-2", "some-opaque-id"},
                {"x-amz-request-id", "req-id-12345"},
                {"date", "Fri, 18 Aug 2023 10:32:49 GMT"},
                {"server", "AmazonS3"}
              ]
            }}
         end}
      ])

      assert {:ok,
              %{
                status_code: 204,
                body: "",
                headers: [
                  {"x-amz-id-2", "some-opaque-id"},
                  {"x-amz-request-id", "req-id-12345"},
                  {"date", "Fri, 18 Aug 2023 10:32:49 GMT"},
                  {"server", "AmazonS3"}
                ]
              }} =
               S3.abort_multipart_upload(@bucket, @object, "upload_id_123", @options)
    end

    test "returns service_unavailable error on failure to abort" do
      S3Sandbox.set_abort_multipart_upload_responses([
        {@bucket,
         fn ->
           {:error,
            %ErrorMessage{
              code: :service_unavailable,
              message: "service temporarily unavailable"
            }}
         end}
      ])

      assert {:error,
              %ErrorMessage{
                code: :service_unavailable,
                message: "service temporarily unavailable"
              }} =
               S3.abort_multipart_upload(@bucket, @object, "upload_id_123", @options)
    end
  end
end
