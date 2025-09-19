if Mix.env() === :test do
  defmodule CloudCache.Adapters.S3.Testing.Local do
    @moduledoc false

    @test "test"
    @default_options [
      access_key_id: @test,
      secret_access_key: @test,
      scheme: "http://",
      host: "s3.localhost.localstack.cloud",
      port: 4566,
      retries: [max_attempts: 1]
    ]

    def config(opts \\ []) do
      ExAws.Config.new(:s3, Keyword.merge(@default_options, opts))
    end

    def head_or_create_bucket(region, bucket, opts \\ []) do
      with {:error, _} <- head_bucket(bucket, opts) do
        create_bucket(region, bucket, opts)
      end
    end

    def create_bucket(region, bucket, opts \\ []) do
      bucket
      |> ExAws.S3.put_bucket(region, opts)
      |> perform(opts)
      |> then(fn result ->
        Process.sleep(10)
        result
      end)
    end

    def head_bucket(bucket, opts \\ []) do
      bucket
      |> ExAws.S3.head_bucket()
      |> perform(opts)
      |> then(fn result ->
        Process.sleep(10)
        result
      end)
    end

    def head_object(bucket, key, opts \\ []) do
      bucket
      |> ExAws.S3.head_object(key, opts)
      |> perform(opts)
      |> then(fn
        {:ok, %{headers: headers}} ->
          Process.sleep(10)
          {:ok, headers}

        {:error, reason} ->
          Process.sleep(10)
          {:error, reason}
      end)
    end

    def put_object(bucket, key, content, opts \\ []) do
      bucket
      |> ExAws.S3.put_object(key, content, opts)
      |> perform(opts)
      |> then(fn
        {:ok, %{headers: headers}} ->
          Process.sleep(10)
          {:ok, Map.new(headers)}

        {:error, reason} ->
          Process.sleep(10)
          {:error, reason}
      end)
    end

    def upload_part(bucket, key, upload_id, part_number, content, opts \\ []) do
      bucket
      |> ExAws.S3.upload_part(key, upload_id, part_number, content, opts)
      |> perform(opts)
      |> then(fn result ->
        Process.sleep(10)
        result
      end)
    end

    def create_multipart_upload(bucket, key, opts \\ []) do
      bucket
      |> ExAws.S3.initiate_multipart_upload(key, opts)
      |> perform(opts)
      |> then(fn
        {:ok, %{body: body}} ->
          Process.sleep(10)
          {:ok, body}

        {:error, reason} ->
          Process.sleep(10)
          {:error, reason}
      end)
    end

    defp perform(op, opts) do
      ExAws.Operation.perform(op, config(opts))
    end
  end
end
