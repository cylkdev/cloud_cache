defmodule CloudCache.S3 do
  alias ExAws.{Operation, S3}

  @app :up
  @region "us-west-1"
  @one_minute_seconds 60

  @config_keys [
    :port,
    :scheme,
    :host,
    :http_client,
    :region,
    :access_key_id,
    :secret_access_key,
    :retries,
    :json_codec,
    :normalize_path,
    :require_imds_v2
  ]

  @doc """
  Returns the S3 configuration as a map.

  The config is resolved in the following order:

    1. Options under the `:s3` key in the given options:

        CloudCache.S3.config(s3: [region: "us-east-2"])

    2. **Top-level options** passed directly to `config/1`

        CloudCache.S3.config(region: "us-east-2")

    3. Application configuration provided via `config :up, :s3`

        config :up, :s3,
          region: "us-west-1",
          access_key_id: System.get_env("AWS_ACCESS_KEY_ID")

    4. If the value is not set or nil the default value is used.

  ## Examples

      iex> CloudCache.S3.config()
      %{
        port: 443,
        scheme: "https://",
        host: "s3.us-west-1.amazonaws.com",
        http_client: CloudCache.S3.HTTP,
        region: "us-west-1",
        access_key_id: "<ACCESS_KEY_ID>",
        secret_access_key: "<SECRET_ACCESS_KEY>",
        retries: [max_attempts: 10, base_backoff_in_ms: 10, max_backoff_in_ms: 10000],
        json_codec: Jason,
        normalize_path: true,
        require_imds_v2: false
      }
  """
  def config(opts \\ []) do
    http_client = get_aws_config(opts, :http_client, CloudCache.S3.HTTP)
    region = get_aws_config(opts, :region, @region)
    access_key_id = get_aws_config(opts, :access_key_id, "<ACCESS_KEY_ID>")
    secret_access_key = get_aws_config(opts, :secret_access_key, "<SECRET_ACCESS_KEY>")

    config =
      opts
      |> Keyword.take(@config_keys)
      |> Keyword.merge(
        http_client: http_client,
        region: region,
        access_key_id: access_key_id,
        secret_access_key: secret_access_key
      )

    ExAws.Config.new(:s3, config)
  end

  defp get_aws_config(opts, key, default) do
    opts[:s3][key] ||
      opts[key] ||
      Application.get_env(@app, :s3, [])[key] ||
      default
  end

  @doc """
  CloudCache.S3.describe_object("requis-developer-sandbox", "Bootstrap.png", s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])

  CloudCache.S3.describe_object("requis-developer-sandbox", "does_not_exist", s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])
  """
  def describe_object(bucket, object, opts \\ []) do
    sandbox? = opts[:s3][:sandbox]

    if sandbox? && !sandbox_disabled?() do
      case bucket |> S3.head_object(object, opts) |> perform(opts) do
        {:ok, %{headers: headers}} ->
          {:ok,
           headers
           |> Keyword.take([:last_modified, :content_length, :etag, :content_type])
           |> Map.new()}

        {:error, %{status: status}} when status in 400..499 ->
          {:error, ErrorMessage.not_found("object not found", %{bucket: bucket, object: object})}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service unavailable.", %{
             reason: reason
           })}
      end
    else
      sandbox_describe_object_response(bucket, object, opts)
    end
  end

  @doc """
  CloudCache.S3.pre_sign("requis-developer-sandbox", "example.zip", s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "u7kT5uqe8DqCNOjFf0WMGTOliZWeoymiImZJEJ9K"])
  """
  def pre_sign(bucket, object, opts \\ []) do
    sandbox? = opts[:s3][:sandbox]

    if sandbox? && !sandbox_disabled?() do
      http_method = opts[:http_method] || :put

      expires_in = opts[:expires_in] || @one_minute_seconds
      sign_opts = Keyword.put(opts, :expires_in, expires_in)

      case opts |> config() |> S3.presigned_url(http_method, bucket, object, sign_opts) do
        {:ok, url} ->
          {:ok,
           %{
             key: object,
             url: url,
             expires_in: expires_in,
             expires_at: DateTime.add(DateTime.utc_now(), expires_in, :second)
           }}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service unavailable.", %{
             reason: reason
           })}
      end
    else
      sandbox_pre_sign_response(bucket, object, opts)
    end
  end

  @doc """
  CloudCache.S3.list_parts("requis-developer-sandbox", "ChatGPT.dmg.zip", "4eT1xCDeyW5LEyr2Of45Ig3VNoVD.tbtAZXVVldYfdoMKeLKtS6SdOC9nxcT82rS51fPKuCsAk_Xde5RNsMxMc5gXGvxL4nqUBDnXnCA6DonGgUPHWsp_nWuHhwhkxqS", s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])
  {:ok, %{
    body: %{parts: []},
    headers: %{
      date: ["Fri, 29 Aug 2025 03:50:53 GMT"],
      server: ["AmazonS3"],
      content_type: ["application/xml"],
      transfer_encoding: ["chunked"],
      x_amz_id_2: ["mI7ICPDOYcJ5aZ0x2qLKRreQ2Eimjuz85RZmE1VKGx4gf84QSQrMQgJCIqV8Qp/nU3oywAGImOrIpPN91zSuxA4hezPxH9BmWCWEsqHNbHM="],
      x_amz_request_id: ["FVAD9RR2480C7BTR"]
    },
    status_code: 200
  }}
  """
  def list_parts(bucket, object, upload_id, opts \\ []) do
    sandbox? = opts[:s3][:sandbox]

    if sandbox? && !sandbox_disabled?() do
      list_parts_opts =
        if Keyword.has_key?(opts, :partber_marker) do
          query_params = %{"part-number-marker" => opts[:partber_marker]}

          opts
          |> Keyword.delete(:partber_marker)
          |> Keyword.update(:query_params, query_params, &Map.merge(&1, query_params))
        else
          Keyword.take(opts, [:query_params])
        end

      bucket
      |> S3.list_parts(object, upload_id, list_parts_opts)
      |> perform(opts)
      |> then(fn
        {:ok, %{body: %{parts: parts}}} ->
          {:ok, {parts, length(parts)}}

        {:error, %{status: status}} when status in 400..499 ->
          {:error, ErrorMessage.not_found("object not found", %{bucket: bucket, object: object})}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service unavailable.", %{
             reason: reason
           })}
      end)
    else
      sandbox_list_parts_response(bucket, object, upload_id, opts)
    end
  end

  @doc """
  CloudCache.S3.pre_sign_part("requis-developer-sandbox", "ChatGPT.dmg.zip", "4eT1xCDeyW5LEyr2Of45Ig3VNoVD.tbtAZXVVldYfdoMKeLKtS6SdOC9nxcT82rS51fPKuCsAk_Xde5RNsMxMc5gXGvxL4nqUBDnXnCA6DonGgUPHWsp_nWuHhwhkxqS", [{1, }], s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])
  curl -X PUT -T ./ChatGPT.dmg.zip "https://s3.us-west-1.amazonaws.com/requis-developer-sandbox/ChatGPT.dmg.zip?partNumber=1&uploadId=4eT1xCDeyW5LEyr2Of45Ig3VNoVD.tbtAZXVVldYfdoMKeLKtS6SdOC9nxcT82rS51fPKuCsAk_Xde5RNsMxMc5gXGvxL4nqUBDnXnCA6DonGgUPHWsp_nWuHhwhkxqS&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA25U36JCW7EXUUFIF%2F20250829%2Fus-west-1%2Fs3%2Faws4_request&X-Amz-Date=20250829T035317Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=75fdbddb331fb5b2e29d90f4bc3b64a7d31f176e59892751bb9c3ebe0aa4e990"
  """
  def pre_sign_part(bucket, object, upload_id, part_number, opts \\ []) do
    sandbox? = opts[:s3][:sandbox]

    if sandbox? && !sandbox_disabled?() do
      query_params = %{"uploadId" => upload_id, "partNumber" => part_number}

      sign_opts = Keyword.update(opts, :query_params, query_params, &Map.merge(&1, query_params))

      pre_sign(bucket, object, sign_opts)
    else
      sandbox_pre_sign_part_response(bucket, object, upload_id, part_number, opts)
    end
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
    sandbox? = opts[:s3][:sandbox]

    if sandbox? && !sandbox_disabled?() do
      dest_bucket
      |> S3.upload_part_copy(
        dest_object,
        src_bucket,
        src_object,
        upload_id,
        part_number,
        range,
        opts
      )
      |> perform(opts)
      |> then(fn
        {:ok, _} ->
          :ok

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service unavailable.", %{
             reason: reason
           })}
      end)
    else
      sandbox_copy_part_response(
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
  end

  @doc """
  CloudCache.S3.complete_multipart_upload("requis-developer-sandbox", "ChatGPT.dmg.zip", "4eT1xCDeyW5LEyr2Of45Ig3VNoVD.tbtAZXVVldYfdoMKeLKtS6SdOC9nxcT82rS51fPKuCsAk_Xde5RNsMxMc5gXGvxL4nqUBDnXnCA6DonGgUPHWsp_nWuHhwhkxqS", [{1, "c3d60e2f28497d48df00208a04b1d053"}], s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])
  {:ok, %{
    last_modified: "Fri, 29 Aug 2025 03:48:15 GMT",
    content_length: 56567269,
    etag: "1283ef0559aef6b42e4402bbaaba0234-1",
    content_type: "binary/octet-stream"
  }}
  """
  def complete_multipart_upload(bucket, object, upload_id, parts, opts \\ []) do
    sandbox? = opts[:s3][:sandbox]

    if sandbox? && !sandbox_disabled?() do
      bucket
      |> S3.complete_multipart_upload(object, upload_id, ensure_parts!(parts))
      |> perform(opts)
      |> then(fn
        {:ok, _} = response ->
          response

        {:error, %{status: status}} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("multipart upload not found.", %{
             bucket: bucket,
             object: object,
             upload_id: upload_id
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service unavailable.", %{
             reason: reason
           })}
      end)
    else
      sandbox_complete_multipart_upload_response(bucket, object, upload_id, parts, opts)
    end
  end

  @doc """
  CloudCache.S3.abort_multipart_upload("requis-developer-sandbox", "ChatGPT.dmg.zip", "JfBuKAnK4OMHUHGvBfeqvukc5udqF2d7UT1Tc9OOCgLGXzEnCHjNLAjM_mZLYKnGMTzjRKR6PaCk8TaOgo.iyEOzxm3znfRhbt2wMwWYsAA_ojqZsbPIaUOdlxCBx5aH", s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])
  {:ok, %{
    last_modified: "Fri, 29 Aug 2025 03:48:15 GMT",
    content_length: 56567269,
    etag: "1283ef0559aef6b42e4402bbaaba0234-1",
    content_type: "binary/octet-stream"
  }}
  """
  def abort_multipart_upload(bucket, object, upload_id, opts \\ []) do
    sandbox? = opts[:s3][:sandbox]

    if sandbox? && !sandbox_disabled?() do
      bucket
      |> S3.abort_multipart_upload(object, upload_id)
      |> perform(opts)
      |> then(fn
        {:ok, _} = response ->
          response

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service unavailable.", %{
             reason: reason
           })}
      end)
    else
      sandbox_abort_multipart_upload_response(bucket, object, upload_id, opts)
    end
  end

  @doc """
  CloudCache.S3.create_multipart_upload("requis-developer-sandbox", "ChatGPT.dmg.zip", s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])
  {:ok,
  %{
    body: %{
      key: "ChatGPT.dmg.zip",
      bucket: "requis-developer-sandbox",
      upload_id: "4eT1xCDeyW5LEyr2Of45Ig3VNoVD.tbtAZXVVldYfdoMKeLKtS6SdOC9nxcT82rS51fPKuCsAk_Xde5RNsMxMc5gXGvxL4nqUBDnXnCA6DonGgUPHWsp_nWuHhwhkxqS"
    },
    headers: %{
      date: ["Fri, 29 Aug 2025 03:48:15 GMT"],
      server: ["AmazonS3"],
      transfer_encoding: ["chunked"],
      x_amz_id_2: ["C8sk4Z+e7Qu3kGIurdVN+UTcobXXGUNLE9DlO+ZdsL3Skj1G4AGwjhmV4Uhehx/HSAZkcu8bIYw="],
      x_amz_request_id: ["J3257ZEAWM5P4VXT"],
      x_amz_server_side_encryption: ["AES256"]
    },
    status_code: 200
  }}
  """
  def create_multipart_upload(bucket, object, opts) do
    sandbox? = opts[:s3][:sandbox]

    if sandbox? && !sandbox_disabled?() do
      bucket
      |> S3.initiate_multipart_upload(object, opts)
      |> perform(opts)
      |> then(fn
        {:ok, _} = response ->
          response

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service unavailable.", %{
             reason: reason
           })}
      end)
    else
      sandbox_create_multipart_upload_response(bucket, object, opts)
    end
  end

  defp ensure_parts!(values) do
    Enum.map(values, fn
      {part, etag} when is_integer(part) and is_binary(etag) ->
        {part, etag}

      {part, etag} when is_binary(part) and is_binary(etag) ->
        {String.to_integer(part), etag}

      _ ->
        raise ArgumentError, "Expected a list of {integer(), string()}, got: #{inspect(values)}"
    end)
  end

  defp perform(op, opts) do
    with {:ok, response} <- Operation.perform(op, config(opts)) do
      {:ok, deserialize(response) |> IO.inspect(label: "1")}
    end
  end

  defp deserialize(values) when is_list(values) do
    Enum.map(values, &deserialize/1)
  end

  defp deserialize({key, val}) do
    final_key =
      if is_binary(key) do
        key |> String.replace("-", "_") |> String.to_atom()
      else
        key
      end

    final_value = deserialize_value(final_key, val)

    {final_key, deserialize(final_value)}
  end

  defp deserialize(payload) when is_map(payload) and not is_struct(payload) do
    Map.new(payload, fn {key, val} ->
      deserialize({key, val})
    end)
  end

  defp deserialize(val) do
    val
  end

  defp deserialize_value(key, vals) when is_list(vals) do
    Enum.map(vals, &deserialize_value(key, &1))
  end

  defp deserialize_value(key, val) when key in [:content_length, :partber, :size] do
    String.to_integer(val)
  end

  defp deserialize_value(:date, val) do
    val
    |> Timex.parse!("{WDshort}, {0D} {Mshort} {YYYY} {h24}:{m}:{s} GMT")
    |> DateTime.from_naive!("Etc/UTC")
  end

  defp deserialize_value(:etag, val) do
    String.replace(val, "\"", "")
  end

  defp deserialize_value(_, val) do
    val
  end

  if Mix.env() === :test do
    defdelegate sandbox_disabled?, to: CloudCache.Support.S3Sandbox

    defdelegate sandbox_describe_object_response(bucket, object, opts),
      to: CloudCache.Support.S3Sandbox,
      as: :describe_object_response

    defdelegate sandbox_pre_sign_response(bucket, object, opts),
      to: CloudCache.Support.S3Sandbox,
      as: :pre_sign_response

    defdelegate sandbox_list_parts_response(bucket, object, upload_id, opts),
      to: CloudCache.Support.S3Sandbox,
      as: :list_parts_response

    defdelegate sandbox_create_multipart_upload_response(bucket, object, opts),
      to: CloudCache.Support.S3Sandbox,
      as: :create_multipart_upload_response

    defdelegate sandbox_pre_sign_part_response(bucket, object, upload_id, part_number, opts),
      to: CloudCache.Support.S3Sandbox,
      as: :pre_sign_part_response

    defdelegate sandbox_abort_multipart_upload_response(bucket, object, upload_id, opts),
      to: CloudCache.Support.S3Sandbox,
      as: :abort_multipart_upload_response

    defdelegate sandbox_complete_multipart_upload_response(
                  bucket,
                  object,
                  upload_id,
                  parts,
                  opts
                ),
                to: CloudCache.Support.S3Sandbox,
                as: :complete_multipart_upload_response

    defdelegate sandbox_copy_part_response(
                  dest_bucket,
                  dest_object,
                  src_bucket,
                  src_object,
                  upload_id,
                  part_number,
                  range,
                  opts
                ),
                to: CloudCache.Support.S3Sandbox,
                as: :copy_part_response
  else
    defp sandbox_disabled?, do: true

    defp sandbox_describe_object_response(bucket, object, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.describe_object/3 outside of test.

      bucket: #{inspect(bucket)}
      object: #{inspect(object)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_pre_sign_response(bucket, object, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.pre_sign/3 outside of test.

      bucket: #{inspect(bucket)}
      object: #{inspect(object)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_list_parts_response(bucket, object, upload_id, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.list_parts/4 outside of test.

      bucket: #{inspect(bucket)}
      object: #{inspect(object)}
      upload_id: #{inspect(upload_id)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_create_multipart_upload_response(bucket, object, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)} outside of test.

      bucket: #{inspect(bucket)}
      object: #{inspect(object)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_pre_sign_part_response(bucket, object, upload_id, part_number, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.pre_sign_part/5 outside of test.

      bucket: #{inspect(bucket)}
      object: #{inspect(object)}
      upload_id: #{inspect(upload_id)}
      part_number: #{inspect(part_number)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_abort_multipart_upload_response(bucket, object, upload_id, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.abort_multipart_upload/4 outside of test.

      bucket: #{inspect(bucket)}
      object: #{inspect(object)}
      upload_id: #{inspect(upload_id)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_complete_multipart_upload_response(bucket, object, upload_id, parts, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.complete_multipart_upload/5 outside of test.

      bucket: #{inspect(bucket)}
      object: #{inspect(object)}
      upload_id: #{inspect(upload_id)}
      parts: #{inspect(parts)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_copy_part_response(
           dest_bucket,
           dest_object,
           src_bucket,
           src_object,
           upload_id,
           part_number,
           range,
           opts
         ) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_part/8 outside of test.

      dest_bucket: #{inspect(dest_bucket)}
      dest_object: #{inspect(dest_object)}
      src_bucket: #{inspect(src_bucket)}
      src_object: #{inspect(src_object)}
      upload_id: #{inspect(upload_id)}
      part_number: #{inspect(part_number)}
      range: #{inspect(range)}
      options: #{inspect(opts, pretty: true)}
      """
    end
  end
end
