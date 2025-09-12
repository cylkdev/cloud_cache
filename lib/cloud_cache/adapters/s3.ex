defmodule CloudCache.Adapters.S3 do
  @moduledoc """
  ## Getting Started

  Add the following dependencies to your `mix.exs`:

      defp deps do
        [
          {:ex_aws, "~> 2.0"},
          {:ex_aws_s3, "~> 2.0"},
          {:sweet_xml, ">= 0.0.0"},
          {:proper_case, "~> 1.0"},
          {:jason, "~> 1.0"} # (or another JSON adapter)
        ]
      end
  """
  alias ExAws.{Operation, S3}
  alias CloudCache.Adapters.S3.{Multipart, XMLParser}

  @behaviour CloudCache.Adapter

  @logger_prefix "CloudCache.Adapters.S3"

  @one_minute_seconds 60
  @http_client CloudCache.Adapters.S3.HTTP
  @region "us-west-1"
  @sandbox_scheme "http://"
  @sandbox_host "s3.localhost.localstack.cloud"
  @sandbox_port 4566
  @default_retry_options [
    max_attempts: if(Mix.env() === :test, do: 1, else: 10),
    base_backoff_in_ms: 10,
    max_backoff_in_ms: 10_000
  ]
  @default_options [
    s3: [
      sandbox_enabled: Mix.env() === :test,
      sandbox: [
        scheme: @sandbox_scheme,
        host: @sandbox_host,
        port: @sandbox_port
      ],
      http_client: @http_client,
      region: @region,
      access_key_id: if(Mix.env() === :test, do: "test", else: "<ACCESS_KEY_ID>"),
      secret_access_key: if(Mix.env() === :test, do: "test", else: "<SECRET_ACCESS_KEY>"),
      retries: @default_retry_options
    ]
  ]

  @s3_config_keys [
    :port,
    :scheme,
    :host,
    :http_client,
    :access_key_id,
    :secret_access_key,
    :region,
    :json_codec,
    :retries,
    :normalize_path,
    :require_imds_v2
  ]

  # 64 MiB (67_108_864 bytes)
  @sixty_four_mib 64 * 1_024 * 1_024

  @doc """
  Returns the S3 configuration as a map.

  CloudCache.Adapters.S3.config()
  """
  def config(opts \\ []) do
    opts =
      Keyword.merge(@default_options, opts, fn
        _k, v1, v2 when is_list(v2) -> Keyword.merge(v1, v2)
        _k, v1, v2 when is_map(v2) -> Map.merge(v1, v2)
        _, _v1, v2 -> v2
      end)

    sandbox_opts =
      if CloudCache.Config.mix_env() === :test do
        sandbox_opts = opts[:sandbox] || []

        case sandbox_opts[:endpoint] do
          nil ->
            [
              scheme: uri_scheme(sandbox_opts[:scheme] || @sandbox_scheme),
              host: sandbox_opts[:host] || @sandbox_host,
              port: sandbox_opts[:port] || @sandbox_port
            ]

          uri ->
            uri = URI.parse(uri)
            scheme = uri_scheme(uri.scheme || @sandbox_scheme)
            host = uri.host || @sandbox_host
            port = uri.port || @sandbox_port

            [
              scheme: scheme,
              host: host,
              port: port
            ]
        end
      else
        []
      end

    overrides =
      :ex_aws
      |> Application.get_all_env()
      |> Keyword.merge(opts[:s3] || [])
      |> Keyword.update(
        :retries,
        @default_retry_options,
        &Keyword.merge(@default_retry_options, &1)
      )
      |> then(&Keyword.merge(sandbox_opts, &1))
      |> Keyword.take(@s3_config_keys)
      |> dbg()

    ExAws.Config.new(:s3, overrides)
  end

  defp uri_scheme("https" <> _), do: "https://"
  defp uri_scheme("http" <> _), do: "http://"
  defp uri_scheme(_), do: "https://"

  @impl true
  @doc """
  CloudCache.Adapters.S3.head_object("requis-developer-sandbox", "Bootstrap.png", s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])

  CloudCache.Adapters.S3.head_object("requis-developer-sandbox", "does_not_exist", s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])
  """
  def head_object(bucket, object, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket |> S3.head_object(object, opts) |> perform(opts) do
        {:ok, _} = result ->
          result

        {:error, %{status: status}} when status in 400..499 ->
          {:error, ErrorMessage.not_found("object not found", %{bucket: bucket, object: object})}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             bucket: bucket,
             object: object,
             reason: reason
           })}
      end
    else
      sandbox_head_object_response(bucket, object, opts)
    end
  end

  @impl true
  @doc """
  ...
  """
  def put_object(bucket, object, body, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket
           |> S3.put_object(object, body, opts)
           |> perform(opts) do
        {:ok, %{body: body} = response} ->
          {:ok, %{response | body: maybe_parse_xml(body)}}

        {:error, %{status: status} = reason} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("bucket not found", %{
             bucket: bucket,
             object: object,
             body: body,
             reason: reason
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             bucket: bucket,
             object: object,
             body: body,
             reason: reason
           })}
      end
    else
      sandbox_put_object_response(bucket, object, body, opts)
    end
  end

  @impl true
  @doc """
  ...
  """
  def copy_object(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case dest_bucket
           |> S3.put_object_copy(dest_object, src_bucket, src_object, opts)
           |> perform(opts) do
        {:ok, %{body: body} = response} ->
          {:ok, %{response | body: maybe_parse_xml(body)}}

        {:error, %{status: status}} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("object not found", %{
             dest_bucket: dest_bucket,
             dest_object: dest_object,
             src_bucket: src_bucket,
             src_object: src_object
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             dest_bucket: dest_bucket,
             dest_object: dest_object,
             src_bucket: src_bucket,
             src_object: src_object,
             reason: reason
           })}
      end
    else
      sandbox_copy_object_response(dest_bucket, dest_object, src_bucket, src_object, opts)
    end
  end

  @impl true
  @doc """
  CloudCache.Adapters.S3.pre_sign("requis-developer-sandbox", "example.zip", s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "u7kT5uqe8DqCNOjFf0WMGTOliZWeoymiImZJEJ9K"])
  """
  def pre_sign(bucket, object, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
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
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             bucket: bucket,
             object: object,
             reason: reason
           })}
      end
    else
      sandbox_pre_sign_response(bucket, object, opts)
    end
  end

  @impl true
  @doc """
  CloudCache.Adapters.S3.pre_sign_part("requis-developer-sandbox", "ChatGPT.dmg.zip", "4eT1xCDeyW5LEyr2Of45Ig3VNoVD.tbtAZXVVldYfdoMKeLKtS6SdOC9nxcT82rS51fPKuCsAk_Xde5RNsMxMc5gXGvxL4nqUBDnXnCA6DonGgUPHWsp_nWuHhwhkxqS", [{1, }], s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])
  curl -X PUT -T ./ChatGPT.dmg.zip "https://s3.us-west-1.amazonaws.com/requis-developer-sandbox/ChatGPT.dmg.zip?partNumber=1&uploadId=4eT1xCDeyW5LEyr2Of45Ig3VNoVD.tbtAZXVVldYfdoMKeLKtS6SdOC9nxcT82rS51fPKuCsAk_Xde5RNsMxMc5gXGvxL4nqUBDnXnCA6DonGgUPHWsp_nWuHhwhkxqS&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA25U36JCW7EXUUFIF%2F20250829%2Fus-west-1%2Fs3%2Faws4_request&X-Amz-Date=20250829T035317Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=75fdbddb331fb5b2e29d90f4bc3b64a7d31f176e59892751bb9c3ebe0aa4e990"
  """
  def pre_sign_part(bucket, object, upload_id, part_number, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      query_params = %{"uploadId" => upload_id, "partNumber" => part_number}

      sign_opts = Keyword.update(opts, :query_params, query_params, &Map.merge(&1, query_params))

      case pre_sign(bucket, object, sign_opts) do
        {:error, %{details: details} = message} ->
          details =
            Map.merge(details || %{}, %{
              upload_id: upload_id,
              part_number: part_number
            })

          {:error, %{message | details: details}}

        res ->
          res
      end
    else
      sandbox_pre_sign_part_response(bucket, object, upload_id, part_number, opts)
    end
  end

  @impl true
  @doc """
  CloudCache.Adapters.S3.list_parts("requis-developer-sandbox", "ChatGPT.dmg.zip", "4eT1xCDeyW5LEyr2Of45Ig3VNoVD.tbtAZXVVldYfdoMKeLKtS6SdOC9nxcT82rS51fPKuCsAk_Xde5RNsMxMc5gXGvxL4nqUBDnXnCA6DonGgUPHWsp_nWuHhwhkxqS", s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])
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
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      list_parts_opts =
        if Keyword.has_key?(opts, :part_number_marker) do
          query_params = %{"part-number-marker" => opts[:part_number_marker]}

          opts
          |> Keyword.delete(:part_number_marker)
          |> Keyword.update(:query_params, query_params, &Map.merge(&1, query_params))
        else
          Keyword.take(opts, [:query_params])
        end

      bucket
      |> S3.list_parts(object, upload_id, list_parts_opts)
      |> perform(opts)
      |> then(fn
        {:ok, _} = result ->
          result

        {:error, %{status: status}} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("object not found", %{
             bucket: bucket,
             object: object,
             upload_id: upload_id
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             bucket: bucket,
             object: object,
             upload_id: upload_id,
             reason: reason
           })}
      end)
    else
      sandbox_list_parts_response(bucket, object, upload_id, opts)
    end
  end

  @impl true
  @doc """
  ...
  """
  def upload_part(bucket, object, upload_id, part_number, body, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      bucket
      |> S3.upload_part(object, upload_id, part_number, body, opts)
      |> perform(opts)
      |> then(fn
        {:ok, _} = result ->
          result

        {:error, %{status: status}} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("object not found", %{
             bucket: bucket,
             object: object,
             upload_id: upload_id,
             part_number: part_number,
             body: body
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             bucket: bucket,
             object: object,
             upload_id: upload_id,
             part_number: part_number,
             body: body,
             reason: reason
           })}
      end)
    else
      sandbox_upload_part_response(bucket, object, upload_id, part_number, body, opts)
    end
  end

  @impl true
  @doc """
  ...
  """
  def copy_object_multipart(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      with {:ok, describe_obj} <- head_object(src_bucket, src_object, opts),
           {:ok, create_mpu} <- create_multipart_upload(dest_bucket, dest_object, opts),
           {:ok, parts} <-
             copy_parts(
               dest_bucket,
               dest_object,
               src_bucket,
               src_object,
               create_mpu.body.upload_id,
               describe_obj.headers.content_length,
               opts
             ) do
        complete_multipart_upload(
          dest_bucket,
          dest_object,
          create_mpu.body.upload_id,
          parts,
          opts
        )
      end
    else
      sandbox_copy_object_multipart_response(
        dest_bucket,
        dest_object,
        src_bucket,
        src_object,
        opts
      )
    end
  end

  @impl true
  @doc """
  ...
  """
  def copy_parts(
        dest_bucket,
        dest_object,
        src_bucket,
        src_object,
        upload_id,
        content_length,
        opts \\ []
      ) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      content_byte_stream_opts = opts[:content_byte_stream] || []
      content_start_index = content_byte_stream_opts[:start_index] || 0
      content_chunk_size = content_byte_stream_opts[:chunk_size] || @sixty_four_mib

      async_stream_opts =
        content_byte_stream_opts
        |> Keyword.take([:max_concurrency, :timeout, :on_timeout])
        |> Keyword.put_new(:max_concurrency, System.schedulers_online())
        |> Keyword.put(:ordered, false)

      content_start_index
      |> Multipart.content_byte_stream(content_length, content_chunk_size, :forward)
      |> Stream.with_index(1)
      |> Task.async_stream(
        fn {{start_byte, end_byte}, part_num} ->
          async_stream_task_copy_part(
            {{start_byte, end_byte}, part_num},
            dest_bucket,
            dest_object,
            src_bucket,
            src_object,
            upload_id,
            content_length,
            opts
          )
        end,
        async_stream_opts
      )
      |> handle_async_stream_response()
      |> then(fn
        {:ok, results} ->
          {
            :ok,
            results
            |> Enum.sort(fn {_, pn1}, {_, pn2} -> pn1 < pn2 end)
            |> Enum.map(fn {%{body: %{etag: etag}}, part_num} -> {part_num, etag} end)
          }

        {:error, reasons} ->
          {
            :error,
            reasons
            |> Enum.sort(fn {_, pn1}, {_, pn2} -> pn1 < pn2 end)
            |> Enum.map(fn {term, _} -> term end)
          }
      end)
    else
      sandbox_copy_parts_response(
        dest_bucket,
        dest_object,
        src_bucket,
        src_object,
        upload_id,
        content_length,
        opts
      )
    end
  end

  defp async_stream_task_copy_part(
         {{start_byte, end_byte}, part_num},
         dest_bucket,
         dest_object,
         src_bucket,
         src_object,
         upload_id,
         content_length,
         opts
       ) do
    CloudCache.Logger.debug(
      @logger_prefix,
      """
      Copying part.

      part_number: #{inspect(part_num)}
      start_byte: #{inspect(start_byte)}
      end_byte: #{inspect(end_byte)}

      dest_bucket: #{inspect(dest_bucket)}
      dest_object: #{inspect(dest_object)}
      src_bucket: #{inspect(src_bucket)}
      src_object: #{inspect(src_object)}
      upload_id: #{inspect(upload_id)}
      content_length: #{inspect(content_length)}
      """
    )

    case copy_part(
           dest_bucket,
           dest_object,
           src_bucket,
           src_object,
           upload_id,
           part_num,
           start_byte..end_byte,
           opts
         ) do
      {:ok, result} -> {:ok, {result, part_num}}
      {:error, term} -> {:error, {term, part_num}}
    end
  end

  defp handle_async_stream_response(results) do
    results
    |> Enum.reduce({[], []}, fn
      {:ok, {:ok, result}}, {results, errors} ->
        {[result | results], errors}

      {:ok, {:error, reason}}, {results, errors} ->
        {results, [reason | errors]}

      {:exit, reason}, {results, errors} ->
        err = ErrorMessage.internal_server_error("Task exited.", %{reason: reason})
        {results, [err | errors]}
    end)
    |> then(fn
      {results, []} -> {:ok, results}
      {_, errors} -> {:error, errors}
    end)
  end

  @impl true
  @doc """
  ...
  """
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
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      dest_bucket
      |> S3.upload_part_copy(
        dest_object,
        src_bucket,
        src_object,
        upload_id,
        part_number,
        src_range,
        opts
      )
      |> perform(opts)
      |> then(fn
        {:ok, _} = result ->
          result

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             dest_bucket: dest_bucket,
             dest_object: dest_object,
             src_bucket: src_bucket,
             src_object: src_object,
             src_range: src_range,
             upload_id: upload_id,
             part_number: part_number,
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
        src_range,
        opts
      )
    end
  end

  @impl true
  @doc """
  CloudCache.Adapters.S3.complete_multipart_upload("requis-developer-sandbox", "ChatGPT.dmg.zip", "4eT1xCDeyW5LEyr2Of45Ig3VNoVD.tbtAZXVVldYfdoMKeLKtS6SdOC9nxcT82rS51fPKuCsAk_Xde5RNsMxMc5gXGvxL4nqUBDnXnCA6DonGgUPHWsp_nWuHhwhkxqS", [{1, "c3d60e2f28497d48df00208a04b1d053"}], s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])
  {:ok, %{
    last_modified: "Fri, 29 Aug 2025 03:48:15 GMT",
    content_length: 56567269,
    etag: "1283ef0559aef6b42e4402bbaaba0234-1",
    content_type: "binary/octet-stream"
  }}
  """
  def complete_multipart_upload(bucket, object, upload_id, parts, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      bucket
      |> S3.complete_multipart_upload(object, upload_id, validate_parts!(parts))
      |> perform(opts)
      |> then(fn
        {:ok, _} = result ->
          result

        {:error, %{status: status}} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("multipart upload not found.", %{
             bucket: bucket,
             object: object,
             upload_id: upload_id
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             bucket: bucket,
             object: object,
             upload_id: upload_id,
             parts: parts,
             reason: reason
           })}
      end)
    else
      sandbox_complete_multipart_upload_response(bucket, object, upload_id, parts, opts)
    end
  end

  @impl true
  @doc """
  CloudCache.Adapters.S3.abort_multipart_upload("requis-developer-sandbox", "ChatGPT.dmg.zip", "JfBuKAnK4OMHUHGvBfeqvukc5udqF2d7UT1Tc9OOCgLGXzEnCHjNLAjM_mZLYKnGMTzjRKR6PaCk8TaOgo.iyEOzxm3znfRhbt2wMwWYsAA_ojqZsbPIaUOdlxCBx5aH", s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])
  {:ok, %{
    last_modified: "Fri, 29 Aug 2025 03:48:15 GMT",
    content_length: 56567269,
    etag: "1283ef0559aef6b42e4402bbaaba0234-1",
    content_type: "binary/octet-stream"
  }}
  """
  def abort_multipart_upload(bucket, object, upload_id, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      bucket
      |> S3.abort_multipart_upload(object, upload_id)
      |> perform(opts)
      |> then(fn
        {:ok, _} = result ->
          result

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             bucket: bucket,
             object: object,
             upload_id: upload_id,
             reason: reason
           })}
      end)
    else
      sandbox_abort_multipart_upload_response(bucket, object, upload_id, opts)
    end
  end

  @impl true
  @doc """
  CloudCache.Adapters.S3.create_multipart_upload("requis-developer-sandbox", "ChatGPT.dmg.zip", s3: [region: "us-west-1", access_key_id: "AKIA25U36JCW7EXUUFIF", secret_access_key: "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"])
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
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      bucket
      |> S3.initiate_multipart_upload(object, opts)
      |> perform(opts)
      |> then(fn
        {:ok, _} = result ->
          result

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             bucket: bucket,
             object: object,
             reason: reason
           })}
      end)
    else
      sandbox_create_multipart_upload_response(bucket, object, opts)
    end
  end

  # -----------------
  # Helper API
  # -----------------

  defp validate_parts!(entries) do
    Enum.map(entries, fn
      {part, etag} when is_integer(part) and is_binary(etag) ->
        {part, etag}

      {part, etag} when is_binary(part) and is_binary(etag) ->
        {String.to_integer(part), etag}

      failed_value ->
        raise ArgumentError, """
        Expected parts parameters to be a list of `{part_number :: integer(), etag :: binary()}`

        failed_value:

        #{inspect(failed_value)}

        entries:

        #{inspect(entries)}
        """
    end)
  end

  defp perform(op, opts) do
    with {:ok, payload} <- Operation.perform(op, config(opts)) do
      {:ok, deserialize(payload)}
    end
  end

  defp maybe_parse_xml(val) do
    if is_binary(val) do
      val
      |> XMLParser.parse()
      |> deserialize()
    else
      val
    end
  end

  defp deserialize_key(str) when is_binary(str), do: str |> normalize_key() |> String.to_atom()
  defp deserialize_key(term), do: term

  defp normalize_key(key) when key in ["etag", "e_tag"] do
    "etag"
  end

  defp normalize_key(key) do
    key
    |> String.downcase()
    |> String.replace(["-", " "], "_")
    |> ProperCase.snake_case()
  end

  defp deserialize(values) when is_list(values) do
    Enum.map(values, &deserialize/1)
  end

  defp deserialize({key, val}) do
    final_key = deserialize_key(key)
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

  defp deserialize_value(key, val) when key in [:content_length, :part_number, :size] do
    String.to_integer(val)
  end

  defp deserialize_value(:etag, val), do: remove_quotes(val)

  defp deserialize_value(key, val)
       when key in [
              :creation_date,
              :date,
              :expiration,
              :initiated,
              :last_modified,
              :replication_time,
              :retain_expiry_date,
              :retain_until_date
            ] do
    if iso8601?(val) do
      case parse_iso8601_datetime(val) do
        {:ok, dt} ->
          dt

        :error ->
          raise ArgumentError,
                "Expected value for field `#{inspect(key)}` " <>
                  "to be an RFC timestamp, got: #{inspect(val)}"
      end
    else
      case parse_rfc_datetime(val) do
        {:ok, datetime} ->
          datetime

        :error ->
          raise ArgumentError,
                "Expected value for field `#{inspect(key)}` " <>
                  "to be an RFC timestamp, got: #{inspect(val)}"
      end
    end
  end

  defp deserialize_value(_, val), do: val

  defp remove_quotes(str), do: String.replace(str, "\"", "")

  defp iso8601?(<<
         _year::binary-size(4),
         "-",
         _month::binary-size(2),
         "-",
         _day::binary-size(2),
         "T",
         _rest::binary
       >>),
       do: true

  defp iso8601?(_), do: false

  defp parse_iso8601_datetime(str) do
    case DateTime.from_iso8601(str) do
      {:ok, datetime, _} -> {:ok, datetime}
      #
      _ -> :error
    end
  end

  defp parse_rfc_datetime(str) do
    case Timex.parse(str, "{WDshort}, {0D} {Mshort} {YYYY} {h24}:{m}:{s} GMT") do
      {:ok, datetime} -> {:ok, DateTime.from_naive!(datetime, "Etc/UTC")}
      # raise ArgumentError, "Failed to RFC timestamp, got: #{inspect(str)}"
      _ -> :error
    end
  end

  # -----------------
  # Sandbox API
  # -----------------

  if Mix.env() === :test do
    defdelegate sandbox_disabled?, to: CloudCache.Adapters.S3.Testing.S3Sandbox

    defdelegate sandbox_head_object_response(bucket, object, opts),
      to: CloudCache.Adapters.S3.Testing.S3Sandbox,
      as: :head_object_response

    defdelegate sandbox_put_object_response(bucket, object, body, opts),
      to: CloudCache.Adapters.S3.Testing.S3Sandbox,
      as: :put_object_response

    defdelegate sandbox_copy_object_response(
                  dest_bucket,
                  dest_object,
                  src_bucket,
                  src_object,
                  opts
                ),
                to: CloudCache.Adapters.S3.Testing.S3Sandbox,
                as: :copy_object_response

    defdelegate sandbox_pre_sign_response(bucket, object, opts),
      to: CloudCache.Adapters.S3.Testing.S3Sandbox,
      as: :pre_sign_response

    defdelegate sandbox_list_parts_response(bucket, object, upload_id, opts),
      to: CloudCache.Adapters.S3.Testing.S3Sandbox,
      as: :list_parts_response

    defdelegate sandbox_upload_part_response(
                  bucket,
                  object,
                  upload_id,
                  part_number,
                  body,
                  opts
                ),
                to: CloudCache.Adapters.S3.Testing.S3Sandbox,
                as: :upload_part_response

    defdelegate sandbox_pre_sign_part_response(bucket, object, upload_id, part_number, opts),
      to: CloudCache.Adapters.S3.Testing.S3Sandbox,
      as: :pre_sign_part_response

    defdelegate sandbox_copy_object_multipart_response(
                  dest_bucket,
                  dest_object,
                  src_bucket,
                  src_object,
                  opts
                ),
                to: CloudCache.Adapters.S3.Testing.S3Sandbox,
                as: :copy_object_multipart_response

    defdelegate sandbox_copy_parts_response(
                  dest_bucket,
                  dest_object,
                  src_bucket,
                  src_object,
                  upload_id,
                  content_length,
                  opts
                ),
                to: CloudCache.Adapters.S3.Testing.S3Sandbox,
                as: :copy_parts_response

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
                to: CloudCache.Adapters.S3.Testing.S3Sandbox,
                as: :copy_part_response

    defdelegate sandbox_complete_multipart_upload_response(
                  bucket,
                  object,
                  upload_id,
                  parts,
                  opts
                ),
                to: CloudCache.Adapters.S3.Testing.S3Sandbox,
                as: :complete_multipart_upload_response

    defdelegate sandbox_abort_multipart_upload_response(bucket, object, upload_id, opts),
      to: CloudCache.Adapters.S3.Testing.S3Sandbox,
      as: :abort_multipart_upload_response

    defdelegate sandbox_create_multipart_upload_response(bucket, object, opts),
      to: CloudCache.Adapters.S3.Testing.S3Sandbox,
      as: :create_multipart_upload_response
  else
    defp sandbox_disabled?, do: true

    defp sandbox_head_object_response(bucket, object, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.head_object/3 outside of test.

      bucket: #{inspect(bucket)}
      object: #{inspect(object)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_put_object_response(bucket, object, body, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_object/5 outside of test.

      bucket: #{inspect(bucket)}
      object: #{inspect(object)}
      body: #{inspect(body)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_copy_object_response(dest_bucket, dest_object, src_bucket, src_object, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_object/5 outside of test.

      dest_bucket: #{inspect(dest_bucket)}
      dest_object: #{inspect(dest_object)}
      src_bucket: #{inspect(src_bucket)}
      src_object: #{inspect(src_object)}
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

    defp sandbox_upload_part_response(
           bucket,
           object,
           upload_id,
           part_number,
           body,
           opts
         ) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_part/8 outside of test.

      bucket: #{inspect(bucket)},
      object: #{inspect(object)},
      upload_id: #{inspect(upload_id)},
      part_number: #{inspect(part_number)},
      body: #{inspect(body)},
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

    defp sandbox_copy_object_multipart_response(
           dest_bucket,
           dest_object,
           src_bucket,
           src_object,
           opts
         ) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_object_multipart/5 outside of test.

      dest_bucket: #{inspect(dest_bucket)}
      dest_object: #{inspect(dest_object)}
      src_bucket: #{inspect(src_bucket)}
      src_object: #{inspect(src_object)}
      options: #{inspect(opts, pretty: true)}
      """
    end

    defp sandbox_copy_parts_response(
           dest_bucket,
           dest_object,
           src_bucket,
           src_object,
           upload_id,
           content_length,
           opts
         ) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_parts/7 outside of test.

      dest_bucket: #{inspect(dest_bucket)}
      dest_object: #{inspect(dest_object)}
      src_bucket: #{inspect(src_bucket)}
      src_object: #{inspect(src_object)}
        upload_id: #{inspect(upload_id)}
      content_length: #{inspect(content_length)}
      options: #{inspect(opts, pretty: true)}
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

    defp sandbox_abort_multipart_upload_response(bucket, object, upload_id, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.abort_multipart_upload/4 outside of test.

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
  end
end
