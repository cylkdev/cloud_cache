defmodule CloudCache.Adapters.S3 do
  @moduledoc """
  The S3 adapter for CloudCache.

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

  ## Usage

      CloudCache.Adapters.S3.start_link()
  """
  use Supervisor

  alias ExAws.{Operation, S3}
  alias CloudCache.Config
  alias CloudCache.Adapters.S3.Multipart

  @behaviour CloudCache.Adapter

  @logger_prefix "CloudCache.Adapters.S3"

  @http_methods ~w(get post put patch delete head options trace connect)a

  @default_name __MODULE__
  @default_finch_name CloudCache.Adapters.S3.Finch
  @default_finch_opts [
    name: @default_finch_name,
    pools: %{
      default: [
        size: 32,
        count: 8,
        pool_max_idle_time: 120_000,
        conn_max_idle_time: 60_000,
        conn_opts: [
          protocols: [:http1],
          transport_opts: [
            timeout: 20_000,
            keepalive: true
          ]
        ]
      ]
    }
  ]

  @localstack_endpoint_options [
    scheme: "http://",
    host: "s3.localhost.localstack.cloud",
    port: 4566,
    access_key_id: "test",
    secret_access_key: "test",
    retries: [
      max_attempts: 1,
      base_backoff_in_ms: 10,
      max_backoff_in_ms: 10_000
    ]
  ]

  # 64 MiB (67_108_864 bytes)
  @sixty_four_mib 64 * 1_024 * 1_024
  # 1 GiB (1_073_741_824 bytes)
  @one_gib 1 * 1_024 * 1_024 * 1_024
  @one_minute_seconds 60
  @default_s3_options [
    sandbox_enabled: false,
    http_client: CloudCache.Adapters.S3.HTTP,
    http_opts: [finch: @default_finch_name],
    access_key_id: "<CLOUD_CACHE_ADAPTERS_S3_ACCESS_KEY_ID>",
    secret_access_key: "<CLOUD_CACHE_ADAPTERS_S3_SECRET_ACCESS_KEY>",
    retries: [
      max_attempts: 3,
      base_backoff_in_ms: 10,
      max_backoff_in_ms: 10_000
    ]
  ]

  @default_options [
    s3: @default_s3_options
  ]

  @doc """
  Starts the S3 adapter.

  ### Examples

      iex> CloudCache.Adapters.S3.start_link()
  """
  def start_link(name \\ @default_name, opts \\ []) do
    Supervisor.start_link(__MODULE__, Keyword.put(opts, :name, name))
  end

  def child_spec({@default_name, opts}) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [@default_name, opts]},
      restart: Keyword.get(opts, :restart, :permanent),
      shutdown: Keyword.get(opts, :shutdown, 5000),
      type: :supervisor
    }
  end

  def child_spec({name, opts}) do
    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [name, opts]},
      restart: Keyword.get(opts, :restart, :permanent),
      shutdown: Keyword.get(opts, :shutdown, 5000),
      type: :supervisor
    }
  end

  def child_spec(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    child_spec({name || @default_name, opts})
  end

  @impl true
  def init(opts) do
    children = [
      {Finch, Keyword.merge(@default_finch_opts, opts[:finch] || [])}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @impl true
  @doc """
  Returns a list of all buckets.

  ### Examples

      iex> CloudCache.Adapters.S3.list_buckets()
  """
  def list_buckets(opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case opts
           |> Keyword.take([:host, :port, :region, :scheme, :headers, :timeout])
           |> S3.list_buckets()
           |> perform(opts) do
        {:ok, %{body: body}} ->
          {:ok, body.buckets}

        {:error, %{status: status} = response} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("buckets not found", %{
             function: :list_buckets,
             response: response
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             function: :list_buckets,
             reason: reason
           })}
      end
    else
      sandbox_list_buckets_response(opts)
    end
  end

  @impl true
  @doc """
  Creates a new bucket in the specified region.

  ### Examples

      iex> CloudCache.Adapters.S3.create_bucket("test-bucket", "us-west-1")
  """
  def create_bucket(bucket, region, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket |> S3.put_bucket(region, opts) |> perform(opts) do
        {:ok, %{headers: headers}} ->
          {:ok, headers}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             function: :create_bucket,
             bucket: bucket,
             region: region,
             reason: reason
           })}
      end
    else
      sandbox_create_bucket_response(bucket, region, opts)
    end
  end

  @impl true
  @doc """
  Returns the metadata of an object.

  ### Examples

      iex> CloudCache.Adapters.S3.head_object("test-bucket", "test-object")
  """
  def head_object(bucket, key, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket |> S3.head_object(key, opts) |> perform(opts) do
        {:ok, %{headers: headers}} ->
          {:ok, headers}

        {:error, %{status: status}} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("object not found", %{
             bucket: bucket,
             key: key
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             bucket: bucket,
             key: key,
             reason: reason
           })}
      end
    else
      sandbox_head_object_response(bucket, key, opts)
    end
  end

  @impl true
  @doc """
  Returns the content of an object.

  ### Examples

      iex> CloudCache.Adapters.S3.delete_object("test-bucket", "test-object")
  """
  def delete_object(bucket, key, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket
           |> S3.delete_object(key, opts)
           |> perform(opts) do
        {:ok, %{body: body}} ->
          {:ok, body}

        {:error, %{status: status} = reason} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("object not found", %{
             function: :delete_object,
             bucket: bucket,
             key: key,
             reason: reason
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             function: :delete_object,
             bucket: bucket,
             key: key,
             reason: reason
           })}
      end
    else
      sandbox_delete_object_response(bucket, key, opts)
    end
  end

  @impl true
  @doc """
  Returns the content of an object.

  ### Examples

      iex> CloudCache.Adapters.S3.get_object("test-bucket", "test-object")
  """
  def get_object(bucket, key, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket
           |> S3.get_object(key, opts)
           |> perform(opts) do
        {:ok, %{body: body}} ->
          {:ok, body}

        {:error, %{status: status} = reason} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("bucket not found", %{
             function: :get_object,
             bucket: bucket,
             key: key,
             reason: reason
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             function: :get_object,
             bucket: bucket,
             key: key,
             reason: reason
           })}
      end
    else
      sandbox_get_object_response(bucket, key, opts)
    end
  end

  @impl true
  @doc """
  Uploads an object to a bucket.

  ### Examples

      iex> CloudCache.Adapters.S3.put_object("test-bucket", "test-object", "test-body")
  """
  def put_object(bucket, key, body, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket
           |> S3.put_object(key, body, opts)
           |> perform(opts) do
        {:ok, %{headers: headers}} ->
          {:ok, headers}

        {:error, %{status: status} = reason} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("bucket not found", %{
             function: :put_object,
             bucket: bucket,
             key: key,
             body: body,
             reason: reason
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             function: :put_object,
             bucket: bucket,
             key: key,
             body: body,
             reason: reason
           })}
      end
    else
      sandbox_put_object_response(bucket, key, body, opts)
    end
  end

  @impl true
  @doc """
  Returns a list of objects in a bucket.

  ### Examples

      iex> CloudCache.Adapters.S3.list_objects("test-bucket")
  """
  def list_objects(bucket, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket |> S3.list_objects_v2(opts) |> perform(opts) do
        {:ok, %{body: %{contents: contents}} = res} ->
          IO.inspect(res, label: "res")
          {:ok, contents}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             function: :list_objects,
             bucket: bucket,
             reason: reason
           })}
      end
    else
      sandbox_list_objects_response(bucket, opts)
    end
  end

  @impl true
  @doc """
  Copies an object from one bucket to another.

  ### Examples

      iex> CloudCache.Adapters.S3.copy_object("test-bucket", "test-object", "test-bucket", "test-object")
  """
  def copy_object(dest_bucket, dest_key, src_bucket, src_key, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case dest_bucket
           |> S3.put_object_copy(dest_key, src_bucket, src_key, opts)
           |> perform(opts) do
        {:ok, %{body: body}} ->
          {:ok, body}

        {:error, %{status: status}} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("object not found", %{
             function: :copy_object,
             dest_bucket: dest_bucket,
             dest_key: dest_key,
             src_bucket: src_bucket,
             src_key: src_key
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             function: :copy_object,
             dest_bucket: dest_bucket,
             dest_key: dest_key,
             src_bucket: src_bucket,
             src_key: src_key,
             reason: reason
           })}
      end
    else
      sandbox_copy_object_response(dest_bucket, dest_key, src_bucket, src_key, opts)
    end
  end

  @impl true
  @doc """
  Returns a presigned URL for an object.

  ### Examples

      iex> CloudCache.Adapters.S3.presign("test-bucket", :post, "test-object")
  """
  def presign(bucket, http_method, key, opts \\ []) when http_method in @http_methods do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      expires_in = opts[:expires_in] || @one_minute_seconds
      opts = Keyword.put(opts, :expires_in, expires_in)

      case opts
           |> Keyword.get(:s3, [])
           |> config()
           |> ExAws.S3.presigned_url(http_method, bucket, key, opts) do
        {:ok, url} ->
          %{
            key: key,
            url: url,
            expires_in: expires_in,
            expires_at: DateTime.add(DateTime.utc_now(), expires_in, :second)
          }

        {:error, reason} ->
          raise "Failed to generate presigned URL for object: #{inspect(reason)}"
      end
    else
      sandbox_presign_response(bucket, http_method, key, opts)
    end
  end

  @impl true
  @doc """
  Returns a presigned URL for an object.

  ### Examples

      iex> CloudCache.Adapters.S3.presign_post("test-bucket", "test-object")
  """
  def presign_post(bucket, key, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      expires_in = opts[:expires_in] || @one_minute_seconds
      min_size = Keyword.get(opts, :min_size, 0)
      max_size = Keyword.get(opts, :max_size, @one_gib)
      virtual_host? = Keyword.get(opts, :virtual_host, false)
      s3_accelerate? = Keyword.get(opts, :s3_accelerate, false)
      bucket_as_host? = Keyword.get(opts, :bucket_as_host, false)

      content_type_conditions =
        case Keyword.get(opts, :content_type) do
          nil -> []
          content_type -> ["starts-with", "$Content-Type", content_type]
        end

      opts
      |> Keyword.get(:s3, [])
      |> config()
      |> ExAws.S3.presigned_post(bucket, key,
        expires_in: Keyword.get(opts, :expires_in, expires_in),
        content_length_range: [min_size, max_size],
        acl: Keyword.get(opts, :acl_conditions),
        key: Keyword.get(opts, :key_conditions),
        custom_conditions: [content_type_conditions],
        virtual_host: virtual_host?,
        s3_accelerate: s3_accelerate?,
        bucket_as_host: bucket_as_host?
      )
      |> then(fn %{fields: fields, url: url} ->
        fields2 =
          Map.new(fields, fn {k, v} ->
            k2 =
              k
              |> ProperCase.snake_case()
              |> String.replace("-", "")
              |> String.to_atom()

            {k2, v}
          end)

        {:ok,
         %{
           fields: fields2,
           url: url,
           expires_in: expires_in,
           expires_at: DateTime.add(DateTime.utc_now(), expires_in, :second)
         }}
      end)
    else
      sandbox_presign_post_response(bucket, key, opts)
    end
  end

  @impl true
  @doc """
  Returns a presigned URL for a part of an object.

  ### Examples

      iex> CloudCache.Adapters.S3.presign_part("test-bucket", "test-object", "test-upload-id", 1)
  """
  def presign_part(bucket, object, upload_id, part_number, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      query_params = %{"uploadId" => upload_id, "partNumber" => part_number}
      opts = Keyword.update(opts, :query_params, query_params, &Map.merge(&1, query_params))
      presign(bucket, :put, object, opts)
    else
      sandbox_presign_part_response(bucket, object, upload_id, part_number, opts)
    end
  end

  @impl true
  @doc """
  Returns a list of parts for a multipart upload.

  ### Examples

      iex> CloudCache.Adapters.S3.list_parts("test-bucket", "test-object", "test-upload-id")
  """
  def list_parts(bucket, key, upload_id, opts \\ []) do
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
      |> S3.list_parts(key, upload_id, list_parts_opts)
      |> perform(opts)
      |> then(fn
        {:ok, %{body: %{parts: parts}}} ->
          {:ok, parts}

        {:error, %{status: status}} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("object not found", %{
             function: :list_parts,
             bucket: bucket,
             key: key,
             upload_id: upload_id
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             function: :list_parts,
             bucket: bucket,
             key: key,
             upload_id: upload_id,
             reason: reason
           })}
      end)
    else
      sandbox_list_parts_response(bucket, key, upload_id, opts)
    end
  end

  @impl true
  @doc """
  Uploads a part of a multipart upload.

  ### Examples

      iex> CloudCache.Adapters.S3.upload_part("test-bucket", "test-object", "test-upload-id", 1, "test-body")
  """
  def upload_part(bucket, key, upload_id, part_number, body, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      bucket
      |> S3.upload_part(key, upload_id, part_number, body, opts)
      |> perform(opts)
      |> then(fn
        {:ok, %{headers: headers}} ->
          {:ok, headers}

        {:error, %{status: status}} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("object not found", %{
             function: :upload_part,
             bucket: bucket,
             key: key,
             upload_id: upload_id,
             part_number: part_number,
             body: body
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             function: :upload_part,
             bucket: bucket,
             key: key,
             upload_id: upload_id,
             part_number: part_number,
             body: body,
             reason: reason
           })}
      end)
    else
      sandbox_upload_part_response(bucket, key, upload_id, part_number, body, opts)
    end
  end

  @impl true
  @doc """
  Copies an object from one bucket to another using a multipart upload.

  ### Examples

      iex> CloudCache.Adapters.S3.copy_object_multipart("test-bucket", "test-object", "test-bucket", "test-object")
  """
  def copy_object_multipart(dest_bucket, dest_key, src_bucket, src_key, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      with {:ok, info} <- head_object(src_bucket, src_key, opts),
           {:ok, mpu} <- create_multipart_upload(dest_bucket, dest_key, opts),
           {:ok, parts} <-
             copy_parts(
               dest_bucket,
               dest_key,
               src_bucket,
               src_key,
               mpu.upload_id,
               info.content_length,
               opts
             ) do
        complete_multipart_upload(
          dest_bucket,
          dest_key,
          mpu.upload_id,
          parts,
          opts
        )
      end
    else
      sandbox_copy_object_multipart_response(
        dest_bucket,
        dest_key,
        src_bucket,
        src_key,
        opts
      )
    end
  end

  @impl true
  @doc """
  Copies parts of an object from one bucket to another using a multipart upload.

  ### Examples

      iex> CloudCache.Adapters.S3.copy_parts("test-bucket", "test-object", "test-bucket", "test-object", "test-upload-id", 1)
  """
  def copy_parts(
        dest_bucket,
        dest_key,
        src_bucket,
        src_key,
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
          copy_part_range(
            dest_bucket,
            dest_key,
            src_bucket,
            src_key,
            upload_id,
            part_num,
            {start_byte, end_byte, content_length},
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
            |> Enum.sort(fn {_, n1}, {_, n2} -> n1 < n2 end)
            |> Enum.map(fn {%{etag: etag}, n} -> {n, etag} end)
          }

        {:error, reasons} ->
          {
            :error,
            reasons
            |> Enum.sort(fn {_, n1}, {_, n2} -> n1 < n2 end)
            |> Enum.map(fn {term, _} -> term end)
          }
      end)
    else
      sandbox_copy_parts_response(
        dest_bucket,
        dest_key,
        src_bucket,
        src_key,
        upload_id,
        content_length,
        opts
      )
    end
  end

  defp copy_part_range(
         dest_bucket,
         dest_key,
         src_bucket,
         src_key,
         upload_id,
         part_num,
         {start_byte, end_byte, content_length},
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
      dest_key: #{inspect(dest_key)}
      src_bucket: #{inspect(src_bucket)}
      src_key: #{inspect(src_key)}
      upload_id: #{inspect(upload_id)}
      content_length: #{inspect(content_length)}
      """
    )

    case copy_part(
           dest_bucket,
           dest_key,
           src_bucket,
           src_key,
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
        dest_key,
        src_bucket,
        src_key,
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
        dest_key,
        src_bucket,
        src_key,
        upload_id,
        part_number,
        src_range,
        opts
      )
      |> perform(opts)
      |> then(fn
        {:ok, %{body: body}} ->
          {:ok, body}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             dest_bucket: dest_bucket,
             dest_key: dest_key,
             src_bucket: src_bucket,
             src_key: src_key,
             src_range: src_range,
             upload_id: upload_id,
             part_number: part_number,
             reason: reason
           })}
      end)
    else
      sandbox_copy_part_response(
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

  @impl true
  def complete_multipart_upload(bucket, key, upload_id, parts, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      with :ok <- validate_multipart_size(bucket, key, upload_id, opts) do
        bucket
        |> S3.complete_multipart_upload(key, upload_id, validate_parts!(parts))
        |> perform(opts)
        |> then(fn
          {:ok, %{body: body}} ->
            with :ok <- validate_multipart_content_type(bucket, key, upload_id, opts) do
              {:ok, body}
            end

          {:error, %{status: status}} when status in 400..499 ->
            {:error,
             ErrorMessage.not_found("multipart upload not found.", %{
               bucket: bucket,
               key: key,
               upload_id: upload_id
             })}

          {:error, reason} ->
            {:error,
             ErrorMessage.service_unavailable("service temporarily unavailable", %{
               bucket: bucket,
               key: key,
               upload_id: upload_id,
               parts: parts,
               reason: reason
             })}
        end)
      end
    else
      sandbox_complete_multipart_upload_response(bucket, key, upload_id, parts, opts)
    end
  end

  defp validate_multipart_size(bucket, key, upload_id, opts) do
    case Keyword.get(opts, :max_size, @one_gib) do
      :infinity ->
        :ok

      false ->
        :ok

      nil ->
        :ok

      max ->
        with {:ok, size} <- aggregate_object_size(bucket, key, upload_id, opts) do
          if size > max do
            {:error,
             ErrorMessage.forbidden("multipart upload size exceeds maximum allowed size", %{
               bucket: bucket,
               key: key,
               upload_id: upload_id,
               max_size: max
             })}
          else
            :ok
          end
        end
    end
  end

  defp aggregate_object_size(bucket, key, upload_id, opts) do
    aggregate_object_size(bucket, key, upload_id, nil, 0, opts)
  end

  defp aggregate_object_size(bucket, key, upload_id, part_number_marker, acc, opts) do
    response =
      case part_number_marker do
        nil ->
          list_parts(bucket, key, upload_id, opts)

        marker ->
          list_parts(bucket, key, upload_id, Keyword.put(opts, :part_number_marker, marker))
      end

    case response do
      {:ok, %{body: %{parts: parts} = body}} ->
        size = Enum.reduce(parts, 0, fn p, sum -> sum + (p.size || 0) end)
        acc2 = acc + size

        if body.is_truncated do
          aggregate_object_size(
            bucket,
            key,
            upload_id,
            body.next_part_number_marker,
            acc2,
            opts
          )
        else
          {:ok, acc2}
        end

      {:error, _} = error ->
        error
    end
  end

  defp validate_multipart_content_type(bucket, key, upload_id, opts) do
    case Keyword.get(opts, :content_type) do
      nil ->
        :ok

      :any ->
        :ok

      content_type ->
        with {:ok, meta} <- head_object(bucket, key, opts) do
          if content_type_match?(content_type, meta.content_type) do
            :ok
          else
            error =
              {:error,
               ErrorMessage.forbidden("content type mismatch", %{
                 bucket: bucket,
                 key: key,
                 upload_id: upload_id,
                 content_type: content_type
               })}

            case Keyword.get(opts, :on_content_type_mismatch, :delete) do
              :delete -> with {:ok, _} <- delete_object(bucket, key, opts), do: error
              :error -> error
            end
          end
        end
    end
  end

  defp content_type_match?(regex, pattern) when is_struct(regex, Regex) do
    Regex.match?(regex, pattern)
  end

  defp content_type_match?(str, pattern) when is_binary(str) do
    str =~ pattern
  end

  @impl true
  def abort_multipart_upload(bucket, key, upload_id, opts \\ []) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      bucket
      |> S3.abort_multipart_upload(key, upload_id)
      |> perform(opts)
      |> then(fn
        {:ok, %{headers: headers}} ->
          {:ok, headers}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             bucket: bucket,
             key: key,
             upload_id: upload_id,
             reason: reason
           })}
      end)
    else
      sandbox_abort_multipart_upload_response(bucket, key, upload_id, opts)
    end
  end

  @impl true
  def create_multipart_upload(bucket, key, opts) do
    opts = Keyword.merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    one_min_from_now = DateTime.add(DateTime.utc_now(), 1, :minute)
    expiry = to_http_date(one_min_from_now)

    opts =
      Keyword.update(opts, :expires, expiry, fn
        nil -> expiry
        %DateTime{} = datetime -> to_http_date(datetime)
        expires -> expires
      end)

    if not sandbox? or sandbox_disabled?() do
      bucket
      |> S3.initiate_multipart_upload(key, opts)
      |> perform(opts)
      |> then(fn
        {:ok, %{body: body}} ->
          {:ok, body}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             bucket: bucket,
             key: key,
             reason: reason
           })}
      end)
    else
      sandbox_create_multipart_upload_response(bucket, key, opts)
    end
  end

  # Helper API

  defp to_http_date(datetime) do
    datetime
    |> DateTime.to_unix(:second)
    |> DateTime.from_unix!()
    |> Calendar.strftime("%a, %d %b %Y %H:%M:%S GMT")
  end

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
    with {:ok, payload} <- Operation.perform(op, config(opts[:s3] || [])) do
      {:ok, deserialize(payload)}
    end
  end

  @doc false
  def config(opts \\ []) do
    adapter_config = Config.get_env(CloudCache.Adapters.S3, [])

    opts =
      @default_s3_options
      |> Keyword.merge(adapter_config)
      |> Keyword.merge(opts)

    opts =
      if (opts[:localstack] || adapter_config[:localstack]) === true do
        Keyword.merge(opts, @localstack_endpoint_options)
      else
        opts
      end

    opts =
      if Keyword.has_key?(opts, :profile) do
        profile = Keyword.fetch!(opts, :profile)

        case ExAws.CredentialsIni.File.security_credentials(profile) do
          {:ok, credentials} ->
            region = credentials[:region]
            access_key_id = credentials[:access_key_id]
            secret_access_key = credentials[:secret_access_key]

            if is_nil(access_key_id) do
              CloudCache.Logger.warning(
                @logger_prefix,
                "Access key ID is missing for profile #{profile}, got: #{inspect(credentials)}"
              )
            end

            if is_nil(secret_access_key) do
              CloudCache.Logger.warning(
                @logger_prefix,
                "Secret access key is missing for profile #{profile}, got: #{inspect(credentials)}"
              )
            end

            opts =
              if is_binary(access_key_id) and is_binary(secret_access_key) do
                opts
                |> Keyword.put(:access_key_id, access_key_id)
                |> Keyword.put(:secret_access_key, secret_access_key)
              else
                opts
              end

            opts =
              if is_binary(region) do
                Keyword.put(opts, :region, region)
              else
                opts
              end

            opts

          {:error, reason} ->
            raise "Failed to fetch credentials for profile: #{profile}, reason: #{inspect(reason)}"
        end
      else
        opts
      end

    ExAws.Config.new(:s3, opts)
  end

  defp atomize_key(key) do
    key
    |> String.downcase()
    |> String.replace(["-", " "], "_")
    |> ProperCase.snake_case()
    |> String.to_atom()
  end

  defp normalize_key(:e_tag), do: :etag
  defp normalize_key("etag"), do: :etag
  defp normalize_key("e_tag"), do: :etag
  defp normalize_key(key), do: key

  defp deserialize(values) when is_list(values) do
    Enum.map(values, &deserialize/1)
  end

  defp deserialize({key, val}) do
    final_key = deserialize_key(key)
    {final_key, final_key |> deserialize_value(val) |> deserialize()}
  end

  defp deserialize(payload) when is_map(payload) and not is_struct(payload) do
    Map.new(payload, fn {key, val} -> deserialize({key, val}) end)
  end

  defp deserialize(val) do
    val
  end

  defp deserialize_key(key) do
    case normalize_key(key) do
      k when is_atom(k) -> k
      k when is_binary(k) -> atomize_key(k)
    end
  end

  defp deserialize_value(key, vals) when is_list(vals) do
    Enum.map(vals, &deserialize_value(key, &1))
  end

  defp deserialize_value(key, val) when key in [:content_length, :part_number, :size] do
    val |> remove_quotes() |> String.to_integer()
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
      case val |> remove_quotes() |> parse_iso8601_datetime() do
        {:ok, dt} ->
          dt

        :error ->
          raise ArgumentError,
                "Expected value for field `#{inspect(key)}` " <>
                  "to be an RFC timestamp, got: #{inspect(val)}"
      end
    else
      case val |> remove_quotes() |> parse_rfc_datetime() do
        {:ok, datetime} ->
          datetime

        :error ->
          raise ArgumentError,
                "Expected value for field `#{inspect(key)}` " <>
                  "to be an RFC timestamp, got: #{inspect(val)}"
      end
    end
  end

  defp deserialize_value(_, val) do
    if is_binary(val) do
      remove_quotes(val)
    else
      val
    end
  end

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

  # Sandbox API

  if Code.ensure_loaded?(SandboxRegistry) do
    defdelegate sandbox_disabled?, to: CloudCache.Adapters.S3.Sandbox

    defdelegate sandbox_list_buckets_response(opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :list_buckets_response

    defdelegate sandbox_head_object_response(bucket, key, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :head_object_response

    defdelegate sandbox_delete_object_response(bucket, key, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :delete_object_response

    defdelegate sandbox_get_object_response(bucket, key, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :get_object_response

    defdelegate sandbox_put_object_response(bucket, key, body, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :put_object_response

    defdelegate sandbox_list_objects_response(bucket, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :list_objects_response

    defdelegate sandbox_create_bucket_response(bucket, region, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :create_bucket_response

    defdelegate sandbox_copy_object_response(
                  dest_bucket,
                  dest_key,
                  src_bucket,
                  src_key,
                  opts
                ),
                to: CloudCache.Adapters.S3.Sandbox,
                as: :copy_object_response

    defdelegate sandbox_presign_response(bucket, http_method, key, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :presign_response

    defdelegate sandbox_presign_part_response(
                  bucket,
                  key,
                  upload_id,
                  part_number,
                  opts
                ),
                to: CloudCache.Adapters.S3.Sandbox,
                as: :presign_part_response

    defdelegate sandbox_presign_post_response(bucket, key, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :presign_post_response

    defdelegate sandbox_list_parts_response(bucket, key, upload_id, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :list_parts_response

    defdelegate sandbox_upload_part_response(
                  bucket,
                  key,
                  upload_id,
                  part_number,
                  body,
                  opts
                ),
                to: CloudCache.Adapters.S3.Sandbox,
                as: :upload_part_response

    defdelegate sandbox_copy_object_multipart_response(
                  dest_bucket,
                  dest_key,
                  src_bucket,
                  src_key,
                  opts
                ),
                to: CloudCache.Adapters.S3.Sandbox,
                as: :copy_object_multipart_response

    defdelegate sandbox_copy_parts_response(
                  dest_bucket,
                  dest_key,
                  src_bucket,
                  src_key,
                  upload_id,
                  content_length,
                  opts
                ),
                to: CloudCache.Adapters.S3.Sandbox,
                as: :copy_parts_response

    defdelegate sandbox_copy_part_response(
                  dest_bucket,
                  dest_key,
                  src_bucket,
                  src_key,
                  upload_id,
                  part_number,
                  range,
                  opts
                ),
                to: CloudCache.Adapters.S3.Sandbox,
                as: :copy_part_response

    defdelegate sandbox_complete_multipart_upload_response(
                  bucket,
                  key,
                  upload_id,
                  parts,
                  opts
                ),
                to: CloudCache.Adapters.S3.Sandbox,
                as: :complete_multipart_upload_response

    defdelegate sandbox_abort_multipart_upload_response(bucket, key, upload_id, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :abort_multipart_upload_response

    defdelegate sandbox_create_multipart_upload_response(bucket, key, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :create_multipart_upload_response
  else
    defp sandbox_disabled?, do: true

    defp sandbox_list_buckets_response(opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.list_buckets/1 outside of test.

      options: #{inspect(opts)}
      """
    end

    defp sandbox_create_bucket_response(bucket, region, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.create_bucket/3 outside of test.

      bucket: #{inspect(bucket)}
      region: #{inspect(region)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_head_object_response(bucket, key, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.head_object/3 outside of test.

      bucket: #{inspect(bucket)}
      key: #{inspect(key)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_delete_object_response(bucket, key, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.delete_object/3 outside of test.

      bucket: #{inspect(bucket)}
      key: #{inspect(key)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_get_object_response(bucket, key, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_object/5 outside of test.

      bucket: #{inspect(bucket)}
      key: #{inspect(key)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_put_object_response(bucket, key, body, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_object/5 outside of test.

      bucket: #{inspect(bucket)}
      key: #{inspect(key)}
      body: #{inspect(body)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_list_objects_response(bucket, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.list_objects/2 outside of test.

      bucket: #{inspect(bucket)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_copy_object_response(dest_bucket, dest_key, src_bucket, src_key, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_object/5 outside of test.

      dest_bucket: #{inspect(dest_bucket)}
      dest_key: #{inspect(dest_key)}
      src_bucket: #{inspect(src_bucket)}
      src_key: #{inspect(src_key)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_presign_response(bucket, http_method, key, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.presign/3 outside of test.

      bucket: #{inspect(bucket)}
      http_method: #{inspect(http_method)}
      key: #{inspect(key)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_presign_post_response(bucket, key, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.presign_post/3 outside of test.

      bucket: #{inspect(bucket)}
      key: #{inspect(key)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_presign_part_response(bucket, key, upload_id, part_number, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.presign_part/5 outside of test.

      bucket: #{inspect(bucket)}
      key: #{inspect(key)}
      upload_id: #{inspect(upload_id)}
      part_number: #{inspect(part_number)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_list_parts_response(bucket, key, upload_id, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.list_parts/4 outside of test.

      bucket: #{inspect(bucket)}
      key: #{inspect(key)}
      upload_id: #{inspect(upload_id)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_upload_part_response(
           bucket,
           key,
           upload_id,
           part_number,
           body,
           opts
         ) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_part/8 outside of test.

      bucket: #{inspect(bucket)},
      key: #{inspect(key)},
      upload_id: #{inspect(upload_id)},
      part_number: #{inspect(part_number)},
      body: #{inspect(body)},
      options: #{inspect(opts)}
      """
    end

    defp sandbox_copy_object_multipart_response(
           dest_bucket,
           dest_key,
           src_bucket,
           src_key,
           opts
         ) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_object_multipart/5 outside of test.

      dest_bucket: #{inspect(dest_bucket)}
      dest_key: #{inspect(dest_key)}
      src_bucket: #{inspect(src_bucket)}
      src_key: #{inspect(src_key)}
      options: #{inspect(opts, pretty: true)}
      """
    end

    defp sandbox_copy_parts_response(
           dest_bucket,
           dest_key,
           src_bucket,
           src_key,
           upload_id,
           content_length,
           opts
         ) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_parts/7 outside of test.

      dest_bucket: #{inspect(dest_bucket)}
      dest_key: #{inspect(dest_key)}
      src_bucket: #{inspect(src_bucket)}
      src_key: #{inspect(src_key)}
        upload_id: #{inspect(upload_id)}
      content_length: #{inspect(content_length)}
      options: #{inspect(opts, pretty: true)}
      """
    end

    defp sandbox_copy_part_response(
           dest_bucket,
           dest_key,
           src_bucket,
           src_key,
           upload_id,
           part_number,
           range,
           opts
         ) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_part/8 outside of test.

      dest_bucket: #{inspect(dest_bucket)}
      dest_key: #{inspect(dest_key)}
      src_bucket: #{inspect(src_bucket)}
      src_key: #{inspect(src_key)}
      upload_id: #{inspect(upload_id)}
      part_number: #{inspect(part_number)}
      range: #{inspect(range)}
      options: #{inspect(opts, pretty: true)}
      """
    end

    defp sandbox_complete_multipart_upload_response(bucket, key, upload_id, parts, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.complete_multipart_upload/5 outside of test.

      bucket: #{inspect(bucket)}
      key: #{inspect(key)}
      upload_id: #{inspect(upload_id)}
      parts: #{inspect(parts)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_abort_multipart_upload_response(bucket, key, upload_id, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.abort_multipart_upload/4 outside of test.

      bucket: #{inspect(bucket)}
      key: #{inspect(key)}
      upload_id: #{inspect(upload_id)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_create_multipart_upload_response(bucket, key, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)} outside of test.

      bucket: #{inspect(bucket)}
      key: #{inspect(key)}
      options: #{inspect(opts)}
      """
    end
  end
end
