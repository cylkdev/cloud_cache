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
  alias CloudCache.{Config, Utils}
  alias CloudCache.Adapters.S3.Multipart

  @behaviour CloudCache.Adapter

  @logger_prefix "CloudCache.Adapters.S3"

  @default_name __MODULE__
  @default_finch_name CloudCache.Adapters.S3.Finch
  @default_finch_opts [
    name: @default_finch_name,
    pools: %{
      default: [
        # max connections per pool
        size: 32,
        # number of pools (shards)
        count: 8,
        # clean up idle per-host pools and connections pool terminates if unused for 2m
        pool_max_idle_time: 120_000,
        # drop idle HTTP/1 sockets at 60s
        conn_max_idle_time: 60_000,
        # S3 is HTTP/1 from client perspective
        conn_opts: [
          protocols: [:http1],
          transport_opts: [
            # connect/TLS handshake timeout
            timeout: 20_000,
            keepalive: true
          ]
        ]
      ]
    }
  ]

  @local_stack_options [
    scheme: "http://",
    host: "s3.localhost.localstack.cloud",
    port: 4566,
    access_key_id: "test",
    secret_access_key: "test",
    retries: [max_attempts: 1]
  ]

  # 64 MiB (67_108_864 bytes)
  @sixty_four_mib 64 * 1_024 * 1_024
  @one_minute_seconds 60
  @default_s3_options [
    sandbox_enabled: false,
    http_client: CloudCache.Adapters.S3.HTTP,
    http_opts: [finch: @default_finch_name],
    retries: [
      max_attempts: 1,
      base_backoff_in_ms: 10,
      max_backoff_in_ms: 10_000
    ],
    access_key_id: [
      {:awscli, System.get_env("AWS_PROFILE", "cloud_cache"), 30},
      {:awscli, System.get_env("AWS_PROFILE", "default"), 30},
      {:system, "AWS_ACCESS_KEY_ID"},
      :instance_role,
      "<AWS_ACCESS_KEY_ID>"
    ],
    secret_access_key: [
      {:awscli, System.get_env("AWS_PROFILE", "cloud_cache"), 30},
      {:awscli, System.get_env("AWS_PROFILE", "default"), 30},
      {:system, "AWS_SECRET_ACCESS_KEY"},
      :instance_role,
      "<AWS_SECRET_ACCESS_KEY>"
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

  @doc """
  Returns the S3 configuration as a map.

  ### Examples

      iex> CloudCache.Adapters.S3.config()
  """
  def config(opts \\ []) do
    overrides =
      @default_s3_options
      |> Utils.deep_merge(Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

    overrides =
      if overrides[:local_stack] === true do
        Keyword.merge(overrides, @local_stack_options)
      else
        overrides
      end

    final_overrides =
      case overrides[:profile] do
        nil ->
          overrides

        profile ->
          overrides
          |> Keyword.update!(:access_key_id, fn val ->
            entries =
              val
              |> List.wrap()
              |> Enum.reject(fn
                {:awscli, ^profile, _} -> true
                _ -> false
              end)

            [{:awscli, profile, 30} | entries]
          end)
          |> Keyword.update!(:secret_access_key, fn val ->
            entries =
              val
              |> List.wrap()
              |> Enum.reject(fn
                {:awscli, ^profile, _} -> true
                _ -> false
              end)

            [{:awscli, profile, 30} | entries]
          end)
      end

    ExAws.Config.new(:s3, final_overrides)
  end

  @impl true
  @doc """
  Returns a list of all buckets.

  ### Examples

      iex> CloudCache.Adapters.S3.list_buckets()
  """
  def list_buckets(opts \\ []) do
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case opts
           |> Keyword.take([:host, :port, :region, :scheme, :headers, :timeout])
           |> S3.list_buckets()
           |> perform(opts[:s3] || []) do
        {:ok, %{body: body}} ->
          {:ok, body.buckets}

        {:error, %{status: status} = response} when status in 400..499 ->
          {:error, ErrorMessage.not_found("buckets not found", %{response: response})}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{reason: reason})}
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
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket |> S3.put_bucket(region, opts[:s3] || []) |> perform(opts[:s3] || []) do
        {:ok, %{headers: headers}} ->
          {:ok, headers}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
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
  def head_object(bucket, object, opts \\ []) do
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket |> S3.head_object(object, opts) |> perform(opts[:s3] || []) do
        {:ok, %{headers: headers}} ->
          {:ok, headers}

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
  Returns the content of an object.

  ### Examples

      iex> CloudCache.Adapters.S3.delete_object("test-bucket", "test-object")
  """
  def delete_object(bucket, object, opts \\ []) do
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket
           |> S3.delete_object(object, opts)
           |> perform(opts[:s3] || []) do
        {:ok, %{body: body}} ->
          {:ok, body}

        {:error, %{status: status} = reason} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("object not found", %{
             bucket: bucket,
             object: object,
             reason: reason
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             bucket: bucket,
             object: object,
             reason: reason
           })}
      end
    else
      sandbox_delete_object_response(bucket, object, opts)
    end
  end

  @impl true
  @doc """
  Returns the content of an object.

  ### Examples

      iex> CloudCache.Adapters.S3.get_object("test-bucket", "test-object")
  """
  def get_object(bucket, object, opts \\ []) do
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket
           |> S3.get_object(object, opts)
           |> perform(opts[:s3] || []) do
        {:ok, %{body: body}} ->
          {:ok, body}

        {:error, %{status: status} = reason} when status in 400..499 ->
          {:error,
           ErrorMessage.not_found("bucket not found", %{
             bucket: bucket,
             object: object,
             reason: reason
           })}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
             bucket: bucket,
             object: object,
             reason: reason
           })}
      end
    else
      sandbox_get_object_response(bucket, object, opts)
    end
  end

  @impl true
  @doc """
  Uploads an object to a bucket.

  ### Examples

      iex> CloudCache.Adapters.S3.put_object("test-bucket", "test-object", "test-body")
  """
  def put_object(bucket, object, body, opts \\ []) do
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket
           |> S3.put_object(object, body, opts)
           |> perform(opts[:s3] || []) do
        {:ok, %{headers: headers}} ->
          {:ok, headers}

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
  Returns a list of objects in a bucket.

  ### Examples

      iex> CloudCache.Adapters.S3.list_objects("test-bucket")
  """
  def list_objects(bucket, opts \\ []) do
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case bucket |> S3.list_objects_v2(opts) |> perform(opts[:s3] || []) do
        {:ok, %{body: %{contents: contents}} = res} ->
          IO.inspect(res, label: "res")
          {:ok, contents}

        {:error, reason} ->
          {:error,
           ErrorMessage.service_unavailable("service temporarily unavailable", %{
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
  def copy_object(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      case dest_bucket
           |> S3.put_object_copy(dest_object, src_bucket, src_object, opts)
           |> perform(opts[:s3] || []) do
        {:ok, %{body: body}} ->
          {:ok, body}

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
  Returns a presigned URL for an object.

  ### Examples

      iex> CloudCache.Adapters.S3.pre_sign("test-bucket", "test-object")
  """
  def pre_sign(bucket, object, opts \\ []) do
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      http_method = opts[:http_method] || :put

      expires_in = opts[:expires_in] || @one_minute_seconds
      sign_opts = Keyword.put(opts, :expires_in, expires_in)

      case opts
           |> Keyword.get(:s3, [])
           |> config()
           |> S3.presigned_url(http_method, bucket, object, sign_opts) do
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
  Returns a presigned URL for a part of an object.

  ### Examples

      iex> CloudCache.Adapters.S3.pre_sign_part("test-bucket", "test-object", "test-upload-id", 1)
  """
  def pre_sign_part(bucket, object, upload_id, part_number, opts \\ []) do
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

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

        {:ok, _} = response ->
          response
      end
    else
      sandbox_pre_sign_part_response(bucket, object, upload_id, part_number, opts)
    end
  end

  @impl true
  @doc """
  Returns a list of parts for a multipart upload.

  ### Examples

      iex> CloudCache.Adapters.S3.list_parts("test-bucket", "test-object", "test-upload-id")
  """
  def list_parts(bucket, object, upload_id, opts \\ []) do
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

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
      |> perform(opts[:s3] || [])
      |> then(fn
        {:ok, %{body: %{parts: parts}}} ->
          {:ok, parts}

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
  Uploads a part of a multipart upload.

  ### Examples

      iex> CloudCache.Adapters.S3.upload_part("test-bucket", "test-object", "test-upload-id", 1, "test-body")
  """
  def upload_part(bucket, object, upload_id, part_number, body, opts \\ []) do
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      bucket
      |> S3.upload_part(object, upload_id, part_number, body, opts)
      |> perform(opts[:s3] || [])
      |> then(fn
        {:ok, %{headers: headers}} ->
          {:ok, headers}

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
  Copies an object from one bucket to another using a multipart upload.

  ### Examples

      iex> CloudCache.Adapters.S3.copy_object_multipart("test-bucket", "test-object", "test-bucket", "test-object")
  """
  def copy_object_multipart(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      with {:ok, info} <- head_object(src_bucket, src_object, opts),
           {:ok, mpu} <- create_multipart_upload(dest_bucket, dest_object, opts),
           {:ok, parts} <-
             copy_parts(
               dest_bucket,
               dest_object,
               src_bucket,
               src_object,
               mpu.upload_id,
               info.content_length,
               opts
             ) do
        complete_multipart_upload(
          dest_bucket,
          dest_object,
          mpu.upload_id,
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
  Copies parts of an object from one bucket to another using a multipart upload.

  ### Examples

      iex> CloudCache.Adapters.S3.copy_parts("test-bucket", "test-object", "test-bucket", "test-object", "test-upload-id", 1)
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
    opts =
      @default_options
      |> Utils.deep_merge(s3: Config.get_env(__MODULE__) || [])
      |> Utils.deep_merge(opts)

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
    opts = Utils.deep_merge(@default_options, opts)

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
      |> perform(opts[:s3] || [])
      |> then(fn
        {:ok, %{body: body}} ->
          {:ok, body}

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
  def complete_multipart_upload(bucket, object, upload_id, parts, opts \\ []) do
    opts = Utils.deep_merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      bucket
      |> S3.complete_multipart_upload(object, upload_id, validate_parts!(parts))
      |> perform(opts[:s3] || [])
      |> then(fn
        {:ok, %{body: body}} ->
          {:ok, body}

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
  def abort_multipart_upload(bucket, object, upload_id, opts \\ []) do
    opts = Utils.deep_merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      bucket
      |> S3.abort_multipart_upload(object, upload_id)
      |> perform(opts[:s3] || [])
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
  def create_multipart_upload(bucket, object, opts) do
    opts = Utils.deep_merge(@default_options, opts)

    sandbox? = opts[:s3][:sandbox_enabled] === true

    if not sandbox? or sandbox_disabled?() do
      bucket
      |> S3.initiate_multipart_upload(object, opts)
      |> perform(opts[:s3] || [])
      |> then(fn
        {:ok, %{body: body}} ->
          {:ok, body}

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
    with {:ok, payload} <- Operation.perform(op, config(opts[:s3] || [])) do
      {:ok, deserialize(payload)}
    end
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

  if Code.ensure_loaded?(SandboxRegistry) do
    defdelegate sandbox_disabled?, to: CloudCache.Adapters.S3.Sandbox

    defdelegate sandbox_list_buckets_response(opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :list_buckets_response

    defdelegate sandbox_head_object_response(bucket, object, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :head_object_response

    defdelegate sandbox_delete_object_response(bucket, object, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :delete_object_response

    defdelegate sandbox_get_object_response(bucket, object, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :get_object_response

    defdelegate sandbox_put_object_response(bucket, object, body, opts),
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
                  dest_object,
                  src_bucket,
                  src_object,
                  opts
                ),
                to: CloudCache.Adapters.S3.Sandbox,
                as: :copy_object_response

    defdelegate sandbox_pre_sign_response(bucket, object, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :pre_sign_response

    defdelegate sandbox_list_parts_response(bucket, object, upload_id, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :list_parts_response

    defdelegate sandbox_upload_part_response(
                  bucket,
                  object,
                  upload_id,
                  part_number,
                  body,
                  opts
                ),
                to: CloudCache.Adapters.S3.Sandbox,
                as: :upload_part_response

    defdelegate sandbox_pre_sign_part_response(bucket, object, upload_id, part_number, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :pre_sign_part_response

    defdelegate sandbox_copy_object_multipart_response(
                  dest_bucket,
                  dest_object,
                  src_bucket,
                  src_object,
                  opts
                ),
                to: CloudCache.Adapters.S3.Sandbox,
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
                to: CloudCache.Adapters.S3.Sandbox,
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
                to: CloudCache.Adapters.S3.Sandbox,
                as: :copy_part_response

    defdelegate sandbox_complete_multipart_upload_response(
                  bucket,
                  object,
                  upload_id,
                  parts,
                  opts
                ),
                to: CloudCache.Adapters.S3.Sandbox,
                as: :complete_multipart_upload_response

    defdelegate sandbox_abort_multipart_upload_response(bucket, object, upload_id, opts),
      to: CloudCache.Adapters.S3.Sandbox,
      as: :abort_multipart_upload_response

    defdelegate sandbox_create_multipart_upload_response(bucket, object, opts),
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

    defp sandbox_head_object_response(bucket, object, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.head_object/3 outside of test.

      bucket: #{inspect(bucket)}
      object: #{inspect(object)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_delete_object_response(bucket, object, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.delete_object/3 outside of test.

      bucket: #{inspect(bucket)}
      object: #{inspect(object)}
      options: #{inspect(opts)}
      """
    end

    defp sandbox_get_object_response(bucket, object, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.copy_object/5 outside of test.

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

    defp sandbox_list_objects_response(bucket, opts) do
      raise """
      Cannot use #{inspect(__MODULE__)}.list_objects/2 outside of test.

      bucket: #{inspect(bucket)}
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
