defmodule CloudCache.Adapters.S3.HTTP do
  @moduledoc false
  alias Req.Response

  @logger_prefix "CloudCache.Adapters.S3.HTTP"

  @finch CloudCache.Adapters.S3.Finch
  @default_opts [
    decode_body: false,
    pool_timeout: 8_000,
    receive_timeout: 120_000,
    retry: :transient
  ]

  def build_request(opts) do
    @default_opts
    |> Keyword.merge(opts)
    |> Keyword.put(:finch, @finch)
    |> normalize_follow_redirects_opt()
    |> Req.new()
  end

  defp normalize_follow_redirects_opt(opts) do
    if Keyword.has_key?(opts, :follow_redirect) do
      opts
      |> Keyword.delete(:follow_redirect)
      |> Keyword.put(:follow_redirects, Keyword.fetch!(opts, :follow_redirect))
    else
      opts
    end
  end

  defp put_body(opts, body) do
    if opts[:json?] do
      opts
      |> Keyword.put(:json, body)
      |> Keyword.delete(:json?)
    else
      Keyword.put(opts, :body, body)
    end
  end

  def request(:get, url, _body, headers, opts) do
    ensure_finch_started!()

    request = build_request(opts)

    CloudCache.Logger.debug(
      @logger_prefix,
      """
      Making HTTP request.

      url:
      #{inspect(url)}

      method:
      GET

      headers:
      #{inspect(headers)}

      options:
      #{inspect(opts, pretty: true)}

      request:
      #{inspect(request, pretty: true)}
      """
    )

    request
    |> Req.get(body: "", url: url, headers: headers)
    |> handle_response()
  end

  def request(:head, url, _body, headers, opts) do
    ensure_finch_started!()

    request = build_request(opts)

    CloudCache.Logger.debug(
      @logger_prefix,
      """
      Making HTTP request.

      url:
      #{inspect(url)}

      method:
      HEAD

      headers:
      #{inspect(headers)}

      options:
      #{inspect(opts, pretty: true)}

      request:
      #{inspect(request, pretty: true)}
      """
    )

    request
    |> Req.head(url: url, headers: headers)
    |> handle_response()
  end

  def request(:delete, url, _body, headers, opts) do
    ensure_finch_started!()

    request = build_request(opts)

    CloudCache.Logger.debug(
      @logger_prefix,
      """
      Making HTTP request.

      url:
        #{inspect(url)}

      method:
        DELETE

      headers:
        #{inspect(headers)}

      options:
        #{inspect(opts, pretty: true)}

      request:
        #{inspect(request, pretty: true)}

      """
    )

    request
    |> Req.delete(url: url, headers: headers)
    |> handle_response()
  end

  def request(:post, url, body, headers, opts) do
    ensure_finch_started!()

    request = build_request(opts)

    CloudCache.Logger.debug(
      @logger_prefix,
      """
      Making HTTP request.

      url:
      #{inspect(url)}

      method:
      POST

      headers:
      #{inspect(headers)}

      options:
      #{inspect(opts, pretty: true)}

      request:
      #{inspect(request, pretty: true)}
      """
    )

    request
    |> Req.post(put_body([url: url, headers: headers], body))
    |> handle_response()
  end

  def request(:put, url, body, headers, opts) do
    ensure_finch_started!()

    request = build_request(opts)

    CloudCache.Logger.debug(
      @logger_prefix,
      """
      Making HTTP request.

      url:
      #{inspect(url)}

      method:
      PUT

      headers:
      #{inspect(headers)}

      options:
      #{inspect(opts, pretty: true)}

      request:
      #{inspect(request, pretty: true)}
      """
    )

    request
    |> Req.put(put_body([url: url, headers: headers], body))
    |> handle_response()
  end

  defp ensure_finch_started! do
    with pid when is_pid(pid) <- Process.whereis(@finch),
         true <- Process.alive?(pid) do
      :ok
    else
      _ -> raise "#{inspect(@finch)} not started."
    end
  end

  defp handle_response({:ok, %Response{status: status_code} = response})
       when status_code in 200..299 do
    headers =
      response
      |> Req.get_headers_list()
      |> Map.new()

    {:ok,
     %{
       status_code: status_code,
       headers: headers,
       body: response.body
     }}
  end

  defp handle_response({:ok, %Response{} = response}) do
    {:error, %{reason: response}}
  end

  defp handle_response({:error, reason}) do
    {:error, %{reason: reason}}
  end
end
