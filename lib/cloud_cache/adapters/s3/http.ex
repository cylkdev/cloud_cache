defmodule CloudCache.Adapters.S3.HTTP do
  @moduledoc false
  alias Req.Response

  @default_opts [decode_body: false]

  def request(:get, url, _body, headers, opts) do
    @default_opts
    |> Keyword.merge(opts)
    |> Keyword.merge(url: url, headers: headers)
    |> Req.get()
    |> handle_response()
  end

  def request(:head, url, _body, headers, opts) do
    @default_opts
    |> Keyword.merge(opts)
    |> Keyword.merge(url: url, headers: headers)
    |> Req.head()
    |> handle_response()
  end

  def request(:delete, url, _body, headers, opts) do
    @default_opts
    |> Keyword.merge(opts)
    |> Keyword.merge(url: url, headers: headers)
    |> Req.delete()
    |> handle_response()
  end

  def request(:post, url, body, headers, opts) do
    @default_opts
    |> Keyword.merge(opts)
    |> Keyword.merge(url: url, headers: headers)
    |> put_body(body)
    |> Req.post()
    |> handle_response()
  end

  def request(:put, url, body, headers, opts) do
    @default_opts
    |> Keyword.merge(opts)
    |> Keyword.merge(url: url, headers: headers)
    |> put_body(body)
    |> Req.put()
    |> handle_response()
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

  defp handle_response({:ok, %Response{status: status_code, body: body} = response})
       when status_code in 200..299 do
    headers =
      response
      |> Req.get_headers_list()
      |> Map.new()

    {:ok, %{status_code: status_code, headers: headers, body: body}}
  end

  defp handle_response({:ok, %Response{} = response}) do
    {:error, %{reason: response}}
  end

  defp handle_response({:error, reason}) do
    {:error, %{reason: reason}}
  end
end
