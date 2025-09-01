defmodule CloudCache.Adapters.S3.Multipart do
  @moduledoc """
  Provides helper functions for S3 multipart uploads.
  """

  @one_mib 1_024 * 1_024
  @one_gib 1_024 * @one_mib
  @one_tib 1_024 * @one_gib

  @five_mib 5 * @one_mib
  @five_tib 5 * @one_tib

  @doc """
  Returns a lazy stream of inclusive byte ranges `{start_byte, end_byte}`
  that divide a file of size `content_length` into fixed-size chunks.

  Ranges are calculated on demand and cover the file either from the
  beginning (`:forward`) or from the end (`:backward`). Each range
  spans `chunk_size` bytes, except the final one which is truncated
  so that `end_byte` never exceeds `content_length - 1`.

  The function validates that the file is large enough to upload (at
  least 5 MiB), does not exceed S3's maximum object size (5 TiB), and
  that the chosen `chunk_size` will not result in more than 10,000
  parts. Passing an invalid `direction` or values outside these limits
  will raise an error.

  ## Examples

      iex> start_index = 0
      ...> content_length = 15 * 1_024 * 1_024 # 15 MiB (15_728_640 bytes)
      ...> chunk_size = 5 * 1_024 * 1_024 # 5 MiB (5_242_880 bytes)
      ...> Enum.to_list(CloudCache.Adapters.S3.Multipart.content_byte_stream(start_index, content_length, chunk_size, :forward))
      [{0, 5_242_879}, {5_242_880, 10_485_759}, {10_485_760, 15_728_639}]

      iex> start_index = 0
      ...> content_length = 15 * 1_024 * 1_024 # 15 MiB (15_728_640 bytes)
      ...> chunk_size = 5 * 1_024 * 1_024 # 5 MiB (5_242_880 bytes)
      ...> Enum.to_list(CloudCache.Adapters.S3.Multipart.content_byte_stream(start_index, content_length, chunk_size, :backward))
      [{10_485_760, 15_728_639}, {5_242_880, 10_485_759}, {0, 5_242_879}]
  """
  def content_byte_stream(start_index, content_length, chunk_size, direction)
      when is_integer(content_length) and content_length > 0 and
             is_integer(chunk_size) and chunk_size > 0 and
             is_integer(start_index) and start_index >= 0 and
             direction in [:forward, :backward] do
    if start_index < 0 do
      raise ArgumentError, "start_index must be >= 0"
    end

    if start_index > @five_tib do
      raise ArgumentError, "start_index must be <= #{@five_tib}"
    end

    if content_length < @five_mib do
      raise ArgumentError, "content_length must be >= 5 MiB"
    end

    if content_length > @five_tib do
      raise ArgumentError, "content_length must be <= 5 TiB"
    end

    required_parts = div(content_length + chunk_size - 1, chunk_size)

    if required_parts > 10_000 do
      raise ArgumentError, "parts would exceed 10,000; increase chunk_size"
    end

    case direction do
      :forward ->
        start = start_index * chunk_size

        if start >= content_length do
          raise ArgumentError, "start_index is out of range"
        end

        Stream.resource(
          fn -> start end,
          fn
            pos when pos >= content_length ->
              {:halt, nil}

            pos ->
              finish = min(pos + chunk_size - 1, content_length - 1)
              {[{pos, finish}], finish + 1}
          end,
          fn _ -> :ok end
        )

      :backward ->
        end_pos = content_length - 1 - start_index * chunk_size

        if end_pos < 0 do
          raise ArgumentError, "start_index is out of range"
        end

        Stream.resource(
          fn -> end_pos end,
          fn
            pos when pos < 0 ->
              {:halt, nil}

            pos ->
              start = max(0, pos - (chunk_size - 1))
              {[{start, pos}], start - 1}
          end,
          fn _ -> :ok end
        )
    end
  end
end
