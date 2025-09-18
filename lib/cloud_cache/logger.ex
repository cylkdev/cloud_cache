defmodule CloudCache.Logger do
  require Logger

  def debug(prefix, message) do
    prefix
    |> format_message(message)
    |> Logger.debug()
  end

  if macro_exported?(Logger, :warning, 2) do
    def warning(prefix, message) do
      prefix
      |> format_message(message)
      |> Logger.warning()
    end
  else
    def warning(prefix, message) do
      prefix
      |> format_message(message)
      |> Logger.warn()
    end
  end

  defp format_message(prefix, message) do
    "[#{prefix}] #{message}"
  end
end
