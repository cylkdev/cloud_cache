defmodule CloudCache.Config do
  @moduledoc false
  @app :cloud_cache

  def get_env(key), do: Application.get_env(@app, key)
  def auto_start, do: Application.get_env(@app, :auto_start, true)
  def caches, do: Application.get_env(@app, :caches) || []
end
