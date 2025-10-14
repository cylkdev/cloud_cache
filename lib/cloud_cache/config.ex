defmodule CloudCache.Config do
  @moduledoc false
  @app :cloud_cache
  def caches, do: Application.get_env(@app, :caches) || []
  def mix_env, do: Application.fetch_env!(@app, :mix_env)
end
