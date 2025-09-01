defmodule CloudCache.Config do
  @moduledoc false

  @app :cloud_cache

  def app, do: @app

  def mix_env, do: Application.fetch_env!(@app, :mix_env)
end
