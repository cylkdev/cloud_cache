defmodule CloudCache.Config do
  @moduledoc false

  @app :cloud_cache

  def caches do
    Application.get_env(@app, :caches) || []
  end
end
