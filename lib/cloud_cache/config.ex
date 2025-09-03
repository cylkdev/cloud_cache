defmodule CloudCache.Config do
  @moduledoc false

  @app :cloud_cache

  @mix_env Mix.env()

  def app, do: @app

  def mix_env do
    Application.get_env(@app, :mix_env) || @mix_env
  end
end
