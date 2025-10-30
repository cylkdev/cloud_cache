defmodule CloudCache.Adapters.S3.Testing do
  alias CloudCache.Adapters.S3
  alias ExUnit.Callbacks

  def start_supervised!(args, opts \\ []) do
    args
    |> S3.child_spec()
    |> Callbacks.start_supervised!(opts)
  end
end
