defmodule CloudCache.Application do
  use Application

  @name CloudCache.Supervisor

  def start(_type, _args) do
    children = children()

    opts = [strategy: :one_for_one, name: @name]
    Supervisor.start_link(children, opts)
  end

  def alive? do
    case whereis() do
      pid when is_pid(pid) -> Process.alive?(pid)
      _ -> false
    end
  end

  def whereis do
    Process.whereis(@name)
  end

  def children do
    [
      {Registry, keys: :duplicate, name: CloudCache.Registry}
    ] ++ supervisor_children()
  end

  defp supervisor_children do
    if CloudCache.Config.auto_start() do
      [{CloudCache, CloudCache.Config.caches()}]
    else
      []
    end
  end
end
