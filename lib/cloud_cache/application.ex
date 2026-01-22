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
    [{Registry, name: CloudCache.Registry, keys: :duplicate}] ++ caches()
  end

  defp caches do
    if CloudCache.Config.auto_start() do
      case CloudCache.Config.caches() do
        [] -> [{CloudCache, [CloudCache.Adapters.S3]}]
        caches -> [{CloudCache, caches}]
      end
    else
      []
    end
  end
end
