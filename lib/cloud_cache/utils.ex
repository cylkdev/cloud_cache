defmodule CloudCache.Utils do
  def deep_merge(left, right) when is_list(left) and is_list(right) do
    if Keyword.keyword?(left) and Keyword.keyword?(right) do
      Keyword.merge(left, right, fn _k, v1, v2 ->
        conflict_merge(v1, v2)
      end)
    else
      right
    end
  end

  def deep_merge(%{} = left, %{} = right) do
    Map.merge(left, right, fn _k, v1, v2 ->
      conflict_merge(v1, v2)
    end)
  end

  def deep_merge(_left, right), do: right

  defp conflict_merge(v1, v2) do
    cond do
      is_list(v1) and is_list(v2) ->
        deep_merge(v1, v2)

      is_map(v1) and is_map(v2) ->
        deep_merge(v1, v2)

      true ->
        v2
    end
  end
end
