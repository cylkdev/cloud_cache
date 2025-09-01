defmodule CloudCache.Adapters.S3.XMLParser do
  @moduledoc false

  import SweetXml, only: [sigil_x: 2]

  def parse(xml) when is_binary(xml) do
    # TODO: update this, it only returns a map right now and this should
    # behave more like JSON decoding.
    xml
    |> SweetXml.parse()
    |> SweetXml.xpath(~x"/*/*"l)
    |> Enum.map(fn node ->
      key = SweetXml.xpath(node, ~x"local-name(.)"s)
      val = SweetXml.xpath(node, ~x"string(.)"s)
      {Macro.underscore(key), val}
    end)
    |> Map.new()
  end
end
