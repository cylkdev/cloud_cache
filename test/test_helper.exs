ExUnit.start()

CloudCache.Adapters.S3.Sandbox.start_link()

{:ok, _} =
  CloudCache.Adapters.S3.Local.head_or_create_bucket("us-west-1", "test-bucket", [])
