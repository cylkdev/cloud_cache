ExUnit.start()

CloudCache.Adapters.S3.Testing.S3Sandbox.start_link()

{:ok, _} =
  CloudCache.Adapters.S3.Testing.LocalStack.head_or_create_bucket("us-west-1", "test-bucket", [])
