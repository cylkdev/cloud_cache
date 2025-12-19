ExUnit.start()

CloudCache.Adapters.S3.Sandbox.start_link()

CloudCache.create_bucket("test-bucket", "us-west-1",
  s3: [sandbox_enabled: false, local_stack: true]
)
