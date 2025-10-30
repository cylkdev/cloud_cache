import Config

if Mix.env() === :test do
  config :cloud_cache,
    auto_start: true,
    caches: [CloudCache.Adapters.S3]

  config :cloud_cache, CloudCache.Adapters.S3,
    sandbox_enabled: false,
    local_stack: true
else
  config :cloud_cache,
    auto_start: true,
    caches: [CloudCache.Adapters.S3]

  config :cloud_cache, CloudCache.Adapters.S3,
    sandbox_enabled: false,
    retries: [
      max_attempts: 10,
      base_backoff_in_ms: 10,
      max_backoff_in_ms: 10_000
    ],
    access_key_id: [
      {:awscli, System.get_env("AWS_PROFILE", "cloud_cache"), 30},
      {:awscli, System.get_env("AWS_PROFILE", "default"), 30},
      {:system, "CLOUD_CACHE_AWS_ACCESS_KEY_ID"},
      {:system, "AWS_ACCESS_KEY_ID"},
      :instance_role
    ],
    secret_access_key: [
      {:awscli, System.get_env("AWS_PROFILE", "cloud_cache"), 30},
      {:awscli, System.get_env("AWS_PROFILE", "default"), 30},
      {:system, "CLOUD_CACHE_AWS_SECRET_ACCESS_KEY"},
      {:system, "AWS_SECRET_ACCESS_KEY"},
      :instance_role
    ]
end
