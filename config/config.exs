import Config

config :excache,
  shard_count: 8,
  sweep_interval: 1000,
  port: String.to_integer(System.get_env("EXCACHE_PORT") || "6379"),
  snapshot_path: System.get_env("EXCACHE_SNAPSHOT_PATH"),
  aof_path: System.get_env("EXCACHE_AOF_PATH"),
  aof_fsync: :everysec,
  keyspace_notifications: true

if File.exists?(Path.join(__DIR__, "#{config_env()}.exs")) do
  import_config "#{config_env()}.exs"
end
