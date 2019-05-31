Embulk::JavaPlugin.register_output(
  :snowflake, "org.embulk.output.SnowflakeOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
