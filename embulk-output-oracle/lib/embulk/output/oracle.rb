Embulk::JavaPlugin.register_output(
  :oracle, "org.embulk.output.OracleOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
