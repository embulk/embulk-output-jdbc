Embulk::JavaPlugin.register_output(
  :sqlserver, "org.embulk.output.SQLServerOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
