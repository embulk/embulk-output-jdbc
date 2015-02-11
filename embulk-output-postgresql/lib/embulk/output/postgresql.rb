Embulk::JavaPlugin.register_output(
  :postgresql, "org.embulk.output.PostgreSQLOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
