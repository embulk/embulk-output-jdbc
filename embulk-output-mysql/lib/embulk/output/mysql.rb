Embulk::JavaPlugin.register_output(
  :mysql, "org.embulk.output.MySQLOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
