Embulk::JavaPlugin.register_output(
  :jdbc, "org.embulk.output.JdbcOutputPlugin",
  File.expand_path('../../../classpath', __FILE__))
