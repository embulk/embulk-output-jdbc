Embulk::JavaPlugin.register_output(
  :db2, "org.embulk.output.DB2OutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
