Embulk::JavaPlugin.register_output(
  :redshift, "org.embulk.output.RedshiftOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
