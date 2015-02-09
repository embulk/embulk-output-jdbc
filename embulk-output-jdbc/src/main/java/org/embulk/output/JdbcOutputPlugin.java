package org.embulk.output;

import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.sql.Types;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.JdbcUtils;
import org.embulk.output.jdbc.setter.ColumnSetter;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;

public abstract class JdbcOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("host")
        public String getHost();

        @Config("int")
        @ConfigDefault("null")
        public Optional<Integer> getPort();

        @Config("username")
        @ConfigDefault("0")
        public String getUsername();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

        @Config("database")
        public String getDatabase();

        @Config("table")
        public String getTable();

        @Config("mode")
        public String getModeConfig();

        @Config("batch_flush_size")
        @ConfigDefault("262144")
        public int getBatchFlushSize();

        @Config("connection_properties")
        @ConfigDefault("{}")
        public Properties getConnectionProperties();

        public void setMode(Mode mode);
        public Mode getMode();
    }

    public enum Mode {
        INSERT(false),
        INSERT_INPLACE(true),
        TRUNCATE_INSERT(false),
        MERGE(false),
        REPLACE(false);

        private final boolean inplace;

        private Mode(boolean inplace)
        {
            this.inplace = inplace;
        }

        public boolean isInplace()
        {
            return inplace;
        }
    }

    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int processorCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        switch(task.getModeConfig()) {
        // TODO
        case "insert":
            task.setMode(Mode.INSERT);
            break;
        //case "insert_inplace":
        //    task.setMode(Mode.INSERT_INPLACE);
        //    break;
        //case "truncate_insert":
        //    task.setMode(Mode.TRUNCATE_INSERT);
        //    break;
        //case "merge":
        //    task.setMode(Mode.MERGE);
        //    break;
        case "replace":
            task.setMode(Mode.REPLACE);
            break;
        default:
            new ConfigException(String.format("Unknown mode '%s'. Supported modes are: insert, replace", task.getModeConfig()));
        }

        task = begin(task, schema, processorCount);
        control.run(task.dump());
        return commit(task, schema, processorCount, control);
    }

    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int processorCount,
            OutputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        if (task.getMode().isInplace()) {
            throw new UnsupportedOperationException("inplace mode is not resumable. You need to delete partially-loaded records from the database and restart the entire transaction.");
        }

        task = begin(task, schema, processorCount);
        control.run(task.dump());
        return commit(task, schema, processorCount, control);
    }

    private String getTransactionUniqueName()
    {
        // TODO use uuid?
        Timestamp t = Exec.session().getTransactionTime();
        return String.format("%016x%08x", t.getEpochSecond(), t.getNano());
    }

    protected PluginTask begin(PluginTask task,
            Schema schema, int processorCount)
    {
        String loadTableNamePrefix = task.getTable() + "__" + getTransactionUniqueName() + "_";

        // TODO set to task
        return task;
    }

    protected ConfigDiff commit(PluginTask task,
            Schema schema, int processorCount,
            OutputPlugin.Control control)
    {
        // TODO
        return null;
    }

    protected JdbcSchema newJdbcSchemaForNewTable(Schema schema)
    {
        final ImmutableList.Builder<JdbcColumn> columns = ImmutableList.builder();
        for (Column c : schema.getColumns()) {
            final String columnName = c.getName();
            c.visit(new ColumnVisitor() {
                public void booleanColumn(Column column)
                {
                    columns.add(new JdbcColumn(
                            columnName, "BOOLEAN",
                            Types.BOOLEAN, 1, 0));
                }

                public void longColumn(Column column)
                {
                    columns.add(new JdbcColumn(
                            columnName, "BIGINT",
                            Types.BIGINT, 22, 0));
                }

                public void doubleColumn(Column column)
                {
                    columns.add(new JdbcColumn(
                            columnName, "DOUBLE PRECISION",
                            Types.FLOAT, 24, 0));
                }

                public void stringColumn(Column column)
                {
                    columns.add(new JdbcColumn(
                                columnName, "CLOB",
                                Types.CLOB, 4000, 0));  // TODO size type param
                }

                public void timestampColumn(Column column)
                {
                    columns.add(new JdbcColumn(
                                columnName, "TIMESTAMP",
                                Types.TIMESTAMP, 26, 0));  // size type param is from postgresql.
                }
            });
        }
        return new JdbcSchema(columns.build());
    }

    protected JdbcSchema newJdbcSchemaFromExistentTable(Connection connection,
            String schemaName, String tableName) throws SQLException
    {
        DatabaseMetaData dbm = connection.getMetaData();
        String escape = dbm.getSearchStringEscape();

        ImmutableList.Builder<JdbcColumn> columns = ImmutableList.builder();
        String schemaNamePattern = JdbcUtils.escapeSearchString(schemaName, escape);
        String tableNamePattern = JdbcUtils.escapeSearchString(tableName, escape);
        ResultSet rs = connection.getMetaData().getColumns(null, schemaNamePattern, tableNamePattern, null);
        try {
            while(rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String typeName = rs.getString("TYPE_NAME");
                typeName = typeName.toUpperCase(Locale.ENGLISH);
                int sqlType = rs.getInt("DATA_TYPE");
                int colSize = rs.getInt("COLUMN_SIZE");
                int decDigit = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    decDigit = -1;
                }
                //rs.getString("IS_NULLABLE").equals("NO")  // "YES" or ""
                //rs.getString("COLUMN_DEF") // or null
                columns.add(new JdbcColumn(
                            columnName, typeName,
                            sqlType, colSize, decDigit));
            }
        } finally {
            rs.close();
        }
        return new JdbcSchema(columns.build());
    }

    public void cleanup(TaskSource taskSource,
            Schema schema, int processorCount,
            List<CommitReport> successCommitReports)
    {
        // TODO
    }

    //protected Properties getTransactionConnectionProperties()
    //{
    //}

    //protected Properties getBatchConnectionProperties()
    //{
    //}

    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int processorIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        // TODO
        //return new PluginPageOutput(schema);
        return null;
    }

    public static class PluginPageOutput
            implements TransactionalPageOutput
    {
        // TODO
        private final Schema schema = null;
        private final PageReader pageReader = null;
        private final ColumnSetter[] columnSetters = null;
        private final JdbcColumn[] columns = null;

        @Override
        public void add(Page page)
        {
            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                for (int i=0; i < columnSetters.length; i++) {
                    schema.getColumn(i).visit(columnSetters[i]);
                }
            }
        }

        @Override
        public void finish()
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public void abort()
        {
        }

        @Override
        public CommitReport commit()
        {
            return Exec.newCommitReport();
        }
    }
}
