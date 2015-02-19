package org.embulk.output.jdbc;

import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.io.IOException;
import java.sql.Types;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import org.slf4j.Logger;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
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
import org.embulk.output.jdbc.setter.ColumnSetter;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.embulk.output.jdbc.RetryExecutor.IdempotentOperation;
import static org.embulk.output.jdbc.RetryExecutor.retryExecutor;

public abstract class AbstractJdbcOutputPlugin
        implements OutputPlugin
{
    private final Logger logger = Exec.getLogger(getClass());

    public interface PluginTask
            extends Task
    {
        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("null")
        public Optional<Integer> getPort();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

        @Config("options")
        @ConfigDefault("{}")
        public Properties getOptions();

        @Config("database")
        public String getDatabase();

        @Config("schema")
        @ConfigDefault("null")
        public Optional<String> getSchema();

        @Config("table")
        public String getTable();

        @Config("mode")
        public String getModeConfig();

        @Config("batch_size")
        @ConfigDefault("16777216")
        // TODO set minimum number
        public int getBatchSize();

        public void setMode(Mode mode);
        public Mode getMode();

        public JdbcSchema getLoadSchema();
        public void setLoadSchema(JdbcSchema schema);

        public Optional<String> getSwapTable();
        public void setSwapTable(Optional<String> name);

        public Optional<String> getMultipleLoadTablePrefix();
        public void setMultipleLoadTablePrefix(Optional<String> prefix);
    }

    // for subclasses to add @Config
    protected Class<? extends PluginTask> getTaskClass()
    {
        return PluginTask.class;
    }

    protected abstract JdbcOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation);

    protected abstract BatchInsert newBatchInsert(PluginTask task) throws IOException, SQLException;

    protected JdbcOutputConnection newConnection(PluginTask task, boolean retryableMetadataOperation,
            boolean autoCommit) throws SQLException
    {
        return getConnector(task, retryableMetadataOperation).connect(autoCommit);
    }

    public enum Mode {
        INSERT,
        INSERT_DIRECT,
        TRUNCATE_INSERT,
        MERGE,
        REPLACE,
        REPLACE_INPLACE;
        //REPLACE_PARTITIONING,  // MySQL: partitioning, PostgreSQL: inheritance

        public boolean isDirectWrite()
        {
            return this == INSERT_DIRECT;
        }

        public boolean isInplace()
        {
            return this == INSERT_DIRECT || this == REPLACE_INPLACE;
        }

        public boolean usesMultipleLoadTables()
        {
            return !isInplace();
        }

        public boolean createAndSwapTable()
        {
            return this == REPLACE_INPLACE || this == REPLACE;
        }
    }

    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(getTaskClass());

        // TODO this is a temporary code. behavior will change in a future release.
        switch(task.getModeConfig()) {
        case "insert":
            task.setMode(Mode.INSERT_DIRECT);
            break;
        case "replace":
            task.setMode(Mode.REPLACE_INPLACE);
            break;
        default:
            throw new ConfigException(String.format("Unknown mode '%s'. Supported modes are: insert, replace", task.getModeConfig()));
        }

        //switch(task.getModeConfig()) {
        ////case "insert":
        ////    task.setMode(Mode.INSERT);
        ////    break;
        //case "insert_direct":
        //    task.setMode(Mode.INSERT_DIRECT);
        //    break;
        ////case "truncate_insert":  // TODO
        ////    task.setMode(Mode.TRUNCATE_INSERT);
        ////    break;
        ////case "merge":  // TODO
        ////    task.setMode(Mode.MERGE);
        ////    break;
        ////case "replace":
        ////    task.setMode(Mode.REPLACE);
        ////    break;
        //case "replace_inplace":
        //    task.setMode(Mode.REPLACE_INPLACE);
        //    break;
        //default:
        //    new ConfigException(String.format("Unknown mode '%s'. Supported modes are: insert_direct, replace_inplace", task.getModeConfig()));
        //}

        task = begin(task, schema, taskCount);
        control.run(task.dump());
        return commit(task, schema, taskCount);
    }

    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(getTaskClass());

        if (task.getMode().isInplace()) {
            throw new UnsupportedOperationException("inplace mode is not resumable. You need to delete partially-loaded records from the database and restart the entire transaction.");
        }

        task = begin(task, schema, taskCount);
        control.run(task.dump());
        return commit(task, schema, taskCount);
    }

    private String getTransactionUniqueName()
    {
        // TODO use uuid?
        Timestamp t = Exec.session().getTransactionTime();
        return String.format("%016x%08x", t.getEpochSecond(), t.getNano());
    }

    private PluginTask begin(final PluginTask task,
            final Schema schema, int taskCount)
    {
        try {
            withRetry(new IdempotentSqlRunnable() {  // no intermediate data if isDirectWrite == true
                public void run() throws SQLException
                {
                    JdbcOutputConnection con = newConnection(task, true, false);
                    try {
                        doBegin(con, task, schema);
                    } finally {
                        con.close();
                    }
                }
            });
        } catch (SQLException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        return task;
    }

    private ConfigDiff commit(final PluginTask task,
            Schema schema, final int taskCount)
    {
        if (!task.getMode().isDirectWrite()) {  // no intermediate data if isDirectWrite == true
            try {
                withRetry(new IdempotentSqlRunnable() {
                    public void run() throws SQLException
                    {
                        JdbcOutputConnection con = newConnection(task, false, false);
                        try {
                            doCommit(con, task, taskCount);
                        } finally {
                            con.close();
                        }
                    }
                });
            } catch (SQLException | InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
        return Exec.newConfigDiff();
    }

    public void cleanup(TaskSource taskSource,
            Schema schema, final int taskCount,
            final List<CommitReport> successCommitReports)
    {
        final PluginTask task = taskSource.loadTask(getTaskClass());

        if (!task.getMode().isDirectWrite()) {  // no intermediate data if isDirectWrite == true
            try {
                withRetry(new IdempotentSqlRunnable() {
                    public void run() throws SQLException
                    {
                        JdbcOutputConnection con = newConnection(task, true, true);
                        try {
                            doCleanup(con, task, taskCount, successCommitReports);
                        } finally {
                            con.close();
                        }
                    }
                });
            } catch (SQLException | InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    protected void doBegin(JdbcOutputConnection con,
            PluginTask task, Schema schema) throws SQLException
    {
        Mode mode = task.getMode();

        JdbcSchema targetTableSchema;
        if (mode.createAndSwapTable()) {
            // DROP TABLE IF EXISTS xyz__0000000054d92dee1e452158_bulk_load_temp
            // CREATE TABLE IF NOT EXISTS xyz__0000000054d92dee1e452158_bulk_load_temp
            // swapTableName = "xyz__0000000054d92dee1e452158_bulk_load_temp"
            String swapTableName = task.getTable() + "_" + getTransactionUniqueName() + "_bulk_load_temp";
            con.dropTableIfExists(swapTableName);
            con.createTableIfNotExists(swapTableName, newJdbcSchemaForNewTable(schema));
            targetTableSchema = newJdbcSchemaFromExistentTable(con, swapTableName);
            task.setSwapTable(Optional.of(swapTableName));
        } else {
            // CREATE TABLE IF NOT EXISTS xyz
            con.createTableIfNotExists(task.getTable(), newJdbcSchemaForNewTable(schema));
            targetTableSchema = newJdbcSchemaFromExistentTable(con, task.getTable());
            task.setSwapTable(Optional.<String>absent());
        }

        if (mode.usesMultipleLoadTables()) {
            // multipleLoadTablePrefix = "xyz__0000000054d92dee1e452158_"
            // workers run:
            //   CREATE TABLE xyz__0000000054d92dee1e452158_%d
            String multipleLoadTablePrefix = task.getTable() + "_" + getTransactionUniqueName();
            task.setMultipleLoadTablePrefix(Optional.of(multipleLoadTablePrefix));
        } else {
            task.setMultipleLoadTablePrefix(Optional.<String>absent());
        }

        task.setLoadSchema(matchSchemaByColumnNames(schema, targetTableSchema));
    }

    protected void doCommit(JdbcOutputConnection con, PluginTask task, int taskCount)
        throws SQLException
    {
        switch (task.getMode()) {
        case INSERT:
            // aggregate insert into target
            //con.gatherInsertTables();
            throw new UnsupportedOperationException("not implemented yet"); // TODO
        case INSERT_DIRECT:
            // already done
            break;
        case TRUNCATE_INSERT:
            // truncate & aggregate insert into target
            throw new UnsupportedOperationException("not implemented yet");
            //break;
        case MERGE:
            // aggregate merge into target
            throw new UnsupportedOperationException("not implemented yet");
            //break;
        case REPLACE:
            if (taskCount == 1) {
                // swap table
                con.replaceTable(task.getSwapTable().get(), task.getLoadSchema(), task.getTable());
            } else {
                // aggregate insert into swap table & swap table
                throw new UnsupportedOperationException("not implemented yet");
            }
            break;
        case REPLACE_INPLACE:
            // swap table
            con.replaceTable(task.getSwapTable().get(), task.getLoadSchema(), task.getTable());
            break;
        }
    }

    protected void doCleanup(JdbcOutputConnection con, PluginTask task, int taskCount,
            List<CommitReport> successCommitReports)
        throws SQLException
    {
        if (task.getSwapTable().isPresent()) {
            con.dropTableIfExists(task.getSwapTable().get());
        }
        if (task.getMultipleLoadTablePrefix().isPresent()) {
            for (int i=0; i < taskCount; i++) {
                con.dropTableIfExists(formatMultipleLoadTableName(task, i));
            }
        }
    }

    static String formatMultipleLoadTableName(PluginTask task, int taskIndex)
    {
        return task.getMultipleLoadTablePrefix().get() + String.format("%04x", taskIndex);
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

    public JdbcSchema newJdbcSchemaFromExistentTable(JdbcOutputConnection connection,
            String tableName) throws SQLException
    {
        DatabaseMetaData dbm = connection.getMetaData();
        String escape = dbm.getSearchStringEscape();

        ImmutableList.Builder<JdbcColumn> columns = ImmutableList.builder();
        String schemaNamePattern = JdbcUtils.escapeSearchString(connection.getSchemaName(), escape);
        String tableNamePattern = JdbcUtils.escapeSearchString(tableName, escape);
        ResultSet rs = dbm.getColumns(null, schemaNamePattern, tableNamePattern, null);
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
                //rs.getString("IS_NULLABLE").equals("NO")  // "YES" or ""  // TODO
                //rs.getString("COLUMN_DEF") // or null  // TODO
                columns.add(new JdbcColumn(
                            columnName, typeName,
                            sqlType, colSize, decDigit));
            }
        } finally {
            rs.close();
        }
        return new JdbcSchema(columns.build());
    }

    private JdbcSchema matchSchemaByColumnNames(Schema inputSchema, JdbcSchema targetTableSchema)
    {
        // TODO for each inputSchema.getColumns(), search a column whose name
        //      matches with targetTableSchema. if not match, create JdbcSchema.skipColumn().
        return targetTableSchema;
    }

    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, final int taskIndex)
    {
        final PluginTask task = taskSource.loadTask(getTaskClass());
        final Mode mode = task.getMode();

        BatchInsert batch;
        try {
            batch = newBatchInsert(task);
        } catch (IOException | SQLException ex) {
            throw new RuntimeException(ex);
        }
        try {
            PageReader reader = new PageReader(schema);
            ColumnSetterFactory factory = new ColumnSetterFactory(batch, reader, null);  // TODO TimestampFormatter

            JdbcSchema loadSchema = task.getLoadSchema();

            ImmutableList.Builder<JdbcColumn> insertColumns = ImmutableList.builder();
            ImmutableList.Builder<ColumnSetter> columnSetters = ImmutableList.builder();
            for (JdbcColumn c : loadSchema.getColumns()) {
                if (c.isSkipColumn()) {
                    columnSetters.add(factory.newSkipColumnSetter());
                } else {
                    columnSetters.add(factory.newColumnSetter(c));
                    insertColumns.add(c);
                }
            }
            final JdbcSchema insertSchema = new JdbcSchema(insertColumns.build());

            final BatchInsert b = batch;
            withRetry(new IdempotentSqlRunnable() {
                public void run() throws SQLException
                {
                    String loadTable;
                    boolean createTable;
                    if (mode.usesMultipleLoadTables()) {
                        // insert, truncate_insert, merge, replace
                        loadTable = formatMultipleLoadTableName(task, taskIndex);
                        JdbcOutputConnection con = newConnection(task, true, true);
                        try {
                            con.createTableIfNotExists(loadTable, insertSchema);
                        } finally {
                            con.close();
                        }

                    } else if (!mode.usesMultipleLoadTables() && mode.createAndSwapTable()) {
                        // replace_inplace
                        loadTable = task.getSwapTable().get();

                    } else {
                        // insert_direct
                        loadTable = task.getTable();
                    }

                    b.prepare(loadTable, insertSchema);
                }
            });

            PluginPageOutput output = new PluginPageOutput(reader, batch, columnSetters.build(),
                    task.getBatchSize());
            batch = null;
            return output;

        } catch (SQLException | InterruptedException ex) {
            throw new RuntimeException(ex);

        } finally {
            if (batch != null) {
                try {
                    batch.close();
                } catch (IOException | SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

    public static class PluginPageOutput
            implements TransactionalPageOutput
    {
        private final PageReader pageReader;
        private final BatchInsert batch;
        private final List<Column> columns;
        private final List<ColumnSetter> columnSetters;
        private final int batchSize;
        private final int foraceBatchFlushSize;

        public PluginPageOutput(PageReader pageReader,
                BatchInsert batch, List<ColumnSetter> columnSetters,
                int batchSize)
        {
            this.pageReader = pageReader;
            this.batch = batch;
            this.columns = pageReader.getSchema().getColumns();
            this.columnSetters = columnSetters;
            this.batchSize = batchSize;
            this.foraceBatchFlushSize = batchSize * 2;
        }

        @Override
        public void add(Page page)
        {
            try {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    if (batch.getBatchWeight() > foraceBatchFlushSize) {
                        batch.flush();
                    }
                    for (int i=0; i < columnSetters.size(); i++) {
                        columns.get(i).visit(columnSetters.get(i));
                    }
                    batch.add();
                }
                if (batch.getBatchWeight() > batchSize) {
                    batch.flush();
                }
            } catch (IOException | SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void finish()
        {
            try {
                batch.finish();
            } catch (IOException | SQLException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void close()
        {
            try {
                batch.close();
            } catch (IOException | SQLException ex) {
                throw new RuntimeException(ex);
            }
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

    public static interface IdempotentSqlRunnable
    {
        public void run() throws SQLException;
    }

    protected void withRetry(IdempotentSqlRunnable op)
            throws SQLException, InterruptedException
    {
        withRetry(op, "Operation failed");
    }

    protected void withRetry(final IdempotentSqlRunnable op, final String errorMessage)
            throws SQLException, InterruptedException
    {
        try {
            retryExecutor()
                .setRetryLimit(12)
                .setInitialRetryWait(1000)
                .setMaxRetryWait(30 * 60 * 1000)
                .runInterruptible(new IdempotentOperation<Void>() {
                    public Void call() throws Exception
                    {
                        op.run();
                        return null;
                    }

                    public void onRetry(Throwable exception, int retryCount, int retryLimit, int retryWait)
                    {
                        if (exception instanceof SQLException) {
                            SQLException ex = (SQLException) exception;
                            String sqlState = ex.getSQLState();
                            int errorCode = ex.getErrorCode();
                            logger.warn("{} ({}:{}), retrying {}/{} after {} seconds. Message: {}",
                                    errorMessage, errorCode, sqlState, retryCount, retryLimit, retryWait/1000,
                                    buildExceptionMessage(exception));
                        } else {
                            logger.warn("{}, retrying {}/{} after {} seconds. Message: {}",
                                    errorMessage, retryCount, retryLimit, retryWait/1000,
                                    buildExceptionMessage(exception));
                        }
                        if (retryCount % 3 == 0) {
                            logger.info("Error details:", exception);
                        }
                    }

                    public void onGiveup(Throwable firstException, Throwable lastException)
                    {
                        if (firstException instanceof SQLException) {
                            SQLException ex = (SQLException) firstException;
                            String sqlState = ex.getSQLState();
                            int errorCode = ex.getErrorCode();
                            logger.error("{} ({}:{})", errorMessage, errorCode, sqlState);
                        }
                    }

                    public boolean isRetryableException(Throwable exception)
                    {
                        if (exception instanceof SQLException) {
                            SQLException ex = (SQLException) exception;
                            String sqlState = ex.getSQLState();
                            int errorCode = ex.getErrorCode();
                            return isRetryableException(ex);
                        }
                        return false;  // TODO
                    }
                });

        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            Throwables.propagateIfInstanceOf(cause, SQLException.class);
            throw Throwables.propagate(cause);
        }
    }

    private String buildExceptionMessage(Throwable ex) {
        StringBuilder sb = new StringBuilder();
        sb.append(ex.getMessage());
        if (ex.getCause() != null) {
            buildExceptionMessageCont(sb, ex.getCause(), ex.getMessage());
        }
        return sb.toString();
    }

    private void buildExceptionMessageCont(StringBuilder sb, Throwable ex, String lastMessage) {
        if (!lastMessage.equals(ex.getMessage())) {
            // suppress same messages
            sb.append(" < ");
            sb.append(ex.getMessage());
        }
        if (ex.getCause() == null) {
            return;
        }
        buildExceptionMessageCont(sb, ex.getCause(), ex.getMessage());
    }
}
