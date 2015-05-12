package org.embulk.output.jdbc;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Types;
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
import org.embulk.spi.PluginClassLoader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.output.jdbc.setter.ColumnSetter;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.embulk.output.jdbc.RetryExecutor.IdempotentOperation;

import static org.embulk.output.jdbc.RetryExecutor.retryExecutor;

public abstract class AbstractJdbcOutputPlugin
        implements OutputPlugin
{
    private final static Set<String> loadedJarGlobs = new HashSet<String>();

    private final Logger logger = Exec.getLogger(getClass());

    public interface PluginTask
            extends Task
    {
        @Config("options")
        @ConfigDefault("{}")
        public Properties getOptions();

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

    protected void loadDriverJar(String glob)
    {
        synchronized (loadedJarGlobs) {
            if (!loadedJarGlobs.contains(glob)) {
                // TODO match glob
                PluginClassLoader loader = (PluginClassLoader) getClass().getClassLoader();
                loader.addPath(Paths.get(glob));
                loadedJarGlobs.add(glob);
            }
        }
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

        /**
         * True if this mode directly modifies the target table without creating temporary tables.
         */
        public boolean isDirectModify()
        {
            return this == INSERT_DIRECT;
        }

        /**
         * True if this mode creates temporary table for each tasks.
         */
        public boolean tempTablePerTask()
        {
            return !(this == INSERT_DIRECT || this == REPLACE_INPLACE || this == MERGE);
        }

        /**
         * True if this mode uses MERGE statement to commit temporary tables to the target table
         */
        public boolean commitByMerge()
        {
            return this == MERGE;
        }

        /**
         * True if this mode swaps the target tables with temporary tables to commit
         */
        public boolean commitBySwapTable()
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
        case "merge":
            task.setMode(Mode.MERGE);
            break;
        default:
            throw new ConfigException(String.format("Unknown mode '%s'. Supported modes are: insert, replace, merge", task.getModeConfig()));
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

        if (!task.getMode().tempTablePerTask()) {
            throw new UnsupportedOperationException("inplace mode is not resumable. You need to delete partially-loaded records from the database and restart the entire transaction.");
        }

        task = begin(task, schema, taskCount);
        control.run(task.dump());
        return commit(task, schema, taskCount);
    }

    protected String getTransactionUniqueName()
    {
        // TODO use uuid?
        Timestamp t = Exec.session().getTransactionTime();
        return String.format("%016x%08x", t.getEpochSecond(), t.getNano());
    }

    private PluginTask begin(final PluginTask task,
            final Schema schema, int taskCount)
    {
        try {
            withRetry(new IdempotentSqlRunnable() {  // no intermediate data if isDirectModify == true
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
        if (!task.getMode().isDirectModify()) {  // no intermediate data if isDirectModify == true
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

        if (!task.getMode().isDirectModify()) {  // no intermediate data if isDirectModify == true
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
        if (mode.commitBySwapTable()) {
            // DROP TABLE IF EXISTS xyz__0000000054d92dee1e452158_bulk_load_temp
            // CREATE TABLE IF NOT EXISTS xyz__0000000054d92dee1e452158_bulk_load_temp
            // swapTableName = "xyz__0000000054d92dee1e452158_bulk_load_temp"
            String swapTableName = generateSwapTableName(task, con);
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

        if (mode.tempTablePerTask()) {
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

    protected String generateSwapTableName(PluginTask task, JdbcOutputConnection con) throws SQLException
    {
        String tableName = task.getTable();
        String suffix = "_bl_tmp";
        String uniqueSuffix = getTransactionUniqueName() + suffix;

        // way to count length of table name varies by DBMSs (bytes or characters),
        // so truncate swap table name by one character.
        while (!isValidIdentifier(con, tableName + "_" + uniqueSuffix)) {
            if (uniqueSuffix.length() > 8 + suffix.length()) {
                // truncate transaction unique name
                // (include 8 characters of the transaction name at least)
                uniqueSuffix = uniqueSuffix.substring(1);
            } else {
                if (tableName.isEmpty()) {
                    throw new ConfigException("Table name is too long to generate temporary table name");
                }
                // truncate table name
                tableName = tableName.substring(0, tableName.length() - 1);
                //if (!connection.tableExists(tableName)) {
                // TODO this doesn't help. Rather than truncating more characters,
                //      here needs to replace characters with random characters. But
                //      to make the result deterministic. So, an idea is replacing
                //      the last character to the first (second, third, ... for each loop)
                //      of md5(original table name).
                //}
            }

        }
        return tableName + "_" + uniqueSuffix;
    }

    // subclass should override and return if identifier (ex. table name) is valid for the DBMS.
    protected boolean isValidIdentifier(JdbcOutputConnection con, String identifier) throws SQLException
    {
        return true;
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
                            Types.BOOLEAN, 1, 0, false));
                }

                public void longColumn(Column column)
                {
                    columns.add(new JdbcColumn(
                            columnName, "BIGINT",
                            Types.BIGINT, 22, 0, false));
                }

                public void doubleColumn(Column column)
                {
                    columns.add(new JdbcColumn(
                            columnName, "DOUBLE PRECISION",
                            Types.FLOAT, 24, 0, false));
                }

                public void stringColumn(Column column)
                {
                    columns.add(new JdbcColumn(
                                columnName, "CLOB",
                                Types.CLOB, 4000, 0, false));  // TODO size type param
                }

                public void timestampColumn(Column column)
                {
                    columns.add(new JdbcColumn(
                                columnName, "TIMESTAMP",
                                Types.TIMESTAMP, 26, 0, false));  // size type param is from postgresql.
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
        String schemaNamePattern = JdbcUtils.escapeSearchString(connection.getSchemaName(), escape);

        ResultSet rs = dbm.getPrimaryKeys(null, schemaNamePattern, tableName);
        ImmutableList.Builder<String> primaryKeysBuilder = ImmutableList.builder();
        try {
            while(rs.next()) {
                primaryKeysBuilder.add(rs.getString("COLUMN_NAME"));
            }
        } finally {
            rs.close();
        }
        ImmutableList<String> primaryKeys = primaryKeysBuilder.build();

        String tableNamePattern = JdbcUtils.escapeSearchString(tableName, escape);
        ImmutableList.Builder<JdbcColumn> columns = ImmutableList.builder();
        rs = dbm.getColumns(null, schemaNamePattern, tableNamePattern, null);
        try {
            while(rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String typeName = rs.getString("TYPE_NAME");
                boolean isPrimaryKey = primaryKeys.contains(columnName);
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
                            sqlType, colSize, decDigit, isPrimaryKey));
            }
        } finally {
            rs.close();
        }
        return new JdbcSchema(columns.build());
    }

    private JdbcSchema matchSchemaByColumnNames(Schema inputSchema, JdbcSchema targetTableSchema)
    {
        ImmutableList.Builder<JdbcColumn> jdbcColumns = ImmutableList.builder();

        outer : for (Column column : inputSchema.getColumns()) {
            for (JdbcColumn jdbcColumn : targetTableSchema.getColumns()) {
                if (jdbcColumn.getName().equals(column.getName())) {
                    jdbcColumns.add(jdbcColumn);
                    continue outer;
                }
            }

            jdbcColumns.add(JdbcColumn.skipColumn());
        }

        return new JdbcSchema(jdbcColumns.build());
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
            ColumnSetterFactory factory = newColumnSetterFactory(batch, reader, null);  // TODO TimestampFormatter

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
                    if (mode.tempTablePerTask()) {
                        // insert, truncate_insert, merge, replace
                        loadTable = formatMultipleLoadTableName(task, taskIndex);
                        JdbcOutputConnection con = newConnection(task, true, true);
                        try {
                            con.createTableIfNotExists(loadTable, insertSchema);
                        } finally {
                            con.close();
                        }

                    } else if (!mode.tempTablePerTask() && mode.commitBySwapTable()) {
                        // replace_inplace
                        loadTable = task.getSwapTable().get();

                    } else {
                        // insert_direct
                        loadTable = task.getTable();
                    }
                    b.prepare(loadTable, insertSchema);
                }
            });

            PluginPageOutput output = newPluginPageOutput(reader, batch, columnSetters.build(), task);
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

    protected ColumnSetterFactory newColumnSetterFactory(BatchInsert batch, PageReader pageReader,
            TimestampFormatter timestampFormatter)
    {
        return new ColumnSetterFactory(batch, pageReader, timestampFormatter);
    }

    protected PluginPageOutput newPluginPageOutput(PageReader reader,
                                                   BatchInsert batch, List<ColumnSetter> columnSetters,
                                                   PluginTask task)
    {
        return new PluginPageOutput(reader, batch, columnSetters, task.getBatchSize());
    }

    public static class PluginPageOutput
            implements TransactionalPageOutput
    {
        protected final List<Column> columns;
        protected final List<ColumnSetter> columnSetters;
        private final PageReader pageReader;
        private final BatchInsert batch;
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
                    handleColumnsSetters();
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

        protected void handleColumnsSetters()
        {
            int size = columnSetters.size();
            for (int i=0; i < size; i++) {
                columns.get(i).visit(columnSetters.get(i));
            }
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
                        //if (exception instanceof SQLException) {
                        //    SQLException ex = (SQLException) exception;
                        //    String sqlState = ex.getSQLState();
                        //    int errorCode = ex.getErrorCode();
                        //    return isRetryableSQLException(ex);
                        //}
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
