package org.embulk.output.jdbc;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.sql.Types;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.joda.time.DateTimeZone;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.plugin.PluginClassLoader;
import org.embulk.spi.Exec;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.output.jdbc.setter.ColumnSetter;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.embulk.output.jdbc.setter.ColumnSetterVisitor;
import org.embulk.spi.util.RetryExecutor.Retryable;

import static org.embulk.spi.util.RetryExecutor.retryExecutor;
import static org.embulk.output.jdbc.JdbcSchema.filterSkipColumns;

public abstract class AbstractJdbcOutputPlugin
        implements OutputPlugin
{
    private final static Set<String> loadedJarGlobs = new HashSet<String>();

    protected final Logger logger = Exec.getLogger(getClass());

    public interface PluginTask
            extends Task
    {
        @Config("options")
        @ConfigDefault("{}")
        public ToStringMap getOptions();

        @Config("table")
        public String getTable();

        @Config("mode")
        public Mode getMode();

        @Config("batch_size")
        @ConfigDefault("16777216")
        // TODO set minimum number
        public int getBatchSize();

        @Config("merge_keys")
        @ConfigDefault("null")
        public Optional<List<String>> getMergeKeys();

        @Config("column_options")
        @ConfigDefault("{}")
        public Map<String, JdbcColumnOption> getColumnOptions();

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public DateTimeZone getDefaultTimeZone();

        @Config("on_duplicate_key_update")
        @ConfigDefault("null")
        public Optional<String> getOnDuplicateKeyUpdate();

        public void setMergeKeys(Optional<List<String>> keys);

        public void setFeatures(Features features);
        public Features getFeatures();

        public Optional<JdbcSchema> getNewTableSchema();
        public void setNewTableSchema(Optional<JdbcSchema> schema);

        public JdbcSchema getTargetTableSchema();
        public void setTargetTableSchema(JdbcSchema schema);

        public Optional<List<String>> getIntermediateTables();
        public void setIntermediateTables(Optional<List<String>> names);
    }

    public static enum LengthSemantics
    {
        BYTES {
            @Override
            public int countLength(Charset charset, String s)
            {
                return charset.encode(s).remaining();
            }
        },
        CHARACTERS {
            @Override
            public int countLength(Charset charset, String s)
            {
                return s.length();
            }
        };

        public abstract int countLength(Charset charset, String s);
    }

    public static class Features
    {
        private int maxTableNameLength = 64;
        private LengthSemantics tableNameLengthSemantics = LengthSemantics.BYTES;
        private Set<Mode> supportedModes = ImmutableSet.copyOf(Mode.values());
        private boolean ignoreMergeKeys = false;

        public Features()
        { }

        @JsonProperty
        public int getMaxTableNameLength()
        {
            return maxTableNameLength;
        }

        @JsonProperty
        public Features setMaxTableNameLength(int bytes)
        {
            this.maxTableNameLength = bytes;
            return this;
        }

        public LengthSemantics getTableNameLengthSemantics()
        {
            return tableNameLengthSemantics;
        }

        @JsonProperty
        public Features setTableNameLengthSemantics(LengthSemantics tableNameLengthSemantics)
        {
            this.tableNameLengthSemantics = tableNameLengthSemantics;
            return this;
        }

        @JsonProperty
        public Set<Mode> getSupportedModes()
        {
            return supportedModes;
        }

        @JsonProperty
        public Features setSupportedModes(Set<Mode> modes)
        {
            this.supportedModes = modes;
            return this;
        }

        @JsonProperty
        public boolean getIgnoreMergeKeys()
        {
            return ignoreMergeKeys;
        }

        @JsonProperty
        public Features setIgnoreMergeKeys(boolean value)
        {
            this.ignoreMergeKeys = value;
            return this;
        }
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

    protected abstract Features getFeatures(PluginTask task);

    protected abstract JdbcOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation);

    protected abstract BatchInsert newBatchInsert(PluginTask task, Optional<List<String>> mergeKeys) throws IOException, SQLException;

    protected JdbcOutputConnection newConnection(PluginTask task, boolean retryableMetadataOperation,
            boolean autoCommit) throws SQLException
    {
        return getConnector(task, retryableMetadataOperation).connect(autoCommit);
    }

    public enum Mode {
        INSERT,
        INSERT_DIRECT,
        MERGE,
        MERGE_DIRECT,
        TRUNCATE_INSERT,
        REPLACE;

        @JsonValue
        @Override
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        public static Mode fromString(String value)
        {
            switch(value) {
            case "insert":
                return INSERT;
            case "insert_direct":
                return INSERT_DIRECT;
            case "merge":
                return MERGE;
            case "merge_direct":
                return MERGE_DIRECT;
            case "truncate_insert":
                return TRUNCATE_INSERT;
            case "replace":
                return REPLACE;
            default:
                throw new ConfigException(String.format("Unknown mode '%s'. Supported modes are insert, insert_direct, merge, merge_direct, truncate_insert, replace", value));
            }
        }

        /**
         * True if this mode directly modifies the target table without creating intermediate tables.
         */
        public boolean isDirectModify()
        {
            return this == INSERT_DIRECT || this == MERGE_DIRECT;
        }

        /**
         * True if this mode merges records on unique keys
         */
        public boolean isMerge()
        {
            return this == MERGE || this == MERGE_DIRECT;
        }

        /**
         * True if this mode creates intermediate table for each tasks.
         */
        public boolean tempTablePerTask()
        {
            return this == INSERT || this == MERGE || this == TRUNCATE_INSERT /*this == REPLACE_VIEW*/;
        }

        /**
         * True if this mode truncates the target table before committing intermediate tables
         */
        public boolean truncateBeforeCommit()
        {
            return this == TRUNCATE_INSERT;
        }

        /**
         * True if this mode uses MERGE statement to commit intermediate tables to the target table
         */
        public boolean commitByMerge()
        {
            return this == MERGE;
        }

        /**
         * True if this mode overwrites schema of the target tables
         */
        public boolean ignoreTargetTableSchema()
        {
            return this == REPLACE /*|| this == REPLACE_VIEW*/;
        }

        /**
         * True if this mode swaps the target tables with intermediate tables to commit
         */
        public boolean commitBySwapTable()
        {
            return this == REPLACE;
        }
    }

    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(getTaskClass());
        Features features = getFeatures(task);
        task.setFeatures(features);

        if (!features.getSupportedModes().contains(task.getMode())) {
            throw new ConfigException(String.format("This output type doesn't support '%s'. Supported modes are: %s", task.getMode(), features.getSupportedModes()));
        }

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
            final Schema schema, final int taskCount)
    {
        try {
            withRetry(new IdempotentSqlRunnable() {  // no intermediate data if isDirectModify == true
                public void run() throws SQLException
                {
                    JdbcOutputConnection con = newConnection(task, true, false);
                    try {
                        doBegin(con, task, schema, taskCount);
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
            final List<TaskReport> successTaskReports)
    {
        final PluginTask task = taskSource.loadTask(getTaskClass());

        if (!task.getMode().isDirectModify()) {  // no intermediate data if isDirectModify == true
            try {
                withRetry(new IdempotentSqlRunnable() {
                    public void run() throws SQLException
                    {
                        JdbcOutputConnection con = newConnection(task, true, true);
                        try {
                            doCleanup(con, task, taskCount, successTaskReports);
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
            PluginTask task, final Schema schema, int taskCount) throws SQLException
    {
        if (schema.getColumnCount() == 0) {
            throw new ConfigException("task count == 0 is not supported");
        }

        Mode mode = task.getMode();
        logger.info("Using {} mode", mode);

        Optional<JdbcSchema> initialTargetTableSchema =
            mode.ignoreTargetTableSchema() ?
                Optional.<JdbcSchema>absent() :
                newJdbcSchemaFromTableIfExists(con, task.getTable());

        // TODO get CREATE TABLE statement from task if set
        JdbcSchema newTableSchema = applyColumnOptionsToNewTableSchema(
                initialTargetTableSchema.or(new Supplier<JdbcSchema>() {
                    public JdbcSchema get()
                    {
                        return newJdbcSchemaForNewTable(schema);
                    }
                }),
                task.getColumnOptions());

        // create intermediate tables
        if (!mode.isDirectModify()) {
            // direct modify mode doesn't need intermediate tables.
            ImmutableList.Builder<String> intermTableNames = ImmutableList.builder();
            if (mode.tempTablePerTask()) {
                String namePrefix = generateIntermediateTableNamePrefix(task.getTable(), con, 3,
                        task.getFeatures().getMaxTableNameLength(), task.getFeatures().getTableNameLengthSemantics());
                for (int i=0; i < taskCount; i++) {
                    intermTableNames.add(namePrefix + String.format("%03d", i));
                }
            } else {
                String name = generateIntermediateTableNamePrefix(task.getTable(), con, 0,
                        task.getFeatures().getMaxTableNameLength(), task.getFeatures().getTableNameLengthSemantics());
                intermTableNames.add(name);
            }
            // create the intermediate tables here
            task.setIntermediateTables(Optional.<List<String>>of(intermTableNames.build()));
            for (String name : task.getIntermediateTables().get()) {
                // DROP TABLE IF EXISTS xyz__0000000054d92dee1e452158_bulk_load_temp
                con.dropTableIfExists(name);
                // CREATE TABLE IF NOT EXISTS xyz__0000000054d92dee1e452158_bulk_load_temp
                con.createTableIfNotExists(name, newTableSchema);
            }
        } else {
            task.setIntermediateTables(Optional.<List<String>>absent());
        }

        // build JdbcSchema from a table
        JdbcSchema targetTableSchema;
        if (initialTargetTableSchema.isPresent()) {
            targetTableSchema = initialTargetTableSchema.get();
            task.setNewTableSchema(Optional.<JdbcSchema>absent());
        } else if (task.getIntermediateTables().isPresent() && !task.getIntermediateTables().get().isEmpty()) {
            String firstItermTable = task.getIntermediateTables().get().get(0);
            targetTableSchema = newJdbcSchemaFromTableIfExists(con, firstItermTable).get();
            task.setNewTableSchema(Optional.of(newTableSchema));
        } else {
            // also create the target table if not exists
            // CREATE TABLE IF NOT EXISTS xyz
            con.createTableIfNotExists(task.getTable(), newTableSchema);
            targetTableSchema = newJdbcSchemaFromTableIfExists(con, task.getTable()).get();
            task.setNewTableSchema(Optional.<JdbcSchema>absent());
        }
        task.setTargetTableSchema(matchSchemaByColumnNames(schema, targetTableSchema));

        // validate column_options
        newColumnSetters(
                newColumnSetterFactory(null, task.getDefaultTimeZone()),  // TODO create a dummy BatchInsert
                task.getTargetTableSchema(), schema,
                task.getColumnOptions());

        // normalize merge_key parameter for merge modes
        if (mode.isMerge()) {
            Optional<List<String>> mergeKeys = task.getMergeKeys();
            if (task.getFeatures().getIgnoreMergeKeys()) {
                if (mergeKeys.isPresent()) {
                    throw new ConfigException("This output type does not accept 'merge_key' option.");
                }
                task.setMergeKeys(Optional.<List<String>>of(ImmutableList.<String>of()));
            } else if (mergeKeys.isPresent()) {
                if (task.getMergeKeys().get().isEmpty()) {
                    throw new ConfigException("Empty 'merge_keys' option is invalid.");
                }
                for (String key : mergeKeys.get()) {
                    if (!targetTableSchema.findColumn(key).isPresent()) {
                        throw new ConfigException(String.format("Merge key '%s' does not exist in the target table.", key));
                    }
                }
            } else {
                ImmutableList.Builder<String> builder = ImmutableList.builder();
                for (JdbcColumn column : targetTableSchema.getColumns()) {
                    if (column.isUniqueKey()) {
                        builder.add(column.getName());
                    }
                }
                task.setMergeKeys(Optional.<List<String>>of(builder.build()));
                if (task.getMergeKeys().get().isEmpty()) {
                    throw new ConfigException("Merging mode is used but the target table does not have primary keys. Please set merge_keys option.");
                }
            }
            logger.info("Using merge keys: {}", task.getMergeKeys().get());
        } else {
            task.setMergeKeys(Optional.<List<String>>absent());
        }
    }

    protected ColumnSetterFactory newColumnSetterFactory(BatchInsert batch, DateTimeZone defaultTimeZone)
    {
        return new ColumnSetterFactory(batch, defaultTimeZone);
    }

    protected String generateIntermediateTableNamePrefix(String baseTableName, JdbcOutputConnection con,
            int suffixLength, int maxLength, LengthSemantics lengthSemantics) throws SQLException
    {
        Charset tableNameCharset = con.getTableNameCharset();
        String tableName = baseTableName;
        String suffix = "_bl_tmp";
        String uniqueSuffix = getTransactionUniqueName() + suffix;

        // way to count length of table name varies by DBMSs (bytes or characters),
        // so truncate swap table name by one character.
        while (!checkTableNameLength(tableName + "_" + uniqueSuffix, tableNameCharset, suffixLength, maxLength, lengthSemantics)) {
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

    private static JdbcSchema applyColumnOptionsToNewTableSchema(JdbcSchema schema, final Map<String, JdbcColumnOption> columnOptions)
    {
        return new JdbcSchema(Lists.transform(schema.getColumns(), new Function<JdbcColumn, JdbcColumn>() {
            public JdbcColumn apply(JdbcColumn c)
            {
                JdbcColumnOption option = columnOptionOf(columnOptions, c);
                if (option.getType().isPresent()) {
                    return JdbcColumn.newTypeDeclaredColumn(
                            c.getName(), Types.OTHER,  // sqlType, isNotNull, and isUniqueKey are ignored
                            option.getType().get(), false, false);
                }
                return c;
            }
        }));
    }

    protected static List<ColumnSetter> newColumnSetters(ColumnSetterFactory factory,
            JdbcSchema targetTableSchema, Schema inputValueSchema,
            Map<String, JdbcColumnOption> columnOptions)
    {
        ImmutableList.Builder<ColumnSetter> builder = ImmutableList.builder();
        int schemaColumnIndex = 0;
        for (JdbcColumn targetColumn : targetTableSchema.getColumns()) {
            if (targetColumn.isSkipColumn()) {
                builder.add(factory.newSkipColumnSetter());
            } else {
                //String columnOptionKey = inputValueSchema.getColumn(schemaColumnIndex).getName();
                JdbcColumnOption option = columnOptionOf(columnOptions, targetColumn);
                builder.add(factory.newColumnSetter(targetColumn, option));
                schemaColumnIndex++;
            }
        }
        return builder.build();
    }

    private static JdbcColumnOption columnOptionOf(Map<String, JdbcColumnOption> columnOptions, JdbcColumn targetColumn)
    {
        return Optional.fromNullable(columnOptions.get(targetColumn.getName())).or(
                    // default column option
                    new Supplier<JdbcColumnOption>()
                    {
                        public JdbcColumnOption get()
                        {
                            return Exec.newConfigSource().loadConfig(JdbcColumnOption.class);
                        }
                    });
    }

    private boolean checkTableNameLength(String tableName, Charset tableNameCharset,
            int suffixLength, int maxLength, LengthSemantics lengthSemantics)
    {
        return lengthSemantics.countLength(tableNameCharset, tableName) + suffixLength <= maxLength;
    }

    protected void doCommit(JdbcOutputConnection con, PluginTask task, int taskCount)
        throws SQLException
    {
        if (task.getIntermediateTables().get().isEmpty()) {
            return;
        }

        JdbcSchema schema = filterSkipColumns(task.getTargetTableSchema());

        switch (task.getMode()) {
        case INSERT_DIRECT:
        case MERGE_DIRECT:
            // already done
            break;

        case INSERT:
            // aggregate insert into target
            if (task.getNewTableSchema().isPresent()) {
                con.createTableIfNotExists(task.getTable(), task.getNewTableSchema().get());
            }
            con.collectInsert(task.getIntermediateTables().get(), schema, task.getTable(), false);
            break;

        case TRUNCATE_INSERT:
            // truncate & aggregate insert into target
            if (task.getNewTableSchema().isPresent()) {
                con.createTableIfNotExists(task.getTable(), task.getNewTableSchema().get());
            }
            con.collectInsert(task.getIntermediateTables().get(), schema, task.getTable(), true);
            break;

        case MERGE:
            // aggregate merge into target
            if (task.getNewTableSchema().isPresent()) {
                con.createTableIfNotExists(task.getTable(), task.getNewTableSchema().get());
            }
            con.collectMerge(task.getIntermediateTables().get(), schema, task.getTable(), task.getMergeKeys().get(), task.getOnDuplicateKeyUpdate());
            break;

        case REPLACE:
            // swap table
            con.replaceTable(task.getIntermediateTables().get().get(0), schema, task.getTable());
            break;
        }
    }

    protected void doCleanup(JdbcOutputConnection con, PluginTask task, int taskCount,
            List<TaskReport> successTaskReports)
        throws SQLException
    {
        if (task.getIntermediateTables().isPresent()) {
            for (String intermTable : task.getIntermediateTables().get()) {
                con.dropTableIfExists(intermTable);
            }
        }
    }

    protected JdbcSchema newJdbcSchemaForNewTable(Schema schema)
    {
        final ImmutableList.Builder<JdbcColumn> columns = ImmutableList.builder();
        for (Column c : schema.getColumns()) {
            final String columnName = c.getName();
            c.visit(new ColumnVisitor() {
                public void booleanColumn(Column column)
                {
                    columns.add(JdbcColumn.newGenericTypeColumn(
                            columnName, Types.BOOLEAN, "BOOLEAN",
                            1, 0, false, false));
                }

                public void longColumn(Column column)
                {
                    columns.add(JdbcColumn.newGenericTypeColumn(
                            columnName, Types.BIGINT, "BIGINT",
                            22, 0, false, false));
                }

                public void doubleColumn(Column column)
                {
                    columns.add(JdbcColumn.newGenericTypeColumn(
                            columnName, Types.FLOAT, "DOUBLE PRECISION",
                            24, 0, false, false));
                }

                public void stringColumn(Column column)
                {
                    columns.add(JdbcColumn.newGenericTypeColumn(
                            columnName, Types.CLOB, "CLOB",
                            4000, 0, false, false));  // TODO size type param
                }

                public void jsonColumn(Column column) {
                    throw new UnsupportedOperationException("This plugin doesn't support json type. Please try to upgrade version of the plugin using 'embulk gem update' command. If the latest version still doesn't support json type, please contact plugin developers, or change configuration of input plugin not to use json type.");
                }

                public void timestampColumn(Column column)
                {
                    columns.add(JdbcColumn.newGenericTypeColumn(
                            columnName, Types.TIMESTAMP, "TIMESTAMP",
                            26, 0, false, false));  // size type param is from postgresql
                }
            });
        }
        return new JdbcSchema(columns.build());
    }

    public Optional<JdbcSchema> newJdbcSchemaFromTableIfExists(JdbcOutputConnection connection,
            String tableName) throws SQLException
    {
        if (!connection.tableExists(tableName)) {
            // DatabaseMetaData.getPrimaryKeys fails if table does not exist
            return Optional.absent();
        }

        DatabaseMetaData dbm = connection.getMetaData();
        String escape = dbm.getSearchStringEscape();

        ResultSet rs = dbm.getPrimaryKeys(null, connection.getSchemaName(), tableName);
        ImmutableSet.Builder<String> primaryKeysBuilder = ImmutableSet.builder();
        try {
            while(rs.next()) {
                primaryKeysBuilder.add(rs.getString("COLUMN_NAME"));
            }
        } finally {
            rs.close();
        }
        ImmutableSet<String> primaryKeys = primaryKeysBuilder.build();

        ImmutableList.Builder<JdbcColumn> builder = ImmutableList.builder();
        rs = dbm.getColumns(null,
                JdbcUtils.escapeSearchString(connection.getSchemaName(), escape),
                JdbcUtils.escapeSearchString(tableName, escape),
                null);
        try {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String simpleTypeName = rs.getString("TYPE_NAME").toUpperCase(Locale.ENGLISH);
                boolean isUniqueKey = primaryKeys.contains(columnName);
                int sqlType = rs.getInt("DATA_TYPE");
                int colSize = rs.getInt("COLUMN_SIZE");
                int decDigit = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    decDigit = -1;
                }
                int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
                boolean isNotNull = "NO".equals(rs.getString("IS_NULLABLE"));
                //rs.getString("COLUMN_DEF") // or null  // TODO
                builder.add(JdbcColumn.newGenericTypeColumn(
                            columnName, sqlType, simpleTypeName, colSize, decDigit, charOctetLength, isNotNull, isUniqueKey));
                // We can't get declared column name using JDBC API.
                // Subclasses need to overwrite it.
            }
        } finally {
            rs.close();
        }
        List<JdbcColumn> columns = builder.build();
        if (columns.isEmpty()) {
            return Optional.absent();
        } else {
            return Optional.of(new JdbcSchema(columns));
        }
    }

    private JdbcSchema matchSchemaByColumnNames(Schema inputSchema, JdbcSchema targetTableSchema)
    {
        ImmutableList.Builder<JdbcColumn> jdbcColumns = ImmutableList.builder();

        for (Column column : inputSchema.getColumns()) {
            Optional<JdbcColumn> c = targetTableSchema.findColumn(column.getName());
            jdbcColumns.add(c.or(JdbcColumn.skipColumn()));
        }

        return new JdbcSchema(jdbcColumns.build());
    }

    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, final int taskIndex)
    {
        final PluginTask task = taskSource.loadTask(getTaskClass());
        final Mode mode = task.getMode();

        // instantiate BatchInsert without table name
        BatchInsert batch = null;
        try {
            batch = newBatchInsert(task,
                    task.getMode() == Mode.MERGE_DIRECT ?
                        task.getMergeKeys() :
                        Optional.<List<String>>absent());
        } catch (IOException | SQLException ex) {
            throw new RuntimeException(ex);
        }

        try {
            // configure PageReader -> BatchInsert
            PageReader reader = new PageReader(schema);

            List<ColumnSetter> columnSetters = newColumnSetters(
                    newColumnSetterFactory(batch, task.getDefaultTimeZone()),
                    task.getTargetTableSchema(), schema,
                    task.getColumnOptions());
            JdbcSchema insertIntoSchema = filterSkipColumns(task.getTargetTableSchema());

            // configure BatchInsert -> an intermediate table (!isDirectModify) or the target table (isDirectModify)
            String destTable;
            if (mode.tempTablePerTask()) {
                destTable = task.getIntermediateTables().get().get(taskIndex);
            } else if (mode.isDirectModify()) {
                destTable = task.getTable();
            } else {
                destTable = task.getIntermediateTables().get().get(0);
            }
            batch.prepare(destTable, insertIntoSchema);

            PluginPageOutput output = new PluginPageOutput(reader, batch, columnSetters, task.getBatchSize());
            batch = null;
            return output;

        } catch (SQLException ex) {
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
        protected final List<Column> columns;
        protected final List<ColumnSetterVisitor> columnVisitors;
        private final PageReader pageReader;
        private final BatchInsert batch;
        private final int batchSize;
        private final int forceBatchFlushSize;

        public PluginPageOutput(final PageReader pageReader,
                BatchInsert batch, List<ColumnSetter> columnSetters,
                int batchSize)
        {
            this.pageReader = pageReader;
            this.batch = batch;
            this.columns = pageReader.getSchema().getColumns();
            this.columnVisitors = ImmutableList.copyOf(Lists.transform(
                        columnSetters, new Function<ColumnSetter, ColumnSetterVisitor>() {
                            public ColumnSetterVisitor apply(ColumnSetter setter)
                            {
                                return new ColumnSetterVisitor(pageReader, setter);
                            }
                        }));
            this.batchSize = batchSize;
            this.forceBatchFlushSize = batchSize * 2;
        }

        @Override
        public void add(Page page)
        {
            try {
                pageReader.setPage(page);
                while (pageReader.nextRecord()) {
                    if (batch.getBatchWeight() > forceBatchFlushSize) {
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
        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }

        protected void handleColumnsSetters()
        {
            int size = columnVisitors.size();
            for (int i=0; i < size; i++) {
                columns.get(i).visit(columnVisitors.get(i));
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
                .withRetryLimit(12)
                .withInitialRetryWait(1000)
                .withMaxRetryWait(30 * 60 * 1000)
                .runInterruptible(new Retryable<Void>() {
                    public Void call() throws Exception
                    {
                        op.run();
                        return null;
                    }

                    public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
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

                    public void onGiveup(Exception firstException, Exception lastException)
                    {
                        if (firstException instanceof SQLException) {
                            SQLException ex = (SQLException) firstException;
                            String sqlState = ex.getSQLState();
                            int errorCode = ex.getErrorCode();
                            logger.error("{} ({}:{})", errorMessage, errorCode, sqlState);
                        }
                    }

                    public boolean isRetryableException(Exception exception)
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
