package org.embulk.output.oracle;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.TableIdentifier;

public class OracleOutputConnection
        extends JdbcOutputConnection
{
    private static final Map<String, String> CHARSET_NAMES = new HashMap<String, String>();
    static {
        CHARSET_NAMES.put("JA16SJIS", "MS932");
        CHARSET_NAMES.put("JA16SJISTILDE", "MS932");
        CHARSET_NAMES.put("JA16EUC", "EUC-JP");
        CHARSET_NAMES.put("JA16EUCTILDE", "EUC-JP");
        CHARSET_NAMES.put("AL32UTF8", "UTF-8");
        CHARSET_NAMES.put("UTF8", "UTF-8");
        CHARSET_NAMES.put("AL16UTF16", "UTF-16BE");
    }

    private final boolean direct;
    private OracleCharset charset;
    private OracleCharset nationalCharset;

    public OracleOutputConnection(Connection connection, String schemaName, boolean autoCommit, boolean direct)
            throws SQLException
    {
        super(connection, schemaName == null ? getSchema(connection) : schemaName);
        connection.setAutoCommit(autoCommit);

        this.direct = direct;
    }

    @Override
    protected String buildColumnTypeName(JdbcColumn c)
    {
        switch(c.getSimpleTypeName()) {
        case "BIGINT":
            return "NUMBER(19,0)";
        default:
            return super.buildColumnTypeName(c);
        }
    }

    @Override
    protected void setSearchPath(String schema) throws SQLException {
        if (!getSchema(connection).equals(schema)) {
            // Because old Oracle JDBC drivers don't support Connection#setSchema method.
            connection.setSchema(schema);
        }
    }

    @Override
    protected boolean supportsTableIfExistsClause()
    {
        return false;
    }

    private static String getSchema(Connection connection) throws SQLException
    {
        // Because old Oracle JDBC drivers don't support Connection#getSchema method.
        String sql = "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                if (resultSet.next()) {
                    return resultSet.getString(1);
                }
                throw new SQLException(String.format("Cannot get schema becase \"%s\" didn't return any value.", sql));
            }
        }
    }

    @Override
    protected String buildRenameTableSql(TableIdentifier fromTable, TableIdentifier toTable)
    {
        // ALTER TABLE doesn't support schema
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ");
        quoteIdentifierString(sb, fromTable.getTableName());
        sb.append(" RENAME TO ");
        quoteIdentifierString(sb, toTable.getTableName());
        return sb.toString();
    }

    @Override
    protected String buildPreparedInsertSql(TableIdentifier toTable, JdbcSchema toTableSchema) throws SQLException
    {
        String sql = super.buildPreparedInsertSql(toTable, toTableSchema);
        if (direct) {
            sql = sql.replaceAll("^INSERT ", "INSERT /*+ APPEND_VALUES */ ");
        }
        return sql;
    }

    @Override
    public Charset getTableNameCharset() throws SQLException
    {
        return getOracleCharset().getJavaCharset();
    }

    public synchronized OracleCharset getOracleCharset() throws SQLException
    {
        if (charset == null) {
            charset = getOracleCharset("NLS_CHARACTERSET", "UTF8");
        }
        return charset;
    }

    public synchronized OracleCharset getOracleNationalCharset() throws SQLException
    {
        if (nationalCharset == null) {
            nationalCharset = getOracleCharset("NLS_NCHAR_CHARACTERSET", "AL16UTF16");
        }
        return nationalCharset;
    }

    private OracleCharset getOracleCharset(String parameterName, String defaultCharsetName) throws SQLException
    {
        String charsetName = defaultCharsetName;
        try (PreparedStatement statement = connection.prepareStatement("SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER=?")) {
            statement.setString(1, parameterName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    String nlsCharacterSet = resultSet.getString(1);
                    if (CHARSET_NAMES.containsKey(nlsCharacterSet)) {
                        charsetName = nlsCharacterSet;
                    }
                }
            }
        }

        try (PreparedStatement statement = connection.prepareStatement("SELECT NLS_CHARSET_ID(?) FROM DUAL")) {
            statement.setString(1, charsetName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new SQLException("Unknown " + parameterName + " : " + charsetName);
                }

                return new OracleCharset(charsetName,
                        resultSet.getShort(1),
                        Charset.forName(CHARSET_NAMES.get(charsetName)));
            }
        }
    }


    private static final String[] SIZE_TYPE_NAMES = {
        "VARCHAR2", "NVARCHAR2",
    };

    @Override
    protected ColumnDeclareType getColumnDeclareType(String convertedTypeName, JdbcColumn col)
    {
        if (Arrays.asList(SIZE_TYPE_NAMES).contains(convertedTypeName)) {
            return ColumnDeclareType.SIZE;
        }
        return super.getColumnDeclareType(convertedTypeName, col);
    }

    @Override
    protected String buildCollectMergeSql(List<TableIdentifier> fromTables, JdbcSchema schema, TableIdentifier toTable, MergeConfig mergeConfig) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("MERGE INTO ");
        sb.append(quoteTableIdentifier(toTable));
        sb.append(" T");
        sb.append(" USING (");
        for (int i = 0; i < fromTables.size(); i++) {
            if (i != 0) { sb.append(" UNION ALL "); }
            sb.append(" SELECT ");
            sb.append(buildColumns(schema, ""));
            sb.append(" FROM ");
            sb.append(quoteTableIdentifier(fromTables.get(i)));
        }
        sb.append(") S");
        sb.append(" ON (");
        for (int i = 0; i < mergeConfig.getMergeKeys().size(); i++) {
            if (i != 0) { sb.append(" AND "); }
            String mergeKey = quoteIdentifierString(mergeConfig.getMergeKeys().get(i));
            sb.append("T.");
            sb.append(mergeKey);
            sb.append(" = S.");
            sb.append(mergeKey);
        }
        sb.append(")");
        sb.append(" WHEN MATCHED THEN");
        sb.append(" UPDATE SET ");
        if (mergeConfig.getMergeRule().isPresent()) {
            for (int i = 0; i < mergeConfig.getMergeRule().get().size(); i++) {
                if (i != 0) { sb.append(", "); }
                sb.append(mergeConfig.getMergeRule().get().get(i));
            }
        } else {
            int index = 0;
            for (int i = 0; i < schema.getCount(); i++) {
                String rawColumn = schema.getColumnName(i);
                if (mergeConfig.getMergeKeys().contains(rawColumn)) {
                    continue;
                }
                if (index++ != 0) { sb.append(", "); }
                String column = quoteIdentifierString(rawColumn);
                sb.append(column);
                sb.append(" = S.");
                sb.append(column);
            }
        }
        sb.append(" WHEN NOT MATCHED THEN");
        sb.append(" INSERT (");
        sb.append(buildColumns(schema, ""));
        sb.append(") VALUES (");
        sb.append(buildColumns(schema, "S."));
        sb.append(")");

        return sb.toString();
    }

    private String buildColumns(JdbcSchema schema, String prefix)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            sb.append(prefix);
            sb.append(quoteIdentifierString(schema.getColumnName(i)));
        }
        return sb.toString();
    }
}
