package org.embulk.output.oracle;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcSchema;

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
    public void dropTableIfExists(String tableName) throws SQLException
    {
        if (tableExists(tableName)) {
            dropTable(tableName);
        }
    }

    @Override
    protected void dropTableIfExists(Statement stmt, String tableName) throws SQLException {
        if (tableExists(tableName)) {
            dropTable(stmt, tableName);
        }
    }

    @Override
    public void createTableIfNotExists(String tableName, JdbcSchema schema) throws SQLException
    {
        if (!tableExists(tableName)) {
            createTable(tableName, schema);
        }
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
    protected String buildPreparedInsertSql(String toTable, JdbcSchema toTableSchema) throws SQLException
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
}
