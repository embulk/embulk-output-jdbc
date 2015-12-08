package org.embulk.output.jdbc;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.embulk.spi.Exec;

public class JdbcUtils
{
    public final Logger logger = Exec.getLogger(JdbcUtils.class.getName());

    private static String[] SEARCH_STRING_SPECIAL_CHARS = new String[] { "_", "%" };

    public static String escapeSearchString(String searchString, String escapeString)
    {
        if (searchString != null && escapeString != null) {
            // First of all, escape escapeString '\' itself: '\' -> '\\'
            searchString = searchString.replaceAll(Pattern.quote(escapeString),
                    Matcher.quoteReplacement(escapeString + escapeString));
            for (String specialChar : SEARCH_STRING_SPECIAL_CHARS) {
                if (specialChar.equals(escapeString)) {
                    throw new IllegalArgumentException("Special char " + specialChar + " cannot be an escape char");
                }
                searchString = searchString.replaceAll(Pattern.quote(specialChar),
                        Matcher.quoteReplacement(escapeString + specialChar));
            }
        }
        return searchString;
    }

    private Class<?> connectionClass;

    // Connection.isValid() is available from Java 1.6 + JDBC4.
    //private boolean hasIsValid;

    // Connection.setNetworkTimeout() is available from Java 1.7 + JDBC4.
    //private boolean hasSetNetworkTimeout;
    //private Method setNetworkTimeoutMethod;

    public JdbcUtils(Class<?> connectionClass)
    {
        this.connectionClass = connectionClass;

        //configureSetNetworkTimeout();
    }

    public int executeUpdateWithSqlLogging(Statement stmt, String sql) throws SQLException
    {
        logger.info("SQL: " + sql);
        long startTime = System.currentTimeMillis();
        int count = stmt.executeUpdate(sql);
        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
        if (count == 0) {
            logger.info(String.format("> %.2f seconds", seconds));
        } else {
            logger.info(String.format("> %.2f seconds (%,d rows)", seconds, count));
        }
        return count;
    }

    //private void configureSetNetworkTimeout() {
    //    try {
    //        Method m = connectionClass.getMethod("setNetworkTimeout", Executor.class, int.class);
    //        if (isCallableMethod(m)) {
    //            setNetworkTimeoutMethod = m;
    //            hasSetNetworkTimeout = true;
    //        }
    //    } catch (SecurityException ex) {
    //    } catch (NoSuchMethodException ex) {
    //    }
    //}

    //private boolean isCallableMethod(Method m) {
    //    int modifiers = m.getModifiers();
    //    if (Modifier.isAbstract(modifiers)) {
    //        // Method.invoke throws java.lang.AbstractMethodError if it's an
    //        // abstract method. Applications can't catch AbstractMethodError.
    //        return false;
    //    }
    //    if (!Modifier.isPublic(modifiers)) {
    //        // we can only call public methods
    //        return false;
    //    }
    //    return true;
    //}

    // PostgreSQL JDBC driver implements isValid() method. But the
    // implementation throws following exception:
    // "java.io.IOException: Method org.postgresql.jdbc4.Jdbc4Connection.isValid(int) is not yet implemented."
    //
    // So, checking mechanism doesn't work at all.
    // Thus here just runs "SELECT 1" to check connectivity.
    //
    //public boolean isValidConnection(Connection connection, int timeout) throws SQLException
    //{
    //    Statement stmt = connection.createStatement();
    //    try {
    //        stmt.executeQuery("SELECT 1").close();
    //        return true;
    //    } catch (SQLException ex) {
    //        return false;
    //    } finally {
    //        stmt.close();
    //    }
    //}

    //public void setNetworkTimeout(Connection connection,
    //        Executor executor, int milliseconds) throws SQLException {
    //    Throwable exception = null;
    //    if (hasSetNetworkTimeout) {
    //        try {
    //            setNetworkTimeoutMethod.invoke(connection, executor, milliseconds);
    //            return;
    //
    //        } catch (IllegalArgumentException ex) {
    //            // ignore error
    //            LOG.warn("Connection.setNetworkTimeout failed due to IllegalArgumentException.");
    //            exception = ex;
    //
    //        } catch (IllegalAccessException ex) {
    //            // ignore error
    //            LOG.warn("Connection.setNetworkTimeout failed due to IllegalAccessException.");
    //            exception = ex;
    //
    //        } catch (InvocationTargetException ex) {
    //            //Throwable cause = ex.getTargetException();
    //            //if (cause instanceof SQLException) {
    //            //    throw (SQLException) cause;
    //            //} else if (cause instanceof RuntimeException) {
    //            //    throw (RuntimeException) cause;
    //            //} else if (cause instanceof Error) {
    //            //    throw (Error) cause;
    //            //} else {
    //            //    throw new SQLException(cause);
    //            //}
    //            exception = ex.getTargetException();
    //            // It's safe to ignore exceptions.
    //        }
    //
    //        hasSetNetworkTimeout = false;
    //    }
    //
    //    if (exception != null) {
    //        LOG.warn("Connection.setNetworkTimeout is not available: "+exception);
    //    } else {
    //        LOG.warn("Connection.setNetworkTimeout is not available.");
    //    }
    //    // TODO any substitute implementations?
    //}
}

