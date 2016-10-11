package org.embulk.output;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.embulk.output.tester.EmbulkPluginTester;

import com.google.common.io.Files;

public abstract class AbstractJdbcOutputPluginTest
{
    protected static boolean enabled;
    protected static EmbulkPluginTester tester = new EmbulkPluginTester();

    protected void dropTable(String table) throws SQLException
    {
        String sql = String.format("DROP TABLE %s", table);
        executeSQL(sql, true);
    }

    protected List<List<Object>> select(String table) throws SQLException
    {
        try (Connection connection = connect()) {
            try (Statement statement = connection.createStatement()) {
                List<List<Object>> rows = new ArrayList<List<Object>>();
                String sql = String.format("SELECT * FROM %s", table);
                System.out.println(sql);
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    while (resultSet.next()) {
                        List<Object> row = new ArrayList<Object>();
                        for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                            row.add(getValue(resultSet, i));
                        }
                        rows.add(row);
                    }
                }
                // cannot sort by CLOB, so sort by Java
                Collections.sort(rows, new Comparator<List<Object>>() {
                    @Override
                    public int compare(List<Object> o1, List<Object> o2) {
                        return o1.toString().compareTo(o2.toString());
                    }
                });
                return rows;
            }
        }

    }

    protected Object getValue(ResultSet resultSet, int index) throws SQLException
    {
        return resultSet.getObject(index);
    }

    protected void executeSQL(String sql) throws SQLException
    {
        executeSQL(sql, false);
    }

    protected void executeSQL(String sql, boolean ignoreError) throws SQLException
    {
        if (!enabled) {
            return;
        }

        try (Connection connection = connect()) {
            try {
                connection.setAutoCommit(true);

                try (Statement statement = connection.createStatement()) {
                    System.out.println(String.format("Execute SQL : \"%s\".", sql));
                    statement.execute(sql);
                }

            } catch (SQLException e) {
                if (!ignoreError) {
                    throw e;
                }
            }
        }
    }

    protected void test(String ymlPath) throws Exception
    {
        if (!enabled) {
            return;
        }

        tester.run(convertYml(ymlPath));
    }

    protected String convertYml(String ymlName) throws Exception
    {
        StringBuilder builder = new StringBuilder();
        Pattern pathPrefixPattern = Pattern.compile("^ *path(_prefix)?: '(.*)'$");
        for (String line : Files.readLines(convertPath(ymlName), Charset.forName("UTF8"))) {
            line = convertYmlLine(line);
            Matcher matcher = pathPrefixPattern.matcher(line);
            if (matcher.matches()) {
                int group = 2;
                builder.append(line.substring(0, matcher.start(group)));
                builder.append(convertPath(matcher.group(group)).getAbsolutePath());
                builder.append(line.substring(matcher.end(group)));
            } else {
                builder.append(line);
            }
            builder.append(System.lineSeparator());
        }
        return builder.toString();
    }

    protected String convertYmlLine(String line)
    {
        return line;
    }

    protected File convertPath(String name) throws URISyntaxException
    {
        return new File(getClass().getResource(name).toURI());
    }

    protected abstract Connection connect() throws SQLException;

}
