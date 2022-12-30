package org.embulk.output.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.embulk.config.ConfigException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class JdbcSchema
{
    private List<JdbcColumn> columns;

    @JsonCreator
    public JdbcSchema(List<JdbcColumn> columns)
    {
        this.columns = columns;
    }

    @JsonValue
    public List<JdbcColumn> getColumns()
    {
        return columns;
    }

    public Optional<JdbcColumn> findColumn(String name)
    {
        // because both upper case column and lower case column may exist, search twice
        for (JdbcColumn column : columns) {
            if (column.getName().equals(name)) {
                return Optional.of(column);
            }
        }

        JdbcColumn foundColumn = null;
        for (JdbcColumn column : columns) {
            if (column.getName().equalsIgnoreCase(name)) {
                if (foundColumn != null) {
                    throw new ConfigException(String.format("Cannot specify column '%s' because both '%s' and '%s' exist.",
                            name, foundColumn.getName(), column.getName()));
                }
                foundColumn = column;
            }
        }

        if (foundColumn != null) {
            return Optional.of(foundColumn);
        }
        return Optional.empty();
    }

    public int getCount()
    {
        return columns.size();
    }

    public JdbcColumn getColumn(int i)
    {
        return columns.get(i);
    }

    public String getColumnName(int i)
    {
        return columns.get(i).getName();
    }

    public static JdbcSchema filterSkipColumns(JdbcSchema schema)
    {
        final ArrayList<JdbcColumn> builder = new ArrayList<>();
        for (JdbcColumn c : schema.getColumns()) {
            if (!c.isSkipColumn()) {
                builder.add(c);
            }
        }
        return new JdbcSchema(Collections.unmodifiableList(builder));
    }
}
