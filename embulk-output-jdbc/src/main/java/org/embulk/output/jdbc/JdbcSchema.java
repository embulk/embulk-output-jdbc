package org.embulk.output.jdbc;

import java.util.List;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
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
        for (JdbcColumn column : columns) {
            if (column.getName().equals(name)) {
                return Optional.of(column);
            }
        }
        return Optional.absent();
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
        ImmutableList.Builder<JdbcColumn> builder = ImmutableList.builder();
        for (JdbcColumn c : schema.getColumns()) {
            if (!c.isSkipColumn()) {
                builder.add(c);
            }
        }
        return new JdbcSchema(builder.build());
    }
}
