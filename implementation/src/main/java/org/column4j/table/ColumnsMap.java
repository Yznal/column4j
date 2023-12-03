package org.column4j.table;

import org.column4j.ColumnType;
import org.column4j.mutable.aggregated.AggregatedColumn;

import java.util.Collections;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class ColumnsMap {
    private final Map<String, AggregatedColumn<?, ?>> columns;

    public ColumnsMap(Map<String, AggregatedColumn<?, ?>> columns) {
        this.columns = Collections.unmodifiableMap(columns);
    }

    public <C extends AggregatedColumn<?, ?>> C getColumn(ColumnType columnType, String columnName) {
        var aggregatedColumn = columns.get(columnName);
        var columnActualType = aggregatedColumn.type();
        if (columnActualType != columnType) {
            throw new IllegalArgumentException("Column type is invalid, actual type is " + columnActualType);
        }
        return (C) aggregatedColumn;
    }
}
