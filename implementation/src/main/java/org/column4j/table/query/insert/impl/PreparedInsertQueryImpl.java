package org.column4j.table.query.insert.impl;

import org.column4j.ColumnType;
import org.column4j.mutable.MutableColumn;
import org.column4j.mutable.aggregated.*;
import org.column4j.table.ColumnsMap;
import org.column4j.table.query.insert.PreparedInsertQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class PreparedInsertQueryImpl implements PreparedInsertQuery {
    private final AtomicInteger cursor;
    private final ColumnsMap columnsMap;
    private final List<Map.Entry<String, BiConsumer<Integer, Object>>> columns;

    public PreparedInsertQueryImpl(AtomicInteger cursor, ColumnsMap columnsMap) {
        this.cursor = cursor;
        this.columnsMap = columnsMap;
        this.columns = new ArrayList<>();
    }

    @Override
    public PreparedInsertQuery columnInt8(String columnName) {
        AggregatedByteMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.INT8, columnName);
        assertCanInsertInto(mutableColumn, columnName);
        columns.add(Map.entry(columnName, (cursorIndex, value) -> mutableColumn.write(cursorIndex, (byte) value)));
        return this;
    }

    @Override
    public PreparedInsertQuery columnInt16(String columnName) {
        AggregatedShortMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.INT16, columnName);
        assertCanInsertInto(mutableColumn, columnName);
        columns.add(Map.entry(columnName, (cursorIndex, value) -> mutableColumn.write(cursorIndex, (short) value)));
        return this;
    }

    @Override
    public PreparedInsertQuery columnInt32(String columnName) {
        AggregatedIntMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.INT32, columnName);
        assertCanInsertInto(mutableColumn, columnName);
        columns.add(Map.entry(columnName, (cursorIndex, value) -> mutableColumn.write(cursorIndex, (int) value)));
        return this;
    }

    @Override
    public PreparedInsertQuery columnInt64(String columnName) {
        AggregatedLongMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.INT64, columnName);
        assertCanInsertInto(mutableColumn, columnName);
        columns.add(Map.entry(columnName, (cursorIndex, value) -> mutableColumn.write(cursorIndex, (long) value)));
        return this;
    }

    @Override
    public PreparedInsertQuery columnFloat32(String columnName) {
        AggregatedFloatMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.FLOAT32, columnName);
        assertCanInsertInto(mutableColumn, columnName);
        columns.add(Map.entry(columnName, (cursorIndex, value) -> mutableColumn.write(cursorIndex, (float) value)));
        return this;
    }

    @Override
    public PreparedInsertQuery columnFloat64(String columnName) {
        AggregatedDoubleMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.FLOAT64, columnName);
        assertCanInsertInto(mutableColumn, columnName);
        columns.add(Map.entry(columnName, (cursorIndex, value) -> mutableColumn.write(cursorIndex, (double) value)));
        return this;
    }

    @Override
    public PreparedInsertQuery columnString(String columnName) {
        AggregatedStringMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.STRING, columnName);
        assertCanInsertInto(mutableColumn, columnName);
        columns.add(Map.entry(columnName, (cursorIndex, value) -> mutableColumn.write(cursorIndex, (String) value)));
        return this;
    }

    private void assertCanInsertInto(MutableColumn<?> mutableColumn, String columnName) {
        if (mutableColumn == null) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist or has another type");
        }
        if (columns.stream().anyMatch(it -> it.getKey().equals(columnName))) {
            throw new IllegalArgumentException("Column " + columnName + " already exists");
        }
    }

    @Override
    public void commit(Object[] values) {
        if (values.length != columns.size()) {
            throw new IllegalArgumentException("Number of values does not match number of columns");
        }
        var cursorIndex = cursor.getAndIncrement();
        for (int i = 0; i < values.length; i++) {
            var columnMeta = columns.get(i);
            var columnWriter = columnMeta.getValue();
            columnWriter.accept(cursorIndex, values[i]);
        }
    }
}
