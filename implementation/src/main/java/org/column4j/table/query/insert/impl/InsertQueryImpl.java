package org.column4j.table.query.insert.impl;

import org.column4j.ColumnType;
import org.column4j.mutable.MutableColumn;
import org.column4j.mutable.aggregated.*;
import org.column4j.table.ColumnsMap;
import org.column4j.table.query.insert.InsertQuery;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class InsertQueryImpl implements InsertQuery {
    private final int cursorIndex;
    private final ColumnsMap columnsMap;
    private final AtomicBoolean committed;

    public InsertQueryImpl(int cursorIndex, ColumnsMap columnsMap) {
        this.cursorIndex = cursorIndex;
        this.columnsMap = columnsMap;
        this.committed = new AtomicBoolean(false);
    }

    @Override
    public InsertQuery column(String columnName, byte value) {
        AggregatedByteMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.INT8, columnName);
        assertColumnNotNull(mutableColumn, columnName);
        mutableColumn.write(cursorIndex, value);
        return this;
    }

    @Override
    public InsertQuery column(String columnName, short value) {
        AggregatedShortMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.INT16, columnName);
        assertColumnNotNull(mutableColumn, columnName);
        mutableColumn.write(cursorIndex, value);
        return this;
    }

    @Override
    public InsertQuery column(String columnName, int value) {
        AggregatedIntMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.INT32, columnName);
        assertColumnNotNull(mutableColumn, columnName);
        mutableColumn.write(cursorIndex, value);
        return this;
    }

    @Override
    public InsertQuery column(String columnName, long value) {
        AggregatedLongMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.INT64, columnName);
        assertColumnNotNull(mutableColumn, columnName);
        mutableColumn.write(cursorIndex, value);
        return this;
    }

    @Override
    public InsertQuery column(String columnName, float value) {
        AggregatedFloatMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.FLOAT32, columnName);
        assertColumnNotNull(mutableColumn, columnName);
        mutableColumn.write(cursorIndex, value);
        return this;
    }

    @Override
    public InsertQuery column(String columnName, double value) {
        AggregatedDoubleMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.FLOAT64, columnName);
        assertColumnNotNull(mutableColumn, columnName);
        mutableColumn.write(cursorIndex, value);
        return this;
    }

    @Override
    public InsertQuery column(String columnName, String value) {
        AggregatedStringMutableColumn mutableColumn = columnsMap.getColumn(ColumnType.STRING, columnName);
        assertColumnNotNull(mutableColumn, columnName);
        mutableColumn.write(cursorIndex, value);
        return this;
    }

    @Override
    public void commit() {
        if (!committed.compareAndSet(false, true)) {
            throw new IllegalStateException("Query already committed");
        }
        /**
         * Here we need to update our indexes
         */
    }

    private static void assertColumnNotNull(MutableColumn<?> mutableColumn, String columnName) {
        if (mutableColumn == null) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist or has another type");
        }
    }
}
