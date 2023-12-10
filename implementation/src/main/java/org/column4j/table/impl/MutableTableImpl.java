package org.column4j.table.impl;

import org.column4j.column.mutable.MutableColumn;
import org.column4j.column.mutable.StringMutableColumn;
import org.column4j.column.mutable.primitive.*;
import org.column4j.table.MutableTable;

import java.util.HashMap;
import java.util.Map;
import java.util.SequencedMap;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class MutableTableImpl implements MutableTable {
    private final Map<String, Integer> columnNameIndexes;
    private final MutableColumn<?, ?>[] mutableColumns;

    public MutableTableImpl(SequencedMap<String, MutableColumn<?, ?>> mutableColumns) {
        this.mutableColumns = new MutableColumn[mutableColumns.size()];
        this.columnNameIndexes = new HashMap<>(mutableColumns.size(), 1);
        var index = 0;
        for (var entry : mutableColumns.entrySet()) {
            this.mutableColumns[index] = entry.getValue();
            this.columnNameIndexes.put(entry.getKey(), index);
            index++;
        }
    }

    @Override
    public void writeInt8(int columnIndex, int position, byte value) {
        var mutableColumn = getTypedColumn(columnIndex, Int8MutableColumn.class);
        mutableColumn.write(position, value);
    }

    @Override
    public void writeInt16(int columnIndex, int position, short value) {
        var mutableColumn = getTypedColumn(columnIndex, Int16MutableColumn.class);
        mutableColumn.write(position, value);
    }

    @Override
    public void writeInt32(int columnIndex, int position, int value) {
        var mutableColumn = getTypedColumn(columnIndex, Int32MutableColumn.class);
        mutableColumn.write(position, value);
    }

    @Override
    public void writeInt64(int columnIndex, int position, long value) {
        var mutableColumn = getTypedColumn(columnIndex, Int64MutableColumn.class);
        mutableColumn.write(position, value);
    }

    @Override
    public void writeFloat32(int columnIndex, int position, float value) {
        var mutableColumn = getTypedColumn(columnIndex, Float32MutableColumn.class);
        mutableColumn.write(position, value);
    }

    @Override
    public void writeFloat64(int columnIndex, int position, double value) {
        var mutableColumn = getTypedColumn(columnIndex, Float64MutableColumn.class);
        mutableColumn.write(position, value);
    }

    @Override
    public void writeString(int columnIndex, int position, String value) {
        var mutableColumn = getTypedColumn(columnIndex, StringMutableColumn.class);
        mutableColumn.write(position, value);
    }

    @Override
    public int getColumnIndex(String columnName) {
        return columnNameIndexes.getOrDefault(columnName, -1);
    }

    @Override
    public byte getInt8(int columnIndex, int position) {
        var mutableColumn = getTypedColumn(columnIndex, Int8MutableColumn.class);
        return mutableColumn.get(position);
    }

    @Override
    public short getInt16(int columnIndex, int position) {
        var mutableColumn = getTypedColumn(columnIndex, Int16MutableColumn.class);
        return mutableColumn.get(position);
    }

    @Override
    public int getInt32(int columnIndex, int position) {
        var mutableColumn = getTypedColumn(columnIndex, Int32MutableColumn.class);
        return mutableColumn.get(position);
    }

    @Override
    public long getInt64(int columnIndex, int position) {
        var mutableColumn = getTypedColumn(columnIndex, Int64MutableColumn.class);
        return mutableColumn.get(position);
    }

    @Override
    public float getFloat32(int columnIndex, int position) {
        var mutableColumn = getTypedColumn(columnIndex, Float32MutableColumn.class);
        return mutableColumn.get(position);
    }

    @Override
    public double getFloat64(int columnIndex, int position) {
        var mutableColumn = getTypedColumn(columnIndex, Float64MutableColumn.class);
        return mutableColumn.get(position);
    }

    @Override
    public String getString(int columnIndex, int position) {
        var mutableColumn = getTypedColumn(columnIndex, StringMutableColumn.class);
        return mutableColumn.get(position);
    }

    @Override
    public <T> T getByIndexes(int columnIndex, int[] indexes) {
        var mutableColumn = getMutableColumn(columnIndex);
        return (T) mutableColumn.getByIndexes(indexes);
    }

    @Override
    public <T> T getByIndexes(int columnIndex, int from, int to) {
        var mutableColumn = getMutableColumn(columnIndex);
        return (T) mutableColumn.getByIndexes(from, to);
    }

    @Override
    public <T> void readByIndexes(int columnIndex, int[] indexes, T buffer) {
        MutableColumn mutableColumn = getMutableColumn(columnIndex);
        mutableColumn.readByIndexes(indexes, buffer);
    }

    @Override
    public <T> void readByIndexes(int columnIndex, int from, int to, T buffer) {
        MutableColumn mutableColumn = getMutableColumn(columnIndex);
        mutableColumn.readByIndexes(from, to, buffer);
    }

    private <T> T getTypedColumn(int columnIndex, Class<T> columnType) {
        var mutableColumn = getMutableColumn(columnIndex);
        if (columnType.isInstance(mutableColumn)) {
            return (T) mutableColumn;
        } else {
            var type = mutableColumn.type();
            throw new IllegalArgumentException("Column with index %d has another type: %s".formatted(columnIndex, type.name()));
        }
    }

    protected MutableColumn<?, ?> getMutableColumn(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= mutableColumns.length) {
            throw new IllegalArgumentException("Column with index %d doesn't exists".formatted(columnIndex));
        }
        var mutableColumn = mutableColumns[columnIndex];
        return mutableColumn;
    }
}
