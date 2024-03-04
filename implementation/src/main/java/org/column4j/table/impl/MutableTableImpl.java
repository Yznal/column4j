package org.column4j.table.impl;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.column4j.column.ColumnType;
import org.column4j.column.impl.mutable.StringMutableColumnImpl;
import org.column4j.column.impl.mutable.primitive.*;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.column.mutable.StringMutableColumn;
import org.column4j.column.mutable.primitive.*;
import org.column4j.table.MutableTable;

import java.util.ArrayList;
import java.util.SequencedMap;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class MutableTableImpl implements MutableTable {

    private final Object2IntMap<CharSequence> columnNameIndexes;
    private final ArrayList<MutableColumn<?, ?>> mutableColumns;
    private int chunkSize;

    public MutableTableImpl(SequencedMap<String, MutableColumn<?, ?>> mutableColumns) {
        this.mutableColumns = new ArrayList<>(mutableColumns.size()*2);
        this.columnNameIndexes = new Object2IntOpenHashMap<>(mutableColumns.size(), 0.99f);

        var index = 0;
        for (var entry : mutableColumns.entrySet()) {
            this.mutableColumns.add(index, entry.getValue());
            this.columnNameIndexes.put(entry.getKey(), index);
            index++;
        }
    }

    public MutableTableImpl(int columnChunkSize) {
        this.mutableColumns = new ArrayList<>();
        this.columnNameIndexes = new Object2IntOpenHashMap<>();
        this.chunkSize = columnChunkSize;
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
    public int createColumn(CharSequence name, ColumnType type) {
        var existingColumn = columnNameIndexes.getOrDefault(name, -1);
        if (existingColumn != -1) {
            return -1;
        }
        int nextIdx = mutableColumns.size();
        mutableColumns.add(createTypedColumn(type));
        columnNameIndexes.put(name, nextIdx);
        return nextIdx;
    }

    @Override
    public MutableColumn<?, ?> getColumn(int colIdx) {
        return mutableColumns.get(colIdx);
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
        MutableColumn<T, ?> mutableColumn = getMutableColumn(columnIndex);
        return (T) mutableColumn.getByIndexes(indexes);
    }

    @Override
    public <T> T getByIndexes(int columnIndex, int from, int to) {
        MutableColumn<T, ?> mutableColumn = getMutableColumn(columnIndex);
        return (T) mutableColumn.getByIndexes(from, to);
    }

    @Override
    public <T> void readByIndexes(int columnIndex, int[] indexes, T buffer) {
        MutableColumn<T, ?> mutableColumn = getMutableColumn(columnIndex);
        mutableColumn.readByIndexes(indexes, buffer);
    }

    @Override
    public <T> void readByIndexes(int columnIndex, int from, int to, T buffer) {
        MutableColumn<T, ?> mutableColumn = getMutableColumn(columnIndex);
        mutableColumn.readByIndexes(from, to, buffer);
    }

    private MutableColumn<?, ?> createTypedColumn(ColumnType type) {
        return switch (type) {
            case FLOAT64 -> new Float64MutableColumnImpl(chunkSize, Double.MAX_VALUE);
            case FLOAT32 -> new Float32MutableColumnImpl(chunkSize, Float.MAX_VALUE);
            case INT64 -> new Int64MutableColumnImpl(chunkSize, Long.MAX_VALUE);
            case INT32 -> new Int32MutableColumnImpl(chunkSize, Integer.MAX_VALUE);
            case INT16 -> new Int16MutableColumnImpl(chunkSize, Short.MAX_VALUE);
            case INT8 -> new Int8MutableColumnImpl(chunkSize, Byte.MAX_VALUE);
            case BOOL -> throw new UnsupportedOperationException("Bool type columns not supported");
            case STRING -> new StringMutableColumnImpl(chunkSize, null);
        };
    }

    @SuppressWarnings("unchecked")
    private <T> T getTypedColumn(int columnIndex, Class<T> columnType) {
        var mutableColumn = getMutableColumn(columnIndex);
        if (columnType.isInstance(mutableColumn)) {
            return (T) mutableColumn;
        } else {
            var type = mutableColumn.type();
            throw new IllegalArgumentException("Column with index %d has another type: %s".formatted(columnIndex, type.name()));
        }
    }


    @SuppressWarnings("unchecked")
    protected <X> MutableColumn<X, ?> getMutableColumn(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= mutableColumns.size()) {
            throw new IllegalArgumentException("Column with index %d doesn't exists".formatted(columnIndex));
        }
        var mutableColumn = mutableColumns.get(columnIndex);
        return (MutableColumn<X, ?>) mutableColumn;
    }
}
