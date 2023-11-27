package org.column4j.mutable.primitive.impl;

import org.column4j.ColumnType;
import org.column4j.mutable.primitive.LongMutableColumn;

import java.util.Arrays;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class LongMutableColumnImpl implements LongMutableColumn {
    private static final long[] DEFAULT_DATA = {};
    private static final int DEFAULT_CAPACITY = 8;
    private static final long DEFAULT_TOMBSTONE = Long.MAX_VALUE;

    private long[] data;
    private int size;
    private final long tombstone;
    private int firstRowIndex;

    /**
     * Constructor allow to create empty long column.<br/>
     * Used {@link Long#MAX_VALUE} as default tombstone
     */
    public LongMutableColumnImpl() {
        this(DEFAULT_DATA, false, DEFAULT_TOMBSTONE);
    }

    /**
     * Copy values into internal column storage.<br/>
     * Used {@link Long#MAX_VALUE} as default tombstone
     *
     * @param source source long array
     */
    public LongMutableColumnImpl(long[] source) {
        this(source, true, DEFAULT_TOMBSTONE);
    }

    /**
     * Create column with passed tombstone
     *
     * @param tombstone specified tombstone
     */
    public LongMutableColumnImpl(long tombstone) {
        this(DEFAULT_DATA, false, tombstone);
    }

    /**
     * Copy values into internal column storage with custom tombstone
     *
     * @param source source long array
     * @param tombstone column tombstone value
     */
    public LongMutableColumnImpl(long[] source, long tombstone) {
        this(source, true, tombstone);
    }

    /**
     * Copy values into internal column storage in {@code copy} is {@code true}.
     * In case when {@code copy} is {@code false} then source used as basic array without copying.
     *
     * @param source source long array
     * @param copy copy source array into new one
     * @param tombstone column tombstone
     */
    public LongMutableColumnImpl(long[] source, boolean copy, long tombstone) {
        this.size = source.length;
        this.tombstone = tombstone;
        if(copy) {
            this.data = new long[size];
            System.arraycopy(source, 0, data, 0, size);
        } else {
            this.data = source;
        }
        this.firstRowIndex = findFirstRowIndex(0, size);
    }

    @Override
    public int firstRowIndex() {
        return firstRowIndex;
    }

    @Override
    public int capacity() {
        return data.length;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public ColumnType type() {
        return ColumnType.INT64;
    }

    @Override
    public void tombstone(int position) {
        write(position, tombstone);
    }

    @Override
    public void write(int position, long value) {
        if(position < 0) {
            throw new IllegalArgumentException("Out of range %d".formatted(position));
        }
        int capacity = data.length;
        if(position >= capacity) {
            data = grow(position + 1);
        }
        data[position] = value;
        if(position >= size) {
            size = position + 1;
        }
        if(tombstone == value) {
            if(position == firstRowIndex) {
                firstRowIndex = findFirstRowIndex(position, size);
            }
        } else if(firstRowIndex == -1 || firstRowIndex > position) {
            firstRowIndex = position;
        }
    }

    @Override
    public long[] getData() {
        return data;
    }

    private int findFirstRowIndex(int from, int to) {
        for (int i = from; i < to; i++) {
            long datum = data[i];
            if (tombstone != datum) {
                return i;
            }
        }
        return -1;
    }

    private long[] grow(int minCapacity) {
        int oldCapacity = data.length;
        if (oldCapacity > 0 || data != DEFAULT_DATA) {
            data = Arrays.copyOf(data, minCapacity);
            if(tombstone != 0) {
                Arrays.fill(data, size, data.length, tombstone);
            }
        } else {
            data = new long[Math.max(DEFAULT_CAPACITY, minCapacity)];
            if(tombstone != 0) {
                Arrays.fill(data, tombstone);
            }
        }
        return data;
    }

    public long getTombstone() {
        return tombstone;
    }
}
