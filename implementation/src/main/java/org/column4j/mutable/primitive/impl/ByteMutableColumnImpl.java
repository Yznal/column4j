package org.column4j.mutable.primitive.impl;

import org.column4j.ColumnType;
import org.column4j.mutable.primitive.ByteMutableColumn;

import java.util.Arrays;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class ByteMutableColumnImpl implements ByteMutableColumn {
    private static final byte[] DEFAULT_DATA = {};
    private static final int DEFAULT_CAPACITY = 8;
    private static final byte DEFAULT_TOMBSTONE = Byte.MAX_VALUE;

    private byte[] data;
    private int size;
    private final byte tombstone;
    private int firstRowIndex;

    /**
     * Constructor allow to create empty byte column.<br/>
     * Used {@link Byte#MAX_VALUE} as default tombstone
     */
    public ByteMutableColumnImpl() {
        this(DEFAULT_DATA, false, DEFAULT_TOMBSTONE);
    }

    /**
     * Copy values into internal column storage.<br/>
     * Used {@link Byte#MAX_VALUE} as default tombstone
     *
     * @param source source byte array
     */
    public ByteMutableColumnImpl(byte[] source) {
        this(source, true, DEFAULT_TOMBSTONE);
    }

    /**
     * Create column with passed tombstone
     *
     * @param tombstone specified tombstone
     */
    public ByteMutableColumnImpl(byte tombstone) {
        this(DEFAULT_DATA, false, tombstone);
    }

    /**
     * Copy values into internal column storage with custom tombstone
     *
     * @param source source byte array
     * @param tombstone column tombstone value
     */
    public ByteMutableColumnImpl(byte[] source, byte tombstone) {
        this(source, true, tombstone);
    }

    /**
     * Copy values into internal column storage in {@code copy} is {@code true}.
     * In case when {@code copy} is {@code false} then source used as basic array without copying.
     *
     * @param source source byte array
     * @param copy copy source array into new one
     * @param tombstone column tombstone
     */
    public ByteMutableColumnImpl(byte[] source, boolean copy, byte tombstone) {
        this.size = source.length;
        this.tombstone = tombstone;
        if(copy) {
            this.data = new byte[size];
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
        return ColumnType.INT8;
    }

    @Override
    public void tombstone(int position) {
        write(position, tombstone);
    }

    @Override
    public void write(int position, byte value) {
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
    public byte[] getData() {
        return data;
    }

    private int findFirstRowIndex(int from, int to) {
        for (int i = from; i < to; i++) {
            byte datum = data[i];
            if (tombstone != datum) {
                return i;
            }
        }
        return -1;
    }

    private byte[] grow(int minCapacity) {
        int oldCapacity = data.length;
        if (oldCapacity > 0 || data != DEFAULT_DATA) {
            data = Arrays.copyOf(data, minCapacity);
            if(tombstone != 0) {
                Arrays.fill(data, size, data.length, tombstone);
            }
        } else {
            data = new byte[Math.max(DEFAULT_CAPACITY, minCapacity)];
            if(tombstone != 0) {
                Arrays.fill(data, tombstone);
            }
        }
        return data;
    }

    public byte getTombstone() {
        return tombstone;
    }
}
