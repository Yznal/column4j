package org.column4j.mutable.primitive.impl;

import org.column4j.ColumnType;
import org.column4j.mutable.GenericMutableColumn;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class StringMutableColumnImpl implements GenericMutableColumn<String> {
    private static final String[] DEFAULT_DATA = {};
    private static final int DEFAULT_CAPACITY = 8;
    private static final String DEFAULT_TOMBSTONE = null;

    private String[] data;
    private int size;
    private final String tombstone;
    private int firstRowIndex;

    /**
     * Constructor allow to create empty String column.<br/>
     * Used {@code null} as default tombstone
     */
    public StringMutableColumnImpl() {
        this(DEFAULT_DATA, false, DEFAULT_TOMBSTONE);
    }

    /**
     * Copy values into internal column storage.<br/>
     * Used {@code null} as default tombstone
     *
     * @param source source String array
     */
    public StringMutableColumnImpl(String[] source) {
        this(source, true, DEFAULT_TOMBSTONE);
    }

    /**
     * Create column with passed tombstone
     *
     * @param tombstone specified tombstone
     */
    public StringMutableColumnImpl(String tombstone) {
        this(DEFAULT_DATA, false, tombstone);
    }

    /**
     * Copy values into internal column storage with custom tombstone
     *
     * @param source source string array
     * @param tombstone column tombstone value
     */
    public StringMutableColumnImpl(String[] source, String tombstone) {
        this(source, true, tombstone);
    }

    /**
     * Copy values into internal column storage in {@code copy} is {@code true}.
     * In case when {@code copy} is {@code false} then source used as basic array without copying.
     *
     * @param source source String array
     * @param copy copy source array into new one
     * @param tombstone column tombstone
     */
    public StringMutableColumnImpl(String[] source, boolean copy, String tombstone) {
        this.size = source.length;
        this.tombstone = tombstone;
        if(copy) {
            this.data = new String[size];
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
        return ColumnType.STRING;
    }

    @Override
    public void tombstone(int position) {
        write(position, tombstone);
    }

    @Override
    public void write(int position, String value) {
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
        if(Objects.equals(tombstone, value)) {
            if(position == firstRowIndex) {
                firstRowIndex = findFirstRowIndex(position, size);
            }
        } else if(firstRowIndex == -1 || firstRowIndex > position) {
            firstRowIndex = position;
        }
    }

    @Override
    public String[] getData() {
        return data;
    }

    private int findFirstRowIndex(int from, int to) {
        for (int i = from; i < to; i++) {
            String datum = data[i];
            if (!Objects.equals(tombstone, datum)) {
                return i;
            }
        }
        return -1;
    }

    private String[] grow(int minCapacity) {
        int oldCapacity = data.length;
        if (oldCapacity > 0 || data != DEFAULT_DATA) {
            data = Arrays.copyOf(data, minCapacity);
            if(tombstone != null) {
                Arrays.fill(data, size, data.length, tombstone);
            }
        } else {
            data = new String[Math.max(DEFAULT_CAPACITY, minCapacity)];
            if(tombstone != null) {
                Arrays.fill(data, tombstone);
            }
        }
        return data;
    }

    public String getTombstone() {
        return tombstone;
    }
}
