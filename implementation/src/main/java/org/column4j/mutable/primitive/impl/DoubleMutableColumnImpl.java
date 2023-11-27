package org.column4j.mutable.primitive.impl;

import org.column4j.ColumnType;
import org.column4j.mutable.primitive.DoubleMutableColumn;

import java.util.Arrays;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class DoubleMutableColumnImpl implements DoubleMutableColumn {
    private static final double[] DEFAULT_DATA = {};
    private static final int DEFAULT_CAPACITY = 8;
    private static final double DEFAULT_TOMBSTONE = Double.MAX_VALUE;

    private double[] data;
    private int size;
    private final double tombstone;
    private int firstRowIndex;

    /**
     * Constructor allow to create empty double column.<br/>
     * Used {@link Double#MAX_VALUE} as default tombstone
     */
    public DoubleMutableColumnImpl() {
        this(DEFAULT_DATA, false, DEFAULT_TOMBSTONE);
    }

    /**
     * Copy values into internal column storage.<br/>
     * Used {@link Double#MAX_VALUE} as default tombstone
     *
     * @param source source double array
     */
    public DoubleMutableColumnImpl(double[] source) {
        this(source, true, DEFAULT_TOMBSTONE);
    }

    /**
     * Create column with passed tombstone
     *
     * @param tombstone specified tombstone
     */
    public DoubleMutableColumnImpl(double tombstone) {
        this(DEFAULT_DATA, false, tombstone);
    }

    /**
     * Copy values into internal column storage with custom tombstone
     *
     * @param source source double array
     * @param tombstone column tombstone value
     */
    public DoubleMutableColumnImpl(double[] source, double tombstone) {
        this(source, true, tombstone);
    }

    /**
     * Copy values into internal column storage in {@code copy} is {@code true}.
     * In case when {@code copy} is {@code false} then source used as basic array without copying.
     *
     * @param source source double array
     * @param copy copy source array into new one
     * @param tombstone column tombstone
     */
    public DoubleMutableColumnImpl(double[] source, boolean copy, double tombstone) {
        this.size = source.length;
        this.tombstone = tombstone;
        if(copy) {
            this.data = new double[size];
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
        return ColumnType.FLOAT64;
    }

    @Override
    public void tombstone(int position) {
        write(position, tombstone);
    }

    @Override
    public void write(int position, double value) {
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
    public double[] getData() {
        return data;
    }

    private int findFirstRowIndex(int from, int to) {
        for (int i = from; i < to; i++) {
            double datum = data[i];
            if (tombstone != datum) {
                return i;
            }
        }
        return -1;
    }

    private double[] grow(int minCapacity) {
        int oldCapacity = data.length;
        if (oldCapacity > 0 || data != DEFAULT_DATA) {
            data = Arrays.copyOf(data, minCapacity);
            if(tombstone != 0) {
                Arrays.fill(data, size, data.length, tombstone);
            }
        } else {
            data = new double[Math.max(DEFAULT_CAPACITY, minCapacity)];
            if(tombstone != 0) {
                Arrays.fill(data, tombstone);
            }
        }
        return data;
    }

    public double getTombstone() {
        return tombstone;
    }
}