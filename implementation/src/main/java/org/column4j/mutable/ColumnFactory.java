package org.column4j.mutable;

import org.column4j.mutable.aggregated.*;
import org.column4j.mutable.aggregated.impl.*;
import org.column4j.mutable.impl.StringMutableColumnImpl;
import org.column4j.mutable.primitive.impl.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public final class ColumnFactory {

    private ColumnFactory() {

    }

    /**
     * Create aggregated byte-column
     *
     * @param tombstone value what should be interpreted as empty
     * @param chunkSize size of column aggregation chunk
     * @return instance of aggregated byte-column
     */
    public static AggregatedByteMutableColumn createInt8Column(byte tombstone, int chunkSize) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }
        var mutableColumn = new ByteMutableColumnImpl(tombstone);
        return new AggregatedByteMutableColumnImpl(mutableColumn, chunkSize);
    }

    /**
     * Create aggregated short-column
     *
     * @param tombstone value what should be interpreted as empty
     * @param chunkSize size of column aggregation chunk
     * @return instance of aggregated short-column
     */
    public static AggregatedShortMutableColumn createInt16Column(short tombstone, int chunkSize) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }
        var mutableColumn = new ShortMutableColumnImpl(tombstone);
        return new AggregatedShortMutableColumnImpl(mutableColumn, chunkSize);
    }

    /**
     * Create aggregated int-column
     *
     * @param tombstone value what should be interpreted as empty
     * @param chunkSize size of column aggregation chunk
     * @return instance of aggregated int-column
     */
    public static AggregatedIntMutableColumn createInt32Column(int tombstone, int chunkSize) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }
        var mutableColumn = new IntMutableColumnImpl(tombstone);
        return new AggregatedIntMutableColumnImpl(mutableColumn, chunkSize);
    }

    /**
     * Create aggregated long-column
     *
     * @param tombstone value what should be interpreted as empty
     * @param chunkSize size of column aggregation chunk
     * @return instance of aggregated long-column
     */
    public static AggregatedLongMutableColumn createInt64Column(long tombstone, int chunkSize) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }
        var mutableColumn = new LongMutableColumnImpl(tombstone);
        return new AggregatedLongMutableColumnImpl(mutableColumn, chunkSize);
    }

    /**
     * Create aggregated float-column
     *
     * @param tombstone value what should be interpreted as empty
     * @param chunkSize size of column aggregation chunk
     * @return instance of aggregated float-column
     */
    public static AggregatedFloatMutableColumn createFloat32Column(float tombstone, int chunkSize) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }
        var mutableColumn = new FloatMutableColumnImpl(tombstone);
        return new AggregatedFloatMutableColumnImpl(mutableColumn, chunkSize);
    }

    /**
     * Create aggregated double-column
     *
     * @param tombstone value what should be interpreted as empty
     * @param chunkSize size of column aggregation chunk
     * @return instance of aggregated double-column
     */
    public static AggregatedDoubleMutableColumn createFloat64Column(double tombstone, int chunkSize) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }
        var mutableColumn = new DoubleMutableColumnImpl(tombstone);
        return new AggregatedDoubleMutableColumnImpl(mutableColumn, chunkSize);
    }

    /**
     * Create aggregated String-column
     *
     * @param tombstone value what should be interpreted as empty
     * @param chunkSize size of column aggregation chunk
     * @return instance of aggregated String-column
     */
    public static AggregatedStringMutableColumn createStringColumn(String tombstone, int chunkSize) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("Chunk size must be positive");
        }
        var mutableColumn = new StringMutableColumnImpl(tombstone);
        return new AggregatedStringMutableColumnImpl(mutableColumn, chunkSize);
    }
}
