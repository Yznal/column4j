package org.column4j.column.chunk.mutable.primitive;

import org.column4j.column.chunk.mutable.MutableColumnChunk;
import org.column4j.column.statistic.Float64Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Float64MutableColumnChunk extends MutableColumnChunk<double[], Float64Statistic> {
    /**
     * Write {@code value} into chunk at {@code position}
     *
     * @param position position in chunk
     * @param value    value to write
     */
    void write(int position, double value);

    /**
     * Get value in column chunk at {@code position}
     *
     * @param position position in chunk
     * @return value in position
     */
    double get(int position);
}
