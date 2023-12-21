package org.column4j.column.chunk.mutable.primitive;

import org.column4j.column.chunk.mutable.MutableColumnChunk;
import org.column4j.column.statistic.Float32Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Float32MutableColumnChunk extends MutableColumnChunk<float[], Float32Statistic> {
    /**
     * Write {@code value} into chunk at {@code position}
     *
     * @param position position in chunk
     * @param value    value to write
     */
    void write(int position, float value);

    /**
     * Get value in column chunk at {@code position}
     *
     * @param position position in chunk
     * @return value in position
     */
    float get(int position);
}
