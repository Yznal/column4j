package org.column4j.column.chunk.mutable.primitive;

import org.column4j.column.chunk.mutable.MutableColumnChunk;
import org.column4j.column.statistic.Int32Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Int32MutableColumnChunk extends MutableColumnChunk<int[], Int32Statistic> {
    /**
     * Write {@code value} into chunk at {@code position}
     *
     * @param position position in chunk
     * @param value    value to write
     */
    void write(int position, int value);

    /**
     * Get value in column chunk at {@code position}
     *
     * @param position position in chunk
     * @return value in position
     */
    int get(int position);
}
