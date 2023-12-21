package org.column4j.column.chunk.mutable.primitive;

import org.column4j.column.chunk.mutable.MutableColumnChunk;
import org.column4j.column.statistic.Int8Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Int8MutableColumnChunk extends MutableColumnChunk<byte[], Int8Statistic> {
    /**
     * Write {@code value} into chunk at {@code position}
     *
     * @param position position in chunk
     * @param value    value to write
     */
    void write(int position, byte value);

    /**
     * Get value in column chunk at {@code position}
     *
     * @param position position in chunk
     * @return value in position
     */
    byte get(int position);
}
