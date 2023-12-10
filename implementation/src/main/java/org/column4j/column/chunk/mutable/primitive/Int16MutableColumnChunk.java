package org.column4j.column.chunk.mutable.primitive;

import org.column4j.column.chunk.mutable.MutableColumnChunk;
import org.column4j.column.statistic.Int16Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Int16MutableColumnChunk extends MutableColumnChunk<short[], Int16Statistic> {
    /**
     * Write {@code value} into chunk at {@code position}
     *
     * @param position position in chunk
     * @param value    value to write
     */
    void write(int position, short value);

    /**
     * Get value in column chunk at {@code position}
     *
     * @param position position in chunk
     * @return value in position
     */
    short get(int position);
}
