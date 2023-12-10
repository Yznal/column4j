package org.column4j.column.chunk.mutable;

import org.column4j.column.statistic.StringStatistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface StringMutableColumnChunk extends MutableColumnChunk<String[], StringStatistic> {
    /**
     * Write {@code value} into chunk at {@code position}
     *
     * @param position position in chunk
     * @param value    value to write
     */
    void write(int position, String value);

    /**
     * Get value in column chunk at {@code position}
     *
     * @param position position in chunk
     * @return value in position
     */
    String get(int position);
}
