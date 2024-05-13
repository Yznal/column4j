package org.column4j.column.mutable;

import org.column4j.column.chunk.mutable.StringMutableColumnChunk;
import org.column4j.column.statistic.StringStatistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface StringMutableColumn extends MutableColumn<String[], StringStatistic> {
    /**
     * Write {@code value} into column at {@code position}
     *
     * @param position position in column
     * @param value    value to write
     */
    void write(int position, String value);

    /**
     * Get value in column at {@code position}
     *
     * @param position position in column
     * @return value in position
     */
    String get(int position);

    @Override
    StringMutableColumnChunk getChunk(int index);

    String getTombstone();
}
