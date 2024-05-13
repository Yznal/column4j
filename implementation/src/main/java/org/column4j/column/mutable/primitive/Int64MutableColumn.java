package org.column4j.column.mutable.primitive;

import org.column4j.column.chunk.mutable.primitive.Int64MutableColumnChunk;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.column.statistic.Int64Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Int64MutableColumn extends MutableColumn<long[], Int64Statistic> {
    /**
     * Write {@code value} into column at {@code position}
     *
     * @param position position in column
     * @param value    value to write
     */
    void write(int position, long value);

    /**
     * Get value in column at {@code position}
     *
     * @param position position in column
     * @return value in position
     */
    long get(int position);

    @Override
    Int64MutableColumnChunk getChunk(int index);

    long getTombstone();
}
