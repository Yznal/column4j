package org.column4j.column.mutable.primitive;

import org.column4j.column.chunk.mutable.primitive.Int32MutableColumnChunk;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.column.statistic.Int32Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Int32MutableColumn extends MutableColumn<int[], Int32Statistic> {
    /**
     * Write {@code value} into column at {@code position}
     *
     * @param position position in column
     * @param value    value to write
     */
    void write(int position, int value);

    /**
     * Get value in column at {@code position}
     *
     * @param position position in column
     * @return value in position
     */
    int get(int position);

    @Override
    Int32MutableColumnChunk getChunk(int index);

    int getTombstone();
}
