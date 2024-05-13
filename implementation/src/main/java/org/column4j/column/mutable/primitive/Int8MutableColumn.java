package org.column4j.column.mutable.primitive;

import org.column4j.column.chunk.mutable.primitive.Int8MutableColumnChunk;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.column.statistic.Int8Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Int8MutableColumn extends MutableColumn<byte[], Int8Statistic> {
    /**
     * Write {@code value} into column at {@code position}
     *
     * @param position position in column
     * @param value    value to write
     */
    void write(int position, byte value);

    /**
     * Get value in column at {@code position}
     *
     * @param position position in column
     * @return value in position
     */
    byte get(int position);

    @Override
    Int8MutableColumnChunk getChunk(int index);

    byte getTombstone();
}
