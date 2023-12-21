package org.column4j.column.mutable.primitive;

import org.column4j.column.chunk.mutable.primitive.Int16MutableColumnChunk;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.column.statistic.Int16Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Int16MutableColumn extends MutableColumn<short[], Int16Statistic> {
    /**
     * Write {@code value} into column at {@code position}
     *
     * @param position position in column
     * @param value    value to write
     */
    void write(int position, short value);

    /**
     * Get value in column at {@code position}
     *
     * @param position position in column
     * @return value in position
     */
    short get(int position);

    @Override
    Int16MutableColumnChunk getChunk(int index);
}
