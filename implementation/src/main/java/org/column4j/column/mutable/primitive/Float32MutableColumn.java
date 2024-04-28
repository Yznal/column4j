package org.column4j.column.mutable.primitive;

import org.column4j.column.chunk.mutable.primitive.Float32MutableColumnChunk;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.column.statistic.Float32Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Float32MutableColumn extends MutableColumn<float[], Float32Statistic> {
    /**
     * Write {@code value} into column at {@code position}
     *
     * @param position position in column
     * @param value    value to write
     */
    void write(int position, float value);

    /**
     * Get value in column at {@code position}
     *
     * @param position position in column
     * @return value in position
     */
    float get(int position);

    @Override
    Float32MutableColumnChunk getChunk(int index);

    float getTombstone();
}
