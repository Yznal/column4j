package org.column4j.column.mutable.primitive;

import org.column4j.column.chunk.mutable.primitive.Float64MutableColumnChunk;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.column.statistic.Float64Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Float64MutableColumn extends MutableColumn<double[], Float64Statistic> {
    /**
     * Write {@code value} into column at {@code position}
     *
     * @param position position in column
     * @param value value to write
     */
    void write(int position, double value);

    /**
     * Get value in column at {@code position}
     *
     * @param position position in column
     * @return value in position
     */
    double get(int position);

    @Override
    Float64MutableColumnChunk getChunk(int index);
}
