package org.column4j.column.mutable;

import org.column4j.column.Column;
import org.column4j.column.chunk.mutable.MutableColumnChunk;
import org.column4j.column.statistic.Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface MutableColumn<T, S extends Statistic> extends Column<T, S> {
    /**
     * Write tombstone into column at {@code position}
     *
     * @param position position in column
     */
    void tombstone(int position);


    /**
     * Get mutable chunk by index
     *
     * @param index index
     * @return instance of mutable chunk
     */
    MutableColumnChunk<T, S> getChunk(int index);

    /**
     * @return last modified position with not tombstone value
     */
    int getCursor();
}
