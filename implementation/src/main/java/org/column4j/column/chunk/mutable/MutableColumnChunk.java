package org.column4j.column.chunk.mutable;

import org.column4j.column.chunk.ColumnChunk;
import org.column4j.column.statistic.Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface MutableColumnChunk<T, S extends Statistic> extends ColumnChunk<T, S> {

    /**
     * Write tombstone into chunk at {@code position}
     *
     * @param position position in chunk
     */
    void tombstone(int position);
}
