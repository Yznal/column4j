package org.column4j.column;

import org.column4j.column.chunk.ColumnChunk;
import org.column4j.column.statistic.Statistic;

import java.util.List;

/**
 * Interface {@link Column} describe column type - sequential set of single type data
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface Column<T, S extends Statistic> {

    /**
     * @return amount of rows in column
     */
    int size();

    /**
     * @return column data type
     */
    ColumnType type();

    /**
     * @return all chunks in the column
     */
    List<ColumnChunk<T, S>> getChunks();

    /**
     * Get column values by indexes array.
     *
     * @param indexes indexes to get
     * @return column values array, size the same as indexes
     */
    T getByIndexes(int[] indexes);

    /**
     * Get column values by indexes bounds.
     *
     * @param from left bound inclusive
     * @param to   right bound inclusive
     * @return column values array, size to - from + 1
     */
    T getByIndexes(int from, int to);

    /**
     * Get column values by indexes array and write into buffer.
     * Buffer size must be the same length as indexes, otherwise {@link IllegalArgumentException} should be thrown.
     *
     * @param indexes indexes to get
     * @param buffer  buffer to write values
     */
    void readByIndexes(int[] indexes, T buffer);

    /**
     * Get column values by indexes bounds and write into buffer.
     * Buffer size must be the same length as indexes, otherwise {@link IllegalArgumentException} should be thrown.
     *
     * @param from   left bound inclusive
     * @param to     right bound inclusive
     * @param buffer buffer to write values
     */
    void readByIndexes(int from, int to, T buffer);
}
