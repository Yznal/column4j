package org.column4j.column.chunk;

import org.column4j.column.statistic.Statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface ColumnChunk<T, S extends Statistic> {

    /**
     * @return chunk aggregated statistics
     */
    S getStatistic();

    /**
     * @return get chunk raw data
     */
    T getData();
}
