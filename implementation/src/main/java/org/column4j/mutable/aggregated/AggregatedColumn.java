package org.column4j.mutable.aggregated;

import org.column4j.ColumnVector;

import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface AggregatedColumn<T, S> extends ColumnVector<T> {
    /**
     * @return aggregation column statistic chunk size
     */
    int getChunkSize();

    /**
     * @return column chunked statistics
     */
    Map<Integer, S> getChunkedColumnStatistics();
}
