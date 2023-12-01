package org.column4j.mutable.aggregated;

import org.column4j.mutable.primitive.IntMutableColumn;

import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface AggregatedIntMutableColumn extends IntMutableColumn {
    /**
     * @return aggregation column statistic bucket size
     */
    int getBucketSize();

    /**
     * @return column bucket statistics
     */
    Map<Integer, IntStatistic> getBucketColumnStatistics();
}
