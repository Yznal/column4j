package org.column4j.mutable.aggregated;

import org.column4j.mutable.aggregated.statistic.IntStatistic;
import org.column4j.mutable.primitive.IntMutableColumn;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface AggregatedIntMutableColumn extends IntMutableColumn, AggregatedColumn<int[], IntStatistic> {
}
