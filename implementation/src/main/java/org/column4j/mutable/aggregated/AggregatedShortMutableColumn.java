package org.column4j.mutable.aggregated;

import org.column4j.mutable.aggregated.statistic.ShortStatistic;
import org.column4j.mutable.primitive.ShortMutableColumn;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface AggregatedShortMutableColumn extends ShortMutableColumn, AggregatedColumn<short[], ShortStatistic> {
}
