package org.column4j.mutable.aggregated;

import org.column4j.mutable.aggregated.statistic.LongStatistic;
import org.column4j.mutable.primitive.LongMutableColumn;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface AggregatedLongMutableColumn extends LongMutableColumn, AggregatedColumn<long[], LongStatistic> {
}
