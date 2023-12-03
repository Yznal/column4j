package org.column4j.mutable.aggregated;

import org.column4j.mutable.aggregated.statistic.FloatStatistic;
import org.column4j.mutable.primitive.FloatMutableColumn;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface AggregatedFloatMutableColumn extends FloatMutableColumn, AggregatedColumn<float[], FloatStatistic> {
}
