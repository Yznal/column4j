package org.column4j.mutable.aggregated;

import org.column4j.mutable.aggregated.statistic.DoubleStatistic;
import org.column4j.mutable.primitive.DoubleMutableColumn;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface AggregatedDoubleMutableColumn extends DoubleMutableColumn, AggregatedColumn<double[], DoubleStatistic> {
}
