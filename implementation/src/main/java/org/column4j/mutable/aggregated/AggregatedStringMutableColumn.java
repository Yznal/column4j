package org.column4j.mutable.aggregated;

import org.column4j.mutable.StringMutableColumn;
import org.column4j.mutable.aggregated.statistic.StringStatistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface AggregatedStringMutableColumn extends StringMutableColumn, AggregatedColumn<String[], StringStatistic> {
}
