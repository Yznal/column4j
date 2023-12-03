package org.column4j.mutable.aggregated;

import org.column4j.mutable.aggregated.statistic.ByteStatistic;
import org.column4j.mutable.primitive.ByteMutableColumn;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface AggregatedByteMutableColumn extends ByteMutableColumn, AggregatedColumn<byte[], ByteStatistic> {
}
