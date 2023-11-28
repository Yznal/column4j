package org.column4j.aggregator;

import org.column4j.mutable.primitive.IntMutableColumn;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface IntAggregator {

    int aggregate(IntMutableColumn column, int from, int to);

}
