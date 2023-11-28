package org.column4j.aggregator;

import org.column4j.ColumnVector;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface IntAggregator {

    int aggregate(ColumnVector<int[]> column, int from, int to);

}
