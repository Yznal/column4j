package com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl;

import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.Aggregator;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class CountAggregator implements Aggregator<SimpleAggregatedValue<Long>> {

    @Override
    public SimpleAggregatedValue<Long> initialize() {
        return new SimpleAggregatedValue<>(0L);
    }

    @Override
    public SimpleAggregatedValue<Long> aggregate(SimpleAggregatedValue<Long> aggregatedValue,
                                                 Object value) {
        if(value == null) {
            return aggregatedValue;
        }
        return new SimpleAggregatedValue<>(aggregatedValue.result() + 1);
    }

    @Override
    public SimpleAggregatedValue<Long> merge(SimpleAggregatedValue<Long> left,
                                             SimpleAggregatedValue<Long> right) {
        return new SimpleAggregatedValue<>(left.result() + right.result());
    }
}
