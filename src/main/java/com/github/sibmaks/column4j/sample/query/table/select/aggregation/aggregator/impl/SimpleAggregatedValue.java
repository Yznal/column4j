package com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl;

import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.AggregatedValue;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public record SimpleAggregatedValue<T>(T result) implements AggregatedValue<T> {

}
