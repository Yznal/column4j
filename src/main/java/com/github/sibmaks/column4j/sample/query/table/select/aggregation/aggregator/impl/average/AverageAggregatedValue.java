package com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl.average;

import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.AggregatedValue;

import java.math.BigDecimal;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public record AverageAggregatedValue(BigDecimal count, BigDecimal result) implements AggregatedValue<BigDecimal> {
}
