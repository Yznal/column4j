package com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl;

import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.Aggregator;
import ru.itmo.column4j.ColumnType;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class MaxAggregator implements Aggregator<SimpleAggregatedValue<Comparable>> {

    public MaxAggregator(String tableName, String columnName, ColumnType<?> columnType) {
        var columnClazz = columnType.getType();
        if(!Comparable.class.isAssignableFrom(columnClazz)) {
            throw new IllegalArgumentException("Can't find max is not comparable column '%s' in table '%s'".formatted(columnName, tableName));
        }
    }

    @Override
    public SimpleAggregatedValue<Comparable> initialize() {
        return new SimpleAggregatedValue<>(null);
    }

    @Override
    public SimpleAggregatedValue<Comparable> aggregate(SimpleAggregatedValue<Comparable> aggregatedValue,
                                                          Object value) {
        if(value == null) {
            return aggregatedValue;
        }
        var result = aggregatedValue.result();
        if(result == null || result.compareTo(value) < 0) {
            return new SimpleAggregatedValue<>((Comparable) value);
        }
        return aggregatedValue;
    }

    @Override
    public SimpleAggregatedValue<Comparable> merge(SimpleAggregatedValue<Comparable> left,
                                                   SimpleAggregatedValue<Comparable> right) {
        return aggregate(left, right.result());
    }
}
