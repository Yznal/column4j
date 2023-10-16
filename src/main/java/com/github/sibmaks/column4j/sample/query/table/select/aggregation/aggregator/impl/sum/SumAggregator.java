package com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl.sum;

import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.Aggregator;
import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl.SimpleAggregatedValue;
import ru.itmo.column4j.ColumnType;

import java.util.function.BiFunction;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class SumAggregator<T extends Number> implements Aggregator<SimpleAggregatedValue<T>> {

    private final T base;
    private final BiFunction<T, T, T> summator;

    public SumAggregator(String tableName,
                         String columnName,
                         ColumnType<?> columnType,
                         Class<T> sumType,
                         T base,
                         BiFunction<T, T, T> summator) {
        this.base = base;
        this.summator = summator;
        var columnClazz = columnType.getType();
        if(!sumType.isAssignableFrom(columnClazz)) {
            throw new IllegalArgumentException("Can't find sum is not %s column '%s' in table '%s'".formatted(sumType, columnName, tableName));
        }
    }

    @Override
    public SimpleAggregatedValue<T> initialize() {
        return new SimpleAggregatedValue<>(base);
    }

    @Override
    public SimpleAggregatedValue<T> aggregate(SimpleAggregatedValue<T> aggregatedValue, Object value) {
        if(value == null) {
            return aggregatedValue;
        }
        T left = aggregatedValue.result();
        T right = (T) value;
        T sum = summator.apply(left, right);
        return new SimpleAggregatedValue<>(sum);
    }

    @Override
    public SimpleAggregatedValue<T> merge(SimpleAggregatedValue<T> left, SimpleAggregatedValue<T> right) {
        T leftT = left.result();
        T rightT = right.result();
        T sum = summator.apply(leftT, rightT);
        return new SimpleAggregatedValue<>(sum);
    }
}
