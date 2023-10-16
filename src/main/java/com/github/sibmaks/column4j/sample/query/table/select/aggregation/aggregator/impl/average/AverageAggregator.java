package com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl.average;

import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.Aggregator;
import ru.itmo.column4j.ColumnType;

import java.math.BigDecimal;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class AverageAggregator implements Aggregator<AverageAggregatedValue> {

    private final String tableName;
    private final String columnName;
    private final int accuracy;

    public AverageAggregator(String tableName, String columnName, ColumnType<?> columnType, int accuracy) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.accuracy = accuracy;
        var columnClazz = columnType.getType();
        if(!Number.class.isAssignableFrom(columnClazz)) {
            throw new IllegalArgumentException("Can't find average is not number column '%s' in table '%s'".formatted(columnName, tableName));
        }
    }

    @Override
    public AverageAggregatedValue initialize() {
        return new AverageAggregatedValue(BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public AverageAggregatedValue aggregate(AverageAggregatedValue aggregatedValue, Object value) {
        if(value == null) {
            return aggregatedValue;
        }
        if(!(value instanceof Number number)) {
            throw new IllegalArgumentException("Can't find average is not number column '%s' in table '%s'".formatted(columnName, tableName));
        }
        var nextCount = aggregatedValue.count().add(BigDecimal.ONE);
        var nextResult = new BigDecimal(number.toString())
                .setScale(accuracy, BigDecimal.ROUND_DOWN)
                .add(aggregatedValue.result())
                .divide(nextCount, BigDecimal.ROUND_DOWN);

        return new AverageAggregatedValue(nextCount, nextResult);
    }

    @Override
    public AverageAggregatedValue merge(AverageAggregatedValue left, AverageAggregatedValue right) {
        var nextCount = left.count().add(right.count());
        var nextResult = left.result()
                .add(right.result())
                .divide(nextCount, BigDecimal.ROUND_DOWN);

        return new AverageAggregatedValue(nextCount, nextResult);
    }
}
