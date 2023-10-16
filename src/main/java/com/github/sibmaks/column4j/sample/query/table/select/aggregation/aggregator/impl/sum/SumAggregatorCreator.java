package com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl.sum;

import ru.itmo.column4j.ColumnType;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface SumAggregatorCreator {

    SumAggregator<?> create(String tableName,
                            String columnName,
                            ColumnType<?> columnType);

}
