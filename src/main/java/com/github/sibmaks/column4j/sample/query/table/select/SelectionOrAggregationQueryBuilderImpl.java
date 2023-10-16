package com.github.sibmaks.column4j.sample.query.table.select;

import com.github.sibmaks.column4j.sample.SampleTable;
import com.github.sibmaks.column4j.sample.query.table.select.aggregation.AggregationQueryBuilderImpl;
import com.github.sibmaks.column4j.sample.query.table.select.selection.SelectionQueryBuilderImpl;
import lombok.AllArgsConstructor;
import ru.itmo.column4j.query.table.select.SelectionOrAggregationQueryBuilder;
import ru.itmo.column4j.query.table.select.aggregation.AggregationQueryBuilder;
import ru.itmo.column4j.query.table.select.selection.SelectionQueryBuilder;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@AllArgsConstructor
public class SelectionOrAggregationQueryBuilderImpl<K> implements SelectionOrAggregationQueryBuilder {
    private final SampleTable<K> table;

    @Override
    public AggregationQueryBuilder count(String columnName, String alias) {
        return new AggregationQueryBuilderImpl<>(table)
                .count(columnName, alias);
    }

    @Override
    public AggregationQueryBuilder average(String columnName, String alias, int accuracy) {
        return new AggregationQueryBuilderImpl<>(table)
                .average(columnName, alias, accuracy);
    }

    @Override
    public AggregationQueryBuilder percentile(String columnName, String alias, int percentile) {
        return new AggregationQueryBuilderImpl<>(table)
                .percentile(columnName, alias, percentile);
    }

    @Override
    public AggregationQueryBuilder max(String columnName, String alias) {
        return new AggregationQueryBuilderImpl<>(table)
                .max(columnName, alias);
    }

    @Override
    public AggregationQueryBuilder min(String columnName, String alias) {
        return new AggregationQueryBuilderImpl<>(table)
                .min(columnName, alias);
    }

    @Override
    public AggregationQueryBuilder sum(String columnName, String alias) {
        return new AggregationQueryBuilderImpl<>(table)
                .sum(columnName, alias);
    }

    @Override
    public SelectionQueryBuilder column(String columnName, String alias) {
        return new SelectionQueryBuilderImpl<>(table)
                .column(columnName, alias);
    }
}
