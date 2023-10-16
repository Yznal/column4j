package com.github.sibmaks.column4j.sample.query.table.select.aggregation;

import com.github.sibmaks.column4j.sample.SampleTable;
import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.Aggregator;
import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl.CountAggregator;
import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl.MaxAggregator;
import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl.MinAggregator;
import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl.average.AverageAggregator;
import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl.sum.SumAggregators;
import com.github.sibmaks.column4j.sample.query.table.select.criteria.CriteriaQueryBuilderImpl;
import ru.itmo.column4j.query.table.select.SelectQuery;
import ru.itmo.column4j.query.table.select.aggregation.AggregationQueryBuilder;
import ru.itmo.column4j.query.table.select.criteria.CriteriaQueryBuilder;
import ru.itmo.column4j.query.table.select.criteria.CriteriaQueryBuilderStep;

import java.util.*;
import java.util.function.Predicate;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class AggregationQueryBuilderImpl<K> implements AggregationQueryBuilder {
    protected final SampleTable<K> table;
    private final Map<String, Map.Entry<String, Aggregator<?>>> columns;
    private final List<Map.Entry<String, Predicate<Optional<Object>>>> conditions;

    public AggregationQueryBuilderImpl(SampleTable<K> table) {
        this.table = table;
        this.columns = new LinkedHashMap<>();
        this.conditions = new ArrayList<>();
    }

    @Override
    public CriteriaQueryBuilderStep<CriteriaQueryBuilder<SelectQuery>> where() {
        return new CriteriaQueryBuilderImpl<>(table, conditions -> {
            this.conditions.addAll(conditions);
            return build();
        });
    }

    @Override
    public SelectQuery build() {
        return new AggregationQueryImpl<>(table, Map.copyOf(columns), columns.keySet(), conditions);
    }

    @Override
    public AggregationQueryBuilder count(String columnName, String alias) {
        if (columns.containsKey(alias)) {
            throw new IllegalArgumentException("Alias '%s' already exists in query".formatted(alias));
        }
        var sourceColumnType = table.getColumnType(columnName);
        if (sourceColumnType == null) {
            throw new IllegalArgumentException("Column '%s' doesn't exists".formatted(columnName));
        }
        var columnAggregation = new CountAggregator();
        columns.put(alias, Map.entry(columnName, columnAggregation));
        return this;
    }

    @Override
    public AggregationQueryBuilder average(String columnName, String alias, int accuracy) {
        if (columns.containsKey(alias)) {
            throw new IllegalArgumentException("Alias '%s' already exists in query".formatted(alias));
        }
        var sourceColumnType = table.getColumnType(columnName);
        if (sourceColumnType == null) {
            throw new IllegalArgumentException("Column '%s' doesn't exists".formatted(columnName));
        }
        var columnAggregation = new AverageAggregator(table.getTableName(), columnName, sourceColumnType, accuracy);
        columns.put(alias, Map.entry(columnName, columnAggregation));
        return this;
    }

    @Override
    public AggregationQueryBuilder percentile(String columnName, String alias, int percentile) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AggregationQueryBuilder max(String columnName, String alias) {
        if (columns.containsKey(alias)) {
            throw new IllegalArgumentException("Alias '%s' already exists in query".formatted(alias));
        }
        var sourceColumnType = table.getColumnType(columnName);
        if (sourceColumnType == null) {
            throw new IllegalArgumentException("Column '%s' doesn't exists".formatted(columnName));
        }
        var columnAggregation = new MaxAggregator(table.getTableName(), columnName, sourceColumnType);
        columns.put(alias, Map.entry(columnName, columnAggregation));
        return this;
    }

    @Override
    public AggregationQueryBuilder min(String columnName, String alias) {
        if (columns.containsKey(alias)) {
            throw new IllegalArgumentException("Alias '%s' already exists in query".formatted(alias));
        }
        var sourceColumnType = table.getColumnType(columnName);
        if (sourceColumnType == null) {
            throw new IllegalArgumentException("Column '%s' doesn't exists".formatted(columnName));
        }
        var columnAggregation = new MinAggregator(table.getTableName(), columnName, sourceColumnType);
        columns.put(alias, Map.entry(columnName, columnAggregation));
        return this;
    }

    @Override
    public AggregationQueryBuilder sum(String columnName, String alias) {
        if (columns.containsKey(alias)) {
            throw new IllegalArgumentException("Alias '%s' already exists in query".formatted(alias));
        }
        var sourceColumnType = table.getColumnType(columnName);
        if (sourceColumnType == null) {
            throw new IllegalArgumentException("Column '%s' doesn't exists".formatted(columnName));
        }
        var columnAggregation = SumAggregators.getSumAggregator(
                table.getTableName(),
                columnName,
                sourceColumnType
        );
        columns.put(alias, Map.entry(columnName, columnAggregation));
        return this;
    }

}
