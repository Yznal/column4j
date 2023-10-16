package com.github.sibmaks.column4j.sample.query.table.select.aggregation;

import com.github.sibmaks.column4j.sample.SampleTable;
import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.AggregatedValue;
import com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.Aggregator;
import ru.itmo.column4j.query.table.select.SelectQuery;
import ru.itmo.column4j.query.table.select.SelectionResult;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class AggregationQueryImpl<K> implements SelectQuery {
    private final SampleTable<K> table;
    private final Set<String> orderedResultColumns;
    private final Map<String, Set<Map.Entry<String, Aggregator>>> sourceToResultsColumns;
    private final List<Map.Entry<String, Predicate<Optional<Object>>>> conditions;

    public AggregationQueryImpl(SampleTable<K> table,
                                Map<String, Map.Entry<String, Aggregator<?>>> columns,
                                Set<String> orderedResultColumns,
                                List<Map.Entry<String, Predicate<Optional<Object>>>> conditions) {
        this.table = table;
        this.orderedResultColumns = orderedResultColumns;
        this.sourceToResultsColumns = columns.entrySet()
                .stream()
                .collect(
                        Collectors.groupingBy(
                                it -> it.getValue().getKey(),
                                Collectors.mapping(
                                        it -> Map.entry(it.getKey(), it.getValue().getValue()),
                                        Collectors.toSet()
                                )
                        )
                );
        this.conditions = conditions;
    }

    @Override
    public SelectionResult execute() {

        Set<K> filteredPrimaryKeys;

        if (conditions.isEmpty()) {
            filteredPrimaryKeys = table.getPrimaryKeys();
        } else {
            filteredPrimaryKeys = table.getFilteredPrimaryKeys(conditions);
        }

        var resultRow = getResultRows(filteredPrimaryKeys);

        return new AggregationResultImpl(orderedResultColumns, resultRow);
    }

    private Map<String, Object> getResultRows(Set<K> filteredPrimaryKeys) {
        var resultRow = new HashMap<String, Object>();

        for (var sourceEntry : sourceToResultsColumns.entrySet()) {
            var aggregation = new HashMap<String, AggregatedValue>();

            for (var aggregationEntry : sourceEntry.getValue()) {
                var alias = aggregationEntry.getKey();
                var aggregator = aggregationEntry.getValue();
                var initial = aggregator.initialize();
                aggregation.put(alias, initial);
            }

            var sourceColumnName = sourceEntry.getKey();
            var columnValues = table.getColumn(sourceColumnName);
            for (var valueEntry : columnValues.entrySet()) {
                if (!filteredPrimaryKeys.contains(valueEntry.getKey())) {
                    continue;
                }
                var value = valueEntry.getValue();
                for (var aggregationEntry : sourceEntry.getValue()) {
                    var alias = aggregationEntry.getKey();
                    var aggregator = aggregationEntry.getValue();
                    var aggregatedValue = aggregation.get(alias);
                    aggregatedValue = aggregator.aggregate(aggregatedValue, value);
                    aggregation.put(alias, aggregatedValue);
                }
            }

            for (var aggregatedEntry : aggregation.entrySet()) {
                var alias = aggregatedEntry.getKey();
                var aggregatedValue = aggregatedEntry.getValue();
                var result = aggregatedValue.result();
                resultRow.put(alias, result);
            }
        }
        return resultRow;
    }
}
