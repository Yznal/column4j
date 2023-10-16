package com.github.sibmaks.column4j.sample.query.table.group;

import com.github.sibmaks.column4j.sample.SampleTable;
import com.github.sibmaks.column4j.sample.query.table.select.aggregation.AggregationQueryBuilderImpl;
import ru.itmo.column4j.query.table.group.GroupingQuery;
import ru.itmo.column4j.query.table.group.GroupingQueryBuilder;
import ru.itmo.column4j.query.table.select.criteria.CriteriaQueryBuilder;
import ru.itmo.column4j.query.table.select.criteria.CriteriaQueryBuilderStep;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class GroupingQueryBuilderImpl<K> implements GroupingQueryBuilder {
    private final SampleTable<K> table;
    private final List<String> groupingColumns;
    private final Map<String, String> aliases;
    private final AggregationQueryBuilderImpl<K> aggregationQueryBuilder;

    public GroupingQueryBuilderImpl(SampleTable<K> table, List<String> groupingColumns) {
        this.table = table;
        this.groupingColumns = List.copyOf(groupingColumns);
        this.aliases = new HashMap<>();
        this.aggregationQueryBuilder = new AggregationQueryBuilderImpl<>(table);
    }

    @Override
    public GroupingQueryBuilder column(String columnName, String alias) {
        if (aliases.containsKey(alias)) {
            throw new IllegalArgumentException("Alias '%s' already exists in query".formatted(alias));
        }
        var sourceColumnType = table.getColumnType(columnName);
        if (sourceColumnType == null) {
            throw new IllegalArgumentException("Column '%s' doesn't exists".formatted(columnName));
        }
        if(!groupingColumns.contains(columnName)) {
            throw new IllegalArgumentException("Column '%s' not used in grouping".formatted(columnName));
        }
        aliases.put(alias, columnName);
        return this;
    }

    @Override
    public CriteriaQueryBuilderStep<CriteriaQueryBuilder<GroupingQuery>> where() {
        return null;
    }

    @Override
    public GroupingQuery build() {
        return null;
    }

    @Override
    public GroupingQueryBuilder count(String columnName, String alias) {
        aggregationQueryBuilder.count(columnName, alias);
        return this;
    }

    @Override
    public GroupingQueryBuilder average(String columnName, String alias, int accuracy) {
        aggregationQueryBuilder.average(columnName, alias, accuracy);
        return this;
    }

    @Override
    public GroupingQueryBuilder percentile(String columnName, String alias, int percentile) {
        aggregationQueryBuilder.percentile(columnName, alias, percentile);
        return this;
    }

    @Override
    public GroupingQueryBuilder max(String columnName, String alias) {
        aggregationQueryBuilder.max(columnName, alias);
        return this;
    }

    @Override
    public GroupingQueryBuilder min(String columnName, String alias) {
        aggregationQueryBuilder.min(columnName, alias);
        return this;
    }

    @Override
    public GroupingQueryBuilder sum(String columnName, String alias) {
        aggregationQueryBuilder.sum(columnName, alias);
        return this;
    }
}
