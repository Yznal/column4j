package com.github.sibmaks.column4j.sample.query.table.select.selection;

import com.github.sibmaks.column4j.sample.SampleTable;
import com.github.sibmaks.column4j.sample.query.table.select.criteria.CriteriaQueryBuilderImpl;
import ru.itmo.column4j.query.table.select.SelectQuery;
import ru.itmo.column4j.query.table.select.criteria.CriteriaQueryBuilder;
import ru.itmo.column4j.query.table.select.criteria.CriteriaQueryBuilderStep;
import ru.itmo.column4j.query.table.select.selection.SelectionQueryBuilder;

import java.util.*;
import java.util.function.Predicate;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class SelectionQueryBuilderImpl<K> implements SelectionQueryBuilder {
    private final SampleTable<K> table;
    private final Map<String, String> columns;
    private final List<Map.Entry<String, Predicate<Optional<Object>>>> conditions;

    public SelectionQueryBuilderImpl(SampleTable<K> table) {
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
        return new SelectQueryImpl<>(table, Map.copyOf(columns), columns.keySet(), List.copyOf(conditions));
    }

    @Override
    public SelectionQueryBuilder column(String columnName, String alias) {
        if (columns.containsKey(alias)) {
            throw new IllegalArgumentException("Alias '%s' already exists in query".formatted(alias));
        }
        var sourceColumnType = table.getColumnType(columnName);
        if (sourceColumnType == null) {
            throw new IllegalArgumentException("Column '%s' doesn't exists".formatted(columnName));
        }
        columns.put(alias, columnName);
        return this;
    }
}
