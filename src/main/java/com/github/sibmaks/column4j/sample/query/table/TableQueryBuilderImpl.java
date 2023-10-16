package com.github.sibmaks.column4j.sample.query.table;

import com.github.sibmaks.column4j.sample.SampleTable;
import com.github.sibmaks.column4j.sample.query.table.insert.InsertQueryBuilderImpl;
import com.github.sibmaks.column4j.sample.query.table.select.SelectionOrAggregationQueryBuilderImpl;
import lombok.AllArgsConstructor;
import ru.itmo.column4j.query.table.TableQueryBuilder;
import ru.itmo.column4j.query.table.insert.InsertQueryInsertionBuilder;
import ru.itmo.column4j.query.table.select.SelectionOrAggregationQueryBuilder;
import ru.itmo.column4j.query.table.group.GroupingQueryBuilderStep;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@AllArgsConstructor
public class TableQueryBuilderImpl<K> implements TableQueryBuilder<K> {
    private final SampleTable<K> table;

    @Override
    public InsertQueryInsertionBuilder<K> insert() {
        return new InsertQueryBuilderImpl<>(table);
    }

    @Override
    public SelectionOrAggregationQueryBuilder select() {
        return new SelectionOrAggregationQueryBuilderImpl<>(table);
    }

    @Override
    public GroupingQueryBuilderStep groupBy(String column, String... columns) {
        throw new UnsupportedOperationException();
    }
}
