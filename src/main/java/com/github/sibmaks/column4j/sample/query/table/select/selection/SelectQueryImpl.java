package com.github.sibmaks.column4j.sample.query.table.select.selection;

import com.github.sibmaks.column4j.sample.SampleTable;
import lombok.AllArgsConstructor;
import ru.itmo.column4j.query.table.select.SelectQuery;
import ru.itmo.column4j.query.table.select.SelectionResult;

import java.util.Map;
import java.util.Set;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@AllArgsConstructor
class SelectQueryImpl<K> implements SelectQuery {
    private final SampleTable<K> table;
    private final Map<String, String> columns;
    private final Set<String> orderedResultColumns;

    @Override
    public SelectionResult execute() {
        var primaryKeys = table.getPrimaryKeys();
        var primaryKeysIterable = primaryKeys.iterator();
        return new SelectionResultImpl<>(table, columns, primaryKeysIterable, orderedResultColumns);
    }
}
