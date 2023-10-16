package com.github.sibmaks.column4j.sample.query.table.select.selection;

import com.github.sibmaks.column4j.sample.SampleTable;
import lombok.AllArgsConstructor;
import ru.itmo.column4j.query.table.select.SelectQuery;
import ru.itmo.column4j.query.table.select.SelectionResult;

import java.util.*;
import java.util.function.Predicate;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@AllArgsConstructor
class SelectQueryImpl<K> implements SelectQuery {
    private final SampleTable<K> table;
    private final Map<String, String> columns;
    private final Set<String> orderedResultColumns;
    private final List<Map.Entry<String, Predicate<Optional<Object>>>> conditions;

    @Override
    public SelectionResult execute() {
        if(conditions.isEmpty()) {
            var primaryKeys = table.getPrimaryKeys();
            var primaryKeysIterable = primaryKeys.iterator();
            return new SelectionResultImpl<>(table, columns, primaryKeysIterable, orderedResultColumns);
        }
        var primaryKeys = table.getPrimaryKeys();
        var filteredPrimaryKeys = new HashSet<K>();
        for (K primaryKey : primaryKeys) {
            boolean valid = true;
            for (var condition : conditions) {
                var column = condition.getKey();
                var optionalValue = table.get(primaryKey, column);
                var predicate = condition.getValue();
                if(!predicate.test(optionalValue)) {
                    valid = false;
                    break;
                }
            }
            if(valid) {
                filteredPrimaryKeys.add(primaryKey);
            }
        }
        return new SelectionResultImpl<>(table, columns, filteredPrimaryKeys.iterator(), orderedResultColumns);
    }
}
