package com.github.sibmaks.column4j.sample.query.table.select.selection;

import com.github.sibmaks.column4j.sample.SampleTable;
import lombok.AllArgsConstructor;
import ru.itmo.column4j.query.table.select.SelectionResultRow;
import ru.itmo.column4j.query.table.select.SelectionResult;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author sibmaks
 * @since 0.0.01
 */
@AllArgsConstructor
class SelectionResultImpl<K> implements SelectionResult {
    private final SampleTable<K> table;
    private final Map<String, String> columns;
    private final Iterator<K> primaryKeysIterable;
    private final Set<String> orderedResultColumns;

    @Override
    public Set<String> getColumns() {
        return orderedResultColumns;
    }

    @Override
    public Iterator<SelectionResultRow> getRows() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return primaryKeysIterable.hasNext();
            }

            @Override
            public SelectionResultRow next() {
                K key = primaryKeysIterable.next();
                return new SelectionResultRow() {
                    @Override
                    public <T> Optional<T> get(String name) {
                        if(!orderedResultColumns.contains(name)) {
                            throw new IllegalArgumentException("Column '%s' was not requested");
                        }
                        var sourceName = columns.get(name);
                        return table.get(key, sourceName);
                    }
                };
            }
        };
    }
}
