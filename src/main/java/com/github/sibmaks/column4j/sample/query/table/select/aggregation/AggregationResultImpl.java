package com.github.sibmaks.column4j.sample.query.table.select.aggregation;

import lombok.AllArgsConstructor;
import ru.itmo.column4j.query.table.select.SelectionResultRow;
import ru.itmo.column4j.query.table.select.SelectionResult;

import java.util.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@AllArgsConstructor
class AggregationResultImpl implements SelectionResult {
    private final Set<String> orderedResultColumns;
    private final Map<String, Object> resultRow;

    @Override
    public Set<String> getColumns() {
        return orderedResultColumns;
    }

    @Override
    public Iterator<SelectionResultRow> getRows() {
        return new Iterator<>() {
            private boolean requested;

            @Override
            public boolean hasNext() {
                if (!requested) {
                    requested = true;
                    return true;
                }
                return false;
            }

            @Override
            public SelectionResultRow next() {
                return new SelectionResultRow() {
                    @Override
                    public <T> Optional<T> get(String name) {
                        if(!orderedResultColumns.contains(name)) {
                            throw new IllegalArgumentException("Column '%s' was not requested");
                        }
                        var value = (T) resultRow.get(name);
                        return Optional.ofNullable(value);
                    }
                };
            }
        };
    }
}
