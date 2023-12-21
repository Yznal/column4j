package org.column4j.dsl.select.impl.simple;

import org.column4j.dsl.select.SelectionResult;
import org.column4j.dsl.select.SelectionResultRow;
import org.column4j.table.Table;

import java.util.*;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class SelectionResultImpl implements SelectionResult {
    private final Table table;
    private final int cursor;
    private final Map<String, Integer> aliasesToColumn;
    private final IntPredicate conditions;

    public SelectionResultImpl(Table table, List<AliesColumn> columns, IntPredicate conditions) {
        this.table = table;
        this.aliasesToColumn = columns.stream()
                .collect(Collectors.toMap(AliesColumn::alias, AliesColumn::index));
        this.cursor = table.getCursor();
        this.conditions = conditions;
    }

    @Override
    public Collection<String> getColumns() {
        return aliasesToColumn.keySet();
    }

    @Override
    public Iterable<SelectionResultRow> getRows() {
        return () -> new Iterator<>() {
            private int position = findNextPosition(0);

            @Override
            public boolean hasNext() {
                return position <= cursor;
            }

            @Override
            public SelectionResultRow next() {
                if (position > cursor) {
                    throw new NoSuchElementException("Out of cursor bounds");
                }
                var pos = position++;
                position = findNextPosition(position);
                return new SelectionResultRowImpl(table, pos, aliasesToColumn);
            }

            private int findNextPosition(int position) {
                while (position <= cursor && !conditions.test(position)) {
                    position++;
                }
                return position;
            }
        };
    }

}
