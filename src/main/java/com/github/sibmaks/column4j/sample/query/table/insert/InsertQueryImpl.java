package com.github.sibmaks.column4j.sample.query.table.insert;

import ru.itmo.column4j.query.table.insert.InsertQuery;
import ru.itmo.column4j.query.table.insert.InsertionResult;
import com.github.sibmaks.column4j.sample.SampleTable;

import java.util.Collections;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public final class InsertQueryImpl<K> implements InsertQuery {
    private final SampleTable<K> table;
    private final Map<K, Map<String, Object>> insertionValues;

    public InsertQueryImpl(SampleTable<K> table, Map<K, Map<String, Object>> insertionValues) {
        this.table = table;
        this.insertionValues = Collections.unmodifiableMap(insertionValues);
    }

    @Override
    public InsertionResult execute() {
        for (var insertion : insertionValues.entrySet()) {
            K key = insertion.getKey();
            if(table.containsPrimaryKey(key)) {
                throw new IllegalArgumentException("Primary key '%s' already exists".formatted(key));
            }
            for (var columnName : insertion.getValue().keySet()) {
                if(!table.containsColumn(columnName)) {
                    throw new IllegalArgumentException("Table not contains '%s' column".formatted(columnName));
                }
            }
        }
        for (var insertion : insertionValues.entrySet()) {
            K key = insertion.getKey();
            table.addPrimaryKey(key);
            for (var entry : insertion.getValue().entrySet()) {
                table.addValue(key, entry.getKey(), entry.getValue());
            }
        }
        return new SampleInsertionResult(true);
    }

}
