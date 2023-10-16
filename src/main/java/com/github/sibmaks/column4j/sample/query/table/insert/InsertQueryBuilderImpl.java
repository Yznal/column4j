package com.github.sibmaks.column4j.sample.query.table.insert;

import com.github.sibmaks.column4j.sample.SampleTable;
import ru.itmo.column4j.query.table.insert.InsertQuery;
import ru.itmo.column4j.query.table.insert.InsertQueryBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class InsertQueryBuilderImpl<K> implements InsertQueryBuilder<K> {
    private final SampleTable<K> table;
    private K currentKey;
    private final Map<K, Map<String, Object>> insertion;

    public InsertQueryBuilderImpl(SampleTable<K> table) {
        this.table = table;
        this.insertion = new HashMap<>();
    }

    @Override
    public <T> InsertQueryBuilder<K> column(String column, T value) {
        var columnType = table.getColumnType(column);
        if(columnType == null) {
            throw new IllegalArgumentException("Columns '%s' doesn't exists".formatted(column));
        }
        var castedValue = columnType.cast(value);
        var columnValues = insertion.get(currentKey);
        columnValues.put(column, castedValue);
        return this;
    }

    @Override
    public InsertQuery build() {
        return new InsertQueryImpl<>(table, insertion);
    }

    @Override
    public InsertQueryBuilder<K> add(K key) {
        currentKey = key;
        insertion.computeIfAbsent(currentKey, it -> new HashMap<>());
        return this;
    }
}
