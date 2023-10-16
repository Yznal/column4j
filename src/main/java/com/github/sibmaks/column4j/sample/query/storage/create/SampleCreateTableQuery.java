package com.github.sibmaks.column4j.sample.query.storage.create;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import ru.itmo.column4j.ColumnType;
import ru.itmo.column4j.Table;
import com.github.sibmaks.column4j.sample.SampleTable;
import ru.itmo.column4j.query.storage.create.CreateTableQuery;

import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Builder
@AllArgsConstructor
public class SampleCreateTableQuery<K> implements CreateTableQuery<K> {
    @Getter
    private final String tableName;
    private final String primaryKey;
    private final ColumnType<K> primaryKeyType;
    private final Map<String, ColumnType<?>> columns;

    @Override
    public Table<K> execute() {
        return new SampleTable<>(tableName, primaryKey, primaryKeyType, columns);
    }
}
