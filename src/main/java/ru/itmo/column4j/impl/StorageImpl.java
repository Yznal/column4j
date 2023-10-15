package ru.itmo.column4j.impl;

import lombok.Getter;
import ru.itmo.column4j.Storage;
import ru.itmo.column4j.Table;
import ru.itmo.column4j.query.storage.StorageQueryBuilder;
import ru.itmo.column4j.query.storage.create.CreateTableQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Хранилище созданных таблиц
 *
 * @author sibmaks
 * @since 0.0.1
 */
public class StorageImpl implements Storage {
    private final Map<String, Table<?>> tables;
    @Getter
    private final StorageQueryBuilder storageQueryBuilder;

    public StorageImpl() {
        this.tables = new HashMap<>();
        this.storageQueryBuilder = new StorageQueryBuilderImpl();
    }

    @Override
    public <K> Table<K> execute(CreateTableQuery<K> query) {
        var tableName = query.getTableName();
        if(tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table '%s' already exists!".formatted(tableName));
        }
        var table = query.execute();
        tables.put(tableName, table);
        return table;
    }

    @Override
    public <K> Optional<Table<K>> findTable(String name) {
        return Optional.ofNullable((Table<K>) tables.get(name));
    }
}
