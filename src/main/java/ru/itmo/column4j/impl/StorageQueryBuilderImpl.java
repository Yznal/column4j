package ru.itmo.column4j.impl;

import ru.itmo.column4j.ColumnType;
import ru.itmo.column4j.query.storage.StorageQueryBuilder;
import ru.itmo.column4j.impl.query.storage.create.CreateTableQueryBuilderImpl;
import ru.itmo.column4j.query.storage.create.CreateTableQueryBuilder;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class StorageQueryBuilderImpl implements StorageQueryBuilder {
    @Override
    public <K> CreateTableQueryBuilder<K> createTable(String name, String primaryKey, ColumnType<K> primaryKeyType) {
        return new CreateTableQueryBuilderImpl<>(name, primaryKey, primaryKeyType);
    }
}
