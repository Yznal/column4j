package ru.itmo.column4j.impl.query.storage.create;

import lombok.Getter;
import ru.itmo.column4j.ColumnType;
import ru.itmo.column4j.TableEngine;
import ru.itmo.column4j.query.storage.create.CreateTableQueryBuilder;
import ru.itmo.column4j.query.storage.create.CreateTableQueryEngineBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class CreateTableQueryBuilderImpl<K> implements CreateTableQueryBuilder<K> {
    @Getter
    private final String tableName;
    @Getter
    private final String primaryKey;
    @Getter
    private final ColumnType<K> primaryKeyType;
    private final Map<String, ColumnType<?>> columns;

    public CreateTableQueryBuilderImpl(String tableName,
                                       String primaryKey,
                                       ColumnType<K> primaryKeyType) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.primaryKeyType = primaryKeyType;
        this.columns = new HashMap<>();
    }

    @Override
    public <T> CreateTableQueryBuilder<K> column(String name, ColumnType<T> columnType) {
        columns.put(name, columnType);
        return this;
    }

    @Override
    public <T extends CreateTableQueryEngineBuilder<K>> T engine(TableEngine<K, T> tableEngine) {
        return tableEngine.create(this);
    }

    @Override
    public Map<String, ColumnType<?>> getColumns() {
        return Collections.unmodifiableMap(columns);
    }
}
