package com.github.sibmaks.column4j.sample.query.storage.create;

import lombok.AllArgsConstructor;
import ru.itmo.column4j.query.storage.create.CreateTableQuery;
import ru.itmo.column4j.query.storage.create.CreateTableQueryBuilder;
import ru.itmo.column4j.query.storage.create.CreateTableQueryEngineBuilder;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@AllArgsConstructor
public class CreateTableQuerySampleBuilder<K> implements CreateTableQueryEngineBuilder<K> {
    private final CreateTableQueryBuilder<K> parentBuilder;

    @Override
    public CreateTableQuery<K> build() {
        return SampleCreateTableQuery.<K>builder()
                .tableName(parentBuilder.getTableName())
                .primaryKey(parentBuilder.getPrimaryKey())
                .primaryKeyType(parentBuilder.getPrimaryKeyType())
                .columns(parentBuilder.getColumns())
                .build();
    }
}
