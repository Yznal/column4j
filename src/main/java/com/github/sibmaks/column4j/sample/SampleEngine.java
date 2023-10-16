package com.github.sibmaks.column4j.sample;

import ru.itmo.column4j.TableEngine;
import ru.itmo.column4j.query.storage.create.CreateTableQueryBuilder;
import com.github.sibmaks.column4j.sample.query.storage.create.CreateTableQuerySampleBuilder;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class SampleEngine<K> implements TableEngine<K, CreateTableQuerySampleBuilder<K>> {

    @Override
    public CreateTableQuerySampleBuilder<K> create(CreateTableQueryBuilder<K> parentBuilder) {
        return new CreateTableQuerySampleBuilder<>(parentBuilder);
    }
}
