package ru.itmo.column4j;

import ru.itmo.column4j.query.storage.create.CreateTableQueryBuilder;
import ru.itmo.column4j.query.storage.create.CreateTableQueryEngineBuilder;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface TableEngine<K, P extends CreateTableQueryEngineBuilder<K>> {

    /**
     * Создать экземпляр builder-а для создания таблицы
     *
     * @param parentBuilder builder с общими данными для создания таблицы
     * @return экземпляр builder-а для заполнения engine-based настроек
     */
    P create(CreateTableQueryBuilder<K> parentBuilder);
}
