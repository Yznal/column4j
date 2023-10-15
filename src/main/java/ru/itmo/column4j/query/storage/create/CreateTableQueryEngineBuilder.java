package ru.itmo.column4j.query.storage.create;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface CreateTableQueryEngineBuilder<K> {

    /**
     * Построить запрос на создание таблицы
     *
     * @return запрос на создание таблицы
     */
    CreateTableQuery<K> build();

}
