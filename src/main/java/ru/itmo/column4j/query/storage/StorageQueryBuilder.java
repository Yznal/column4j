package ru.itmo.column4j.query.storage;

import ru.itmo.column4j.ColumnType;
import ru.itmo.column4j.query.storage.create.CreateTableQueryBuilder;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface StorageQueryBuilder {

    /**
     * Конструируем запрос на создание таблицы в хранилище
     *
     * @param name название таблицы
     * @param primaryKey название столбца первичного ключа
     * @param primaryKeyType тип первичного ключа
     * @return builder запроса на создание таблицы
     */
    <K> CreateTableQueryBuilder<K> createTable(String name, String primaryKey, ColumnType<K> primaryKeyType);

}
