package ru.itmo.column4j.query.storage.create;

import ru.itmo.column4j.Table;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface CreateTableQuery<K> {
    /**
     * @return наименование создаваемой таблицы
     */
    String getTableName();

    /**
     * Выполнить запрос на создание таблицы
     *
     * @return экземпляр созданной таблицы
     */
    Table<K> execute();

}
