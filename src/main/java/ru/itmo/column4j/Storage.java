package ru.itmo.column4j;

import ru.itmo.column4j.query.storage.StorageQueryBuilder;
import ru.itmo.column4j.query.storage.create.CreateTableQuery;

import java.util.Optional;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Storage {

    /**
     * @return builder запросов на изменение схемы хранилища
     */
    StorageQueryBuilder getStorageQueryBuilder();

    /**
     * Выполнить запрос на создание таблицы
     *
     * @param query запрос на создание таблицы
     * @return экземпляр созданной таблицы
     * @param <K> тип ключа
     */
    <K> Table<K> execute(CreateTableQuery<K> query);

    /**
     * Запрос таблицы из хранилища, в случае, если таблица не существует, то
     * будет возвращён {@link Optional#empty()}.
     *
     * @param name наименование таблицы
     * @return экземпляр таблицы
     */
    <K> Optional<Table<K>> findTable(String name);

}
