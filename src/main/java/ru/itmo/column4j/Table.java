package ru.itmo.column4j;

import ru.itmo.column4j.query.table.TableQueryBuilder;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Table<K> {

    /**
     * @return название таблицы
     */
    String getName();

    /**
     * @return builder запросов работы с данными
     */
    TableQueryBuilder<K> getTableQueryBuilder();
}
