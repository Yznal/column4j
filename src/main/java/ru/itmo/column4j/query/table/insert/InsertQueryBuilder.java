package ru.itmo.column4j.query.table.insert;

/**
 * Конструктор запроса вставки данных в таблицу
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface InsertQueryBuilder<K> extends InsertQueryInsertionBuilder<K> {

    /**
     *
     * @param column наименование колонки для вставки
     * @param value значение для вставки
     * @return ссылку на этот {@link InsertQueryBuilder}
     * @param <T> тип значения
     */
    <T> InsertQueryBuilder<K> column(String column, T value);

    /**
     * Создать запрос на вставку данных в таблицу
     *
     * @return запрос вставки данных в таблицу
     */
    InsertQuery build();
}
