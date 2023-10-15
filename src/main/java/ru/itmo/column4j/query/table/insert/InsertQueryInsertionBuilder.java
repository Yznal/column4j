package ru.itmo.column4j.query.table.insert;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface InsertQueryInsertionBuilder<K> {
    /**
     *  Добавить в запрос добавление нового первичного ключа
     *
     * @param key значение первичного ключа
     * @return ссылка на объект {@link InsertQueryBuilder}
     */
    InsertQueryBuilder<K> add(K key);

}
