package ru.itmo.column4j.query.table.insert;

/**
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface InsertQuery {

    /**
     * Выполнить запрос на вставку данных
     *
     * @return результат вставки
     */
    InsertionResult execute();

}
