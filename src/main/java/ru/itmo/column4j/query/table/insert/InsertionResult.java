package ru.itmo.column4j.query.table.insert;

/**
 * Результат вставки данных в таблицу.
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface InsertionResult {

    /**
     * @return {@code true} - данные успешно вставлены, {@code false} - в ином случае
     */
    boolean success();

}
