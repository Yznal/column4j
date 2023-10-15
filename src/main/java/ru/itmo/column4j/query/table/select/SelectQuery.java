package ru.itmo.column4j.query.table.select;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface SelectQuery {

    /**
     * Выполнить запрос на поиск
     * @return результат поиска
     */
    SelectionResult execute();

}
