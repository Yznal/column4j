package ru.itmo.column4j.query.table.group;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface GroupingQuery {

    /**
     * Выполнить запрос на поиск
     * @return результат поиска
     */
    GroupingResult execute();

}
