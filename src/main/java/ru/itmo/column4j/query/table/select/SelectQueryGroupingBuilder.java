package ru.itmo.column4j.query.table.select;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface SelectQueryGroupingBuilder {

    /**
     * Построить запрос на выборку данных
     *
     * @return запрос на выборку данных
     */
    SelectQuery build();
}
