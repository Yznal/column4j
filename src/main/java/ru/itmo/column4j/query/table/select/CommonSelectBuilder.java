package ru.itmo.column4j.query.table.select;

import ru.itmo.column4j.query.table.select.criteria.CriteriaQueryBuilder;
import ru.itmo.column4j.query.table.select.criteria.CriteriaQueryBuilderStep;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface CommonSelectBuilder<T> {
    /**
     * Добавить критерий фильтрации данных запроса
     *
     * @return экземпляр builder-а критерия запроса
     */
    CriteriaQueryBuilderStep<CriteriaQueryBuilder<T>> where();

    /**
     * Построить запрос на выборку данных
     *
     * @return запрос на выборку данных
     */
    T build();
}
