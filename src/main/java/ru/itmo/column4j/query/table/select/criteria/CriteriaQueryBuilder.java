package ru.itmo.column4j.query.table.select.criteria;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface CriteriaQueryBuilder<T> extends CriteriaQueryBuilderStep<CriteriaQueryBuilder<T>> {

    /**
     * Построить запрос на выборку данных
     *
     * @return запрос на выборку данных
     */
    T build();

}
