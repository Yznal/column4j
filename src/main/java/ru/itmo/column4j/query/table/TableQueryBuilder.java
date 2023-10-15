package ru.itmo.column4j.query.table;

import ru.itmo.column4j.query.table.insert.InsertQueryInsertionBuilder;
import ru.itmo.column4j.query.table.select.SelectQueryGroupingBuilder;
import ru.itmo.column4j.query.table.select.SelectionOrAggregationQueryBuilder;
import ru.itmo.column4j.query.table.group.GroupingQueryBuilderStep;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface TableQueryBuilder<K> {

    /**
     * Конструирование запроса на вставку данных в таблицу
     *
     * @return builder запроса на вставку в таблицу
     */
    InsertQueryInsertionBuilder<K> insert();

    /**
     * Конструирование запроса выборки данных из таблицы без группировки.<br/>
     * Позволяет получить "сырые" данные из таблицы, либо провести агрегацию
     *
     * @return builder запроса данных из таблицы
     */
    SelectionOrAggregationQueryBuilder select();

    /**
     * Конструирование запроса выборки сгруппированных данных из таблицы.<br/>
     * Ограничение на выборку данных с группировкой:
     * 1. Столбцы участвующие в группировке должны быть в результирующей выборке
     * 2. Остальные столбцы должны быть агрегатами
     *
     * @param column название первой колонки для группировки
     * @param columns дополнительные колонки для группировки
     * @return ссылка на {@link SelectQueryGroupingBuilder}
     */
    GroupingQueryBuilderStep groupBy(String column, String ... columns);

}
