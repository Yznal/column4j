package ru.itmo.column4j.query.table.select.aggregation;

import ru.itmo.column4j.query.table.select.CommonSelectBuilder;
import ru.itmo.column4j.query.table.select.SelectQuery;

/**
 * Построитель агрегирующего запроса
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface AggregationQueryBuilder extends AggregationQueryBuilderStep<AggregationQueryBuilder>,
        CommonSelectBuilder<SelectQuery> {

}
