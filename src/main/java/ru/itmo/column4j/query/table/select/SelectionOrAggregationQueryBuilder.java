package ru.itmo.column4j.query.table.select;

import ru.itmo.column4j.query.table.select.aggregation.AggregationQueryBuilder;
import ru.itmo.column4j.query.table.select.aggregation.AggregationQueryBuilderStep;
import ru.itmo.column4j.query.table.select.selection.SelectionQueryBuilder;
import ru.itmo.column4j.query.table.select.selection.SelectionQueryBuilderStep;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface SelectionOrAggregationQueryBuilder extends SelectionQueryBuilderStep<SelectionQueryBuilder>,
        AggregationQueryBuilderStep<AggregationQueryBuilder> {

}
