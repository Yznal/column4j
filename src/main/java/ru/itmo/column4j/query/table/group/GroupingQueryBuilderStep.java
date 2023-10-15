package ru.itmo.column4j.query.table.group;

import ru.itmo.column4j.query.table.select.aggregation.AggregationQueryBuilderStep;
import ru.itmo.column4j.query.table.select.selection.SelectionQueryBuilderStep;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface GroupingQueryBuilderStep extends SelectionQueryBuilderStep<GroupingQueryBuilder>,
        AggregationQueryBuilderStep<GroupingQueryBuilder> {

}
