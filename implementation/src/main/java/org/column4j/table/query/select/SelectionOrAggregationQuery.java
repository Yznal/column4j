package org.column4j.table.query.select;

import org.column4j.table.query.select.aggregation.AggregationQueryStep;
import org.column4j.table.query.select.selection.SelectionQuery;
import org.column4j.table.query.select.selection.SelectionQueryStep;
import org.column4j.table.query.select.aggregation.AggregationQuery;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface SelectionOrAggregationQuery extends SelectionQueryStep<SelectionQuery>,
        AggregationQueryStep<AggregationQuery> {

}
