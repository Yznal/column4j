package org.column4j.dsl.select.aggregation;

import org.column4j.dsl.select.CommonSelectionQuery;
import org.column4j.dsl.select.SelectionResult;

/**
 * Aggregation query builder
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface AggregationQuery extends AggregationQueryStep<AggregationQuery>, CommonSelectionQuery<SelectionResult> {

}
