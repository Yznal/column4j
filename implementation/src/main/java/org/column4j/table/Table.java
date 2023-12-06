package org.column4j.table;

import org.column4j.table.query.insert.InsertQuery;
import org.column4j.table.query.select.SelectionOrAggregationQuery;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Table {

    /**
     * Factory method to create TableBuilder
     * @return instance of TableBuilder
     */
    static TableBuilder builder() {
        return new TableImpl.TableBuilderImpl();
    }

    /**
     * Start insertion query
     */
    InsertQuery insert();

    SelectionOrAggregationQuery select();

}
