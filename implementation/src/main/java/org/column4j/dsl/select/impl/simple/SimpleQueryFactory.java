package org.column4j.dsl.select.impl.simple;

import org.column4j.dsl.select.selection.SelectionQuery;
import org.column4j.dsl.select.selection.SelectionQueryStep;
import org.column4j.table.Table;

/**
 * Naive implementation of data queries with full scan and without using indexes
 *
 * @author sibmaks
 * @since 0.0.1
 */
public final class SimpleQueryFactory {

    private SimpleQueryFactory() {

    }

    /**
     * Create simple selection query
     *
     * @param table table
     * @return instance of selection query
     */
    public static SelectionQueryStep<SelectionQuery> select(Table table) {
        return new SelectionQueryImpl(table);
    }

}
