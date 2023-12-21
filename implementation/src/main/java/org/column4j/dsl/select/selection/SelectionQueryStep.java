package org.column4j.dsl.select.selection;

/**
 * Selection query builder
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface SelectionQueryStep<P> {

    /**
     * Add column in result set without aggregation
     *
     * @param column column name
     * @return self reference
     */
    default P column(String column) {
        return column(column, column);
    }

    /**
     * Add column in result set without aggregation with changed name
     *
     * @param column column name
     * @param alias column name is result set
     * @return self reference
     */
    P column(String column, String alias);

}
