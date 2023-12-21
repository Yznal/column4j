package org.column4j.dsl.select.aggregation;

/**
 * Aggregation query builder
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface AggregationQueryStep<T> {

    /**
     * Add request for "amount of values in column"
     *
     * @param column column name
     * @param alias result column name
     * @return self reference
     */
    T count(String column, String alias);

    /**
     * Add request for "max value in column"
     *
     * @param column column name
     * @param alias result column name
     * @return self reference
     */
    T max(String column, String alias);

    /**
     * Add request for "min value in column"
     *
     * @param column column name
     * @param alias result column name
     * @return self reference
     */
    T min(String column, String alias);

    /**
     * Add request for "sum value in column"
     *
     * @param column column name
     * @param alias result column name
     * @return self reference
     */
    T sum(String column, String alias);
}
