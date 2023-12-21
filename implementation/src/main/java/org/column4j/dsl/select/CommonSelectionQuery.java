package org.column4j.dsl.select;

import org.column4j.dsl.select.criteria.CriteriaQuery;
import org.column4j.dsl.select.criteria.CriteriaQueryStep;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface CommonSelectionQuery<T> {
    /**
     * Add criteria in query
     *
     * @return instance of criteria builder
     */
    CriteriaQueryStep<CriteriaQuery<T>> where();

    /**
     * Execute selection query
     *
     * @return selection query result
     */
    T execute();
}
