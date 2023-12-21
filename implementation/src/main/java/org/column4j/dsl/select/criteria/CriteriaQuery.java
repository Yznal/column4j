package org.column4j.dsl.select.criteria;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface CriteriaQuery<T> extends CriteriaQueryStep<CriteriaQuery<T>> {

    /**
     * Execute selection query
     *
     * @return selection query result
     */
    T execute();

}
