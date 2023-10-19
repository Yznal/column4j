package org.column4j;

/**
 * Column-data accessor
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface ColumnVector<T> extends Column<T> {

    /**
     * @return get column raw data
     */
    T getData();

}
