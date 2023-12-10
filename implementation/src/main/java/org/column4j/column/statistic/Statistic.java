package org.column4j.column.statistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Statistic {

    /**
     * @return index of first value in chunk
     */
    int getFirstIndex();

    /**
     * @return index of last value in chunk
     */
    int getLastIndex();

    /**
     * @return amount of values in chunk
     */
    int getCount();
}
