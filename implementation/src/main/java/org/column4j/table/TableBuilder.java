package org.column4j.table;

import org.column4j.mutable.aggregated.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface TableBuilder {

    /**
     * Add byte column into the table
     *
     * @param columnName column name
     * @param column     column instance
     * @return self reference
     */
    TableBuilder column(String columnName, AggregatedByteMutableColumn column);


    /**
     * Add short column into the table
     *
     * @param columnName column name
     * @param column     column instance
     * @return self reference
     */
    TableBuilder column(String columnName, AggregatedShortMutableColumn column);


    /**
     * Add int column into the table
     *
     * @param columnName column name
     * @param column     column instance
     * @return self reference
     */
    TableBuilder column(String columnName, AggregatedIntMutableColumn column);


    /**
     * Add long column into the table
     *
     * @param columnName column name
     * @param column     column instance
     * @return self reference
     */
    TableBuilder column(String columnName, AggregatedLongMutableColumn column);


    /**
     * Add float column into the table
     *
     * @param columnName column name
     * @param column     column instance
     * @return self reference
     */
    TableBuilder column(String columnName, AggregatedFloatMutableColumn column);


    /**
     * Add double column into the table
     *
     * @param columnName column name
     * @param column     column instance
     * @return self reference
     */
    TableBuilder column(String columnName, AggregatedDoubleMutableColumn column);


    /**
     * Add string column into the table
     *
     * @param columnName column name
     * @param column     column instance
     * @return self reference
     */
    TableBuilder column(String columnName, AggregatedStringMutableColumn column);

    /**
     * Builds a new table
     *
     * @return instance of {@link Table}
     */
    Table build();

}
