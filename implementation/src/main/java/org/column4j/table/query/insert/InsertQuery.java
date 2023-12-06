package org.column4j.table.query.insert;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface InsertQuery {
    /**
     * Insert byte value into column
     * @param columnName column name
     * @param value byte value
     * @return self reference
     */
    InsertQuery column(String columnName, byte value);

    /**
     * Insert short value into column
     * @param columnName column name
     * @param value short value
     * @return self reference
     */
    InsertQuery column(String columnName, short value);

    /**
     * Insert int value into column
     * @param columnName column name
     * @param value int value
     * @return self reference
     */
    InsertQuery column(String columnName, int value);

    /**
     * Insert long value into column
     * @param columnName column name
     * @param value long value
     * @return self reference
     */
    InsertQuery column(String columnName, long value);

    /**
     * Insert float value into column
     * @param columnName column name
     * @param value float value
     * @return self reference
     */
    InsertQuery column(String columnName, float value);

    /**
     * Insert double value into column
     * @param columnName column name
     * @param value double value
     * @return self reference
     */
    InsertQuery column(String columnName, double value);

    /**
     * Insert String value into column
     * @param columnName column name
     * @param value String value
     * @return self reference
     */
    InsertQuery column(String columnName, String value);

    /**
     * Commit changes
     */
    void commit();
}
