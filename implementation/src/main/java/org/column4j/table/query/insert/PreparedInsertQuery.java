package org.column4j.table.query.insert;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface PreparedInsertQuery {
    /**
     * Add insertion byte value into column
     *
     * @param columnName column name
     * @return self reference
     */
    PreparedInsertQuery columnInt8(String columnName);

    /**
     * Add insertion short value into column
     *
     * @param columnName column name
     * @return self reference
     */
    PreparedInsertQuery columnInt16(String columnName);

    /**
     * Add insertion int value into column
     *
     * @param columnName column name
     * @return self reference
     */
    PreparedInsertQuery columnInt32(String columnName);

    /**
     * Add insertion long value into column
     *
     * @param columnName column name
     * @return self reference
     */
    PreparedInsertQuery columnInt64(String columnName);

    /**
     * Add insertion float value into column
     *
     * @param columnName column name
     * @return self reference
     */
    PreparedInsertQuery columnFloat32(String columnName);

    /**
     * Add insertion double value into column
     *
     * @param columnName column name
     * @return self reference
     */
    PreparedInsertQuery columnFloat64(String columnName);

    /**
     * Add insertion String value into column
     *
     * @param columnName column name
     * @return self reference
     */
    PreparedInsertQuery columnString(String columnName);

    /**
     * Commit changes
     */
    void commit(Object[] values);
}
