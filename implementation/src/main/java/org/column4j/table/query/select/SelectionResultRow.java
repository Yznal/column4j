package org.column4j.table.query.select;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface SelectionResultRow {

    /**
     * Get byte value of result set column row
     *
     * @param name column name
     * @return cell data or tombstone
     */
    byte getByte(String name);

    /**
     * Get short value of result set column row
     *
     * @param name column name
     * @return cell data or tombstone
     */
    short getShort(String name);

    /**
     * Get int value of result set column row
     *
     * @param name column name
     * @return cell data or tombstone
     */
    int getInt(String name);

    /**
     * Get long value of result set column row
     *
     * @param name column name
     * @return cell data or tombstone
     */
    long getLong(String name);

    /**
     * Get float value of result set column row
     *
     * @param name column name
     * @return cell data or tombstone
     */
    float getFloat(String name);

    /**
     * Get double value of result set column row
     *
     * @param name column name
     * @return cell data or tombstone
     */
    double getDouble(String name);

    /**
     * Get String value of result set column row
     *
     * @param name column name
     * @return cell data or tombstone
     */
    String getString(String name);


}
