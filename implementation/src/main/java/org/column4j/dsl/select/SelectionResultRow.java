package org.column4j.dsl.select;

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
    byte getInt8(String name);

    /**
     * Get short value of result set column row
     *
     * @param name column name
     * @return cell data or tombstone
     */
    short getInt16(String name);

    /**
     * Get int value of result set column row
     *
     * @param name column name
     * @return cell data or tombstone
     */
    int getInt32(String name);

    /**
     * Get long value of result set column row
     *
     * @param name column name
     * @return cell data or tombstone
     */
    long getInt64(String name);

    /**
     * Get float value of result set column row
     *
     * @param name column name
     * @return cell data or tombstone
     */
    float getFloat32(String name);

    /**
     * Get double value of result set column row
     *
     * @param name column name
     * @return cell data or tombstone
     */
    double getFloat64(String name);

    /**
     * Get String value of result set column row
     *
     * @param name column name
     * @return cell data or tombstone
     */
    String getString(String name);


}
