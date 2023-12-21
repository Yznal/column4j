package org.column4j.table;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Table {
    /**
     * Get column index in table by name
     *
     * @param columnName column name
     * @return column index or -1 if column not presented in table
     */
    int getColumnIndex(String columnName);

    /**
     * Get {@link org.column4j.column.ColumnType#INT8} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnName column name
     * @param position   position in column to get
     * @return column value
     */
    default byte getInt8(String columnName, int position) {
        var index = getColumnIndex(columnName);
        if (index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columnName));
        }
        return getInt8(index, position);
    }

    /**
     * Get {@link org.column4j.column.ColumnType#INT8} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position    position in column to get
     * @return column value
     */
    byte getInt8(int columnIndex, int position);

    /**
     * Get {@link org.column4j.column.ColumnType#INT16} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnName column name
     * @param position   position in column to get
     * @return column value
     */
    default short getInt16(String columnName, int position) {
        var index = getColumnIndex(columnName);
        if (index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columnName));
        }
        return getInt16(index, position);
    }

    /**
     * Get {@link org.column4j.column.ColumnType#INT16} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position    position in column to get
     * @return column value
     */
    short getInt16(int columnIndex, int position);


    /**
     * Get {@link org.column4j.column.ColumnType#INT32} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnName column name
     * @param position   position in column to get
     * @return column value
     */
    default int getInt32(String columnName, int position) {
        var index = getColumnIndex(columnName);
        if (index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columnName));
        }
        return getInt32(index, position);
    }

    /**
     * Get {@link org.column4j.column.ColumnType#INT32} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position    position in column to get
     * @return column value
     */
    int getInt32(int columnIndex, int position);

    /**
     * Get {@link org.column4j.column.ColumnType#INT64} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnName column name
     * @param position   position in column to get
     * @return column value
     */
    default long getInt64(String columnName, int position) {
        var index = getColumnIndex(columnName);
        if (index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columnName));
        }
        return getInt64(index, position);
    }

    /**
     * Get {@link org.column4j.column.ColumnType#INT64} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position    position in column to get
     * @return column value
     */
    long getInt64(int columnIndex, int position);

    /**
     * Get {@link org.column4j.column.ColumnType#FLOAT32} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnName column name
     * @param position   position in column to get
     * @return column value
     */
    default float getFloat32(String columnName, int position) {
        var index = getColumnIndex(columnName);
        if (index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columnName));
        }
        return getFloat32(index, position);
    }

    /**
     * Get {@link org.column4j.column.ColumnType#FLOAT32} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position    position in column to get
     * @return column value
     */
    float getFloat32(int columnIndex, int position);

    /**
     * Get {@link org.column4j.column.ColumnType#FLOAT64} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnName column name
     * @param position   position in column to get
     * @return column value
     */
    default double getFloat64(String columnName, int position) {
        var index = getColumnIndex(columnName);
        if (index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columnName));
        }
        return getFloat64(index, position);
    }

    /**
     * Get {@link org.column4j.column.ColumnType#FLOAT64} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position    position in column to get
     * @return column value
     */
    double getFloat64(int columnIndex, int position);

    /**
     * Get {@link org.column4j.column.ColumnType#STRING} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnName column name
     * @param position   position in column to get
     * @return column value
     */
    default String getString(String columnName, int position) {
        var index = getColumnIndex(columnName);
        if (index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columnName));
        }
        return getString(index, position);
    }

    /**
     * Get {@link org.column4j.column.ColumnType#STRING} column value.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position    position in column to get
     * @return column value
     */
    String getString(int columnIndex, int position);


    /**
     * Get column values by indexes array.
     * In case if column doesn't exist {@link IllegalArgumentException} should be thrown.
     *
     * @param columnName column name
     * @param indexes    indexes to get
     * @return column values array, size to - from + 1
     */
    default <T> T getByIndexes(String columnName, int[] indexes) {
        var index = getColumnIndex(columnName);
        if (index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columnName));
        }
        return getByIndexes(index, indexes);
    }

    /**
     * Get column values by indexes array.
     * In case if column doesn't exist {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param indexes     indexes to get
     * @return column values array, size the same as indexes
     */
    <T> T getByIndexes(int columnIndex, int[] indexes);

    /**
     * Get column values by indexes bounds.
     * In case if column doesn't exist {@link IllegalArgumentException} should be thrown.
     *
     * @param columnName column name
     * @param from       left bound inclusive
     * @param to         right bound inclusive
     * @return column values array, size to - from + 1
     */
    default <T> T getByIndexes(String columnName, int from, int to) {
        var index = getColumnIndex(columnName);
        if (index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columnName));
        }
        return getByIndexes(index, from, to);
    }

    /**
     * Get column values by indexes bounds.
     * In case if column doesn't exist {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param from        left bound inclusive
     * @param to          right bound inclusive
     * @return column values array, size to - from + 1
     */
    <T> T getByIndexes(int columnIndex, int from, int to);

    /**
     * Get column values by indexes array and write into buffer.
     * In case if column doesn't exist {@link IllegalArgumentException} should be thrown.
     * Buffer size must be the same length as indexes, otherwise {@link IllegalArgumentException} should be thrown.
     *
     * @param columnName column name
     * @param indexes    indexes to get
     * @param buffer     buffer to write values
     */
    default <T> void readByIndexes(String columnName, int[] indexes, T buffer) {
        var index = getColumnIndex(columnName);
        if (index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columnName));
        }
        readByIndexes(index, indexes, buffer);
    }

    /**
     * Get column values by indexes array and write into buffer.
     * In case if column doesn't exist {@link IllegalArgumentException} should be thrown.
     * Buffer size must be the same length as indexes, otherwise {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param indexes     indexes to get
     * @param buffer      buffer to write values
     */
    <T> void readByIndexes(int columnIndex, int[] indexes, T buffer);

    /**
     * Get column values by indexes bounds and write into buffer.
     * In case if column doesn't exist {@link IllegalArgumentException} should be thrown.
     * Buffer size must be the same length as indexes, otherwise {@link IllegalArgumentException} should be thrown.
     *
     * @param columnName column name
     * @param from       left bound inclusive
     * @param to         right bound inclusive
     * @param buffer     buffer to write values
     */
    default <T> void readByIndexes(String columnName, int from, int to, T buffer) {
        var index = getColumnIndex(columnName);
        if (index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columnName));
        }
        readByIndexes(index, from, to, buffer);
    }

    /**
     * Get column values by indexes bounds and write into buffer.
     * In case if column doesn't exist {@link IllegalArgumentException} should be thrown.
     * Buffer size must be the same length as indexes, otherwise {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param from        left bound inclusive
     * @param to          right bound inclusive
     * @param buffer      buffer to write values
     */
    <T> void readByIndexes(int columnIndex, int from, int to, T buffer);

    /**
     * @return last inserted row cursor
     */
    int getCursor();

}
