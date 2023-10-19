package org.column4j;

/**
 * Interface {@link Column} describe column type - sequential set of single type data
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface Column<T> {
    /**
     * The method returns the index of the first non-empty line in the column, or {@code -1}
     * if the column does not contain data.
     *
     * @return index or {@code -1} otherwise
     */
    int firstRowIndex();

    /**
     * @return amount of unique values in column
     */
    int capacity();

    /**
     *
     * @return amount of rows in column
     */
    int size();

    /**
     * @return column data type
     */
    ColumnType type();

}
