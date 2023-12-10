package org.column4j.table;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface MutableTable extends Table {

    /**
     * Write {@link org.column4j.column.ColumnType#INT8} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columName column name
     * @param position position in column to write
     * @param value value to write
     */
    default void writeInt8(String columName, int position, byte value) {
        var index = getColumnIndex(columName);
        if(index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columName));
        }
        writeInt8(index, position, value);
    }

    /**
     * Write {@link org.column4j.column.ColumnType#INT8} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position position in column to write
     * @param value value to write
     */
    void writeInt8(int columnIndex, int position, byte value);

    /**
     * Write {@link org.column4j.column.ColumnType#INT16} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columName column name
     * @param position position in column to write
     * @param value value to write
     */
    default void writeInt16(String columName, int position, short value) {
        var index = getColumnIndex(columName);
        if(index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columName));
        }
        writeInt16(index, position, value);
    }

    /**
     * Write {@link org.column4j.column.ColumnType#INT16} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position position in column to write
     * @param value value to write
     */
    void writeInt16(int columnIndex, int position, short value);


    /**
     * Write {@link org.column4j.column.ColumnType#INT32} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columName column name
     * @param position position in column to write
     * @param value value to write
     */
    default void writeInt32(String columName, int position, int value) {
        var index = getColumnIndex(columName);
        if(index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columName));
        }
        writeInt32(index, position, value);
    }

    /**
     * Write {@link org.column4j.column.ColumnType#INT32} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position position in column to write
     * @param value value to write
     */
    void writeInt32(int columnIndex, int position, int value);

    /**
     * Write {@link org.column4j.column.ColumnType#INT64} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columName column name
     * @param position position in column to write
     * @param value value to write
     */
    default void writeInt64(String columName, int position, long value) {
        var index = getColumnIndex(columName);
        if(index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columName));
        }
        writeInt64(index, position, value);
    }

    /**
     * Write {@link org.column4j.column.ColumnType#INT64} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position position in column to write
     * @param value value to write
     */
    void writeInt64(int columnIndex, int position, long value);

    /**
     * Write {@link org.column4j.column.ColumnType#FLOAT32} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columName column name
     * @param position position in column to write
     * @param value value to write
     */
    default void writeFloat32(String columName, int position, float value) {
        var index = getColumnIndex(columName);
        if(index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columName));
        }
        writeFloat32(index, position, value);
    }

    /**
     * Write {@link org.column4j.column.ColumnType#FLOAT32} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position position in column to write
     * @param value value to write
     */
    void writeFloat32(int columnIndex, int position, float value);

    /**
     * Write {@link org.column4j.column.ColumnType#FLOAT64} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columName column name
     * @param position position in column to write
     * @param value value to write
     */
    default void writeFloat64(String columName, int position, double value) {
        var index = getColumnIndex(columName);
        if(index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columName));
        }
        writeFloat64(index, position, value);
    }

    /**
     * Write {@link org.column4j.column.ColumnType#FLOAT64} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position position in column to write
     * @param value value to write
     */
    void writeFloat64(int columnIndex, int position, double value);

    /**
     * Write {@link org.column4j.column.ColumnType#STRING} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columName column name
     * @param position position in column to write
     * @param value value to write
     */
    default void writeString(String columName, int position, String value) {
        var index = getColumnIndex(columName);
        if(index == -1) {
            throw new IllegalArgumentException("Column %s doesn't exist".formatted(columName));
        }
        writeString(index, position, value);
    }

    /**
     * Write {@link org.column4j.column.ColumnType#STRING} value into column.
     * In case if column doesn't exist or has another type {@link IllegalArgumentException} should be thrown.
     *
     * @param columnIndex column index
     * @param position position in column to write
     * @param value value to write
     */
    void writeString(int columnIndex, int position, String value);
}
