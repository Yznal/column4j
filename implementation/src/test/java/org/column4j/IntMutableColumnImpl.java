package org.column4j;

import org.column4j.mutable.primitive.IntMutableColumn;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class IntMutableColumnImpl implements IntMutableColumn, ColumnVector<int[]> {

    @Override
    public int firstRowIndex() {
        return 0;
    }

    @Override
    public int capacity() {
        return 0;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public ColumnType type() {
        return ColumnType.INT32;
    }

    @Override
    public int[] getData() {
        return new int[0];
    }

    @Override
    public void write(int pos, int value) {

    }

    @Override
    public void tombstone(int pos) {

    }
}
