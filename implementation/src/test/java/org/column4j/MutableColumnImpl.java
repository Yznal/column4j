package org.column4j;

import org.column4j.base.ColumnType;
import org.column4j.base.ColumnVector;
import org.column4j.base.MutableColumn;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class MutableColumnImpl<T> implements MutableColumn<T>, ColumnVector<T> {
    private final ColumnType columnType;

    public MutableColumnImpl(ColumnType columnType) {
        this.columnType = columnType;
    }

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
        return columnType;
    }

    @Override
    public void write(int pos, double value) {

    }

    @Override
    public void write(int pos, float value) {

    }

    @Override
    public void write(int pos, long value) {

    }

    @Override
    public void write(int pos, int value) {

    }

    @Override
    public void write(int pos, short value) {

    }

    @Override
    public void write(int pos, byte value) {

    }

    @Override
    public void write(int pos, boolean value) {

    }

    @Override
    public void write(int pos, CharSequence value) {

    }

    @Override
    public void tombstone(int pos) {

    }

    @Override
    public T data() {
        return null;
    }
}
