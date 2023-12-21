package org.column4j.dsl.select.impl.simple;

import org.column4j.dsl.select.SelectionResultRow;
import org.column4j.table.Table;

import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class SelectionResultRowImpl implements SelectionResultRow {
    private final Table table;
    private final int position;
    private final Map<String, Integer> aliasesToColumn;

    SelectionResultRowImpl(Table table, int position, Map<String, Integer> aliasesToColumn) {
        this.table = table;
        this.position = position;
        this.aliasesToColumn = aliasesToColumn;
    }

    @Override
    public byte getInt8(String name) {
        var columnIndex = getColumnIndex(name);
        return table.getInt8(columnIndex, position);
    }

    @Override
    public short getInt16(String name) {
        var columnIndex = getColumnIndex(name);
        return table.getInt16(columnIndex, position);
    }

    @Override
    public int getInt32(String name) {
        var columnIndex = getColumnIndex(name);
        return table.getInt32(columnIndex, position);
    }

    @Override
    public long getInt64(String name) {
        var columnIndex = getColumnIndex(name);
        return table.getInt64(columnIndex, position);
    }

    @Override
    public float getFloat32(String name) {
        var columnIndex = getColumnIndex(name);
        return table.getFloat32(columnIndex, position);
    }

    @Override
    public double getFloat64(String name) {
        var columnIndex = getColumnIndex(name);
        return table.getFloat64(columnIndex, position);
    }

    @Override
    public String getString(String name) {
        var columnIndex = getColumnIndex(name);
        return table.getString(columnIndex, position);
    }

    private int getColumnIndex(String name) {
        var columnIndex = aliasesToColumn.get(name);
        if (columnIndex == null) {
            throw new IllegalArgumentException("Column '%s' not in result set".formatted(name));
        }
        return columnIndex;
    }
}
