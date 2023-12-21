package org.column4j.storage;

import org.column4j.column.ColumnType;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.index.Dimension;
import org.column4j.index.InvertedIndex;
import org.column4j.index.v3.inverted.RoaringInvertedIndex;
import org.column4j.table.MutableTable;
import org.column4j.table.impl.MutableTableImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class StorageImpl implements Storage {

    MutableTable dataStore = new MutableTableImpl(4098);

    InvertedIndex dimensionsInvertedIndex = new RoaringInvertedIndex();


    @Override
    public List<MutableColumn<?, ?>> findColumns(Collection<Dimension> query) {
        int[] colIdxs = dimensionsInvertedIndex.lookup(query);
        if (colIdxs.length == 0) {
            return Collections.emptyList();
        }
        var result = new ArrayList<MutableColumn<?, ?>>(colIdxs.length);
        for (int idx : colIdxs) {
            result.add(dataStore.getColumn(idx));
        }
        return result;
    }

    @Override
    public MutableColumn<?, ?> createColumn(Collection<Dimension> dimensions, CharSequence name, ColumnType type) {
        int dataColumnIndex = dataStore.createColumn(name, type);
        if (dataColumnIndex == -1) {
            throw new IllegalArgumentException("Column %s already exists".formatted(name));
        }
        // we have no ability to distinguish 2 identical dimension tuples yet
        dimensionsInvertedIndex.insertValue(dimensions, dataColumnIndex);
        return dataStore.getColumn(dataColumnIndex);
    }
}
