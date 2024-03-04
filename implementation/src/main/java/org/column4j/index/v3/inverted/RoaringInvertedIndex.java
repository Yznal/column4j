package org.column4j.index.v3.inverted;

import org.column4j.index.Dimension;
import org.column4j.index.InvertedIndex;
import org.column4j.index.v3.inverted.column.DimensionColumn;
import org.column4j.index.v3.inverted.column.DimensionColumnImpl;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nonnull;
import java.util.*;

public class RoaringInvertedIndex implements InvertedIndex {
    private static final int[] EMPTY = { };
    private static RoaringBitmap empty() {
        return new RoaringBitmap();
    }

    private final Map<CharSequence, DimensionColumn> columns = new HashMap<>();

    @Nonnull
    private RoaringBitmap lookupSingleColumn(Dimension dimension) {
        DimensionColumn column = columns.get(dimension.dimensionName());
        if (column == null) {
            return empty();
        }

        RoaringBitmap columnResult = column.lookup(dimension.dimensionValue());
        if (columnResult == null) {
            return empty();
        }
        return columnResult;
    }


    private int[] lookupQuery(@Nonnull Collection<Dimension> query) {
        assert !query.isEmpty(); // remove comp warn

        RoaringBitmap result = null;
        for (var dimension : query) {

            RoaringBitmap columnResult = lookupSingleColumn(dimension);

            if (result == null) {
                result = columnResult.clone();
            }  else {
                result.and(columnResult);
            }

            if (result.isEmpty()) {
                return EMPTY;
            }
        }
        return result.toArray();
    }

    @Nonnull
    @Override
    public int[] lookup(@Nonnull Collection<Dimension> query) {
        if (query.isEmpty()) {
            return EMPTY;
        }
        if (query.size() == 1) {
            return lookupSingleColumn(query.iterator().next()).toArray();
        }
        return lookupQuery(query);
    }

    @Override
    public void insertValue(@Nonnull Collection<Dimension> dimensions, int colId) {
        for (var dimension : dimensions) {
            DimensionColumn column = columns.get(dimension.dimensionName());
            if (column == null) {
                column = new DimensionColumnImpl();
                columns.put(dimension.dimensionName(), column);
            }
            column.storeColRecord(dimension.dimensionValue(), colId);
        }
    }
}
