package org.column4j.index.v3.inverted;

import org.column4j.index.Dimension;
import org.column4j.index.InvertedIndex;
import org.column4j.index.v3.inverted.column.DimensionColumn;
import org.column4j.index.v3.inverted.column.TemporalDimensionColumn;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nonnull;
import java.util.*;

public class TemporalInvertedIndex implements InvertedIndex {

    private static final int[] EMPTY = { };

    private final Map<CharSequence, DimensionColumn> columns = new HashMap<>();

    @Nonnull
    @Override
    public int[] lookup(@Nonnull Collection<Dimension> query) {
        MutableIntSet result = new IntHashSet();
        boolean init = false;
        RoaringBitmap r = new RoaringBitmap();
        for (var dimension : query) {

            DimensionColumn column = columns.get(dimension.dimensionName());
            if (column == null) {
                return EMPTY;
            }

            int[] columnResult = column.lookup(dimension.dimensionValue());
            if (columnResult == null) {
                return EMPTY;
            }

            if (!init) {
                result.addAll(columnResult);
                init = true;
            }  else {
                result.retainAll(columnResult);
            }

            if (result.isEmpty()) {
                return EMPTY;
            }
        }
        return result.toArray();
    }

    @Override
    public void insertValue(@Nonnull Collection<Dimension> dimensions, int colId) {
        for (var dimension : dimensions) {
            DimensionColumn column = columns.get(dimension.dimensionName());
            if (column == null) {
                column = new TemporalDimensionColumn();
                columns.put(dimension.dimensionName(), column);
            }
            column.storeColRecord(dimension.dimensionValue(), colId);
        }
    }
}
