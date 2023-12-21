package org.column4j.index.v2;

import org.column4j.index.Dimension;
import org.column4j.index.InvertedIndex;
import org.column4j.index.v2.skiplist.RoaringSkipList;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nonnull;
import java.util.*;

public class BlockInvertedIndex implements InvertedIndex {

    private static final int[] EMPTY = { };

    private final Map<CharSequence, IndexColumn<RoaringBitmap>> columns = new HashMap<>();


    @Override
    @Nonnull
    public int[] lookup(@Nonnull Collection<Dimension> query) {
        RoaringBitmap result = new RoaringBitmap();
        boolean init = false;
        for (var dimension : query) {

            IndexColumn<RoaringBitmap> column = columns.get(dimension.dimensionName());
            if (column == null) {
                return EMPTY;
            }

            RoaringBitmap columnResult = column.lookupIndex(dimension.dimensionValue());
            if (columnResult == null || columnResult.isEmpty()) {
                return EMPTY;
            }

            if (!init) {
                result = columnResult;
                init = true;
            }  else {
                result.and(columnResult);
            }

            if (result.isEmpty()) {
                return EMPTY;
            }
        }

        return result.toArray();
    }

    @Override
    public void insertValue(@Nonnull Collection<Dimension> dimensions, int value) {
        for (var dimension : dimensions) {
            IndexColumn<RoaringBitmap> column = columns.get(dimension.dimensionName());
            if (column == null) {
                column = new RoaringSkipList();
                columns.put(dimension.dimensionName(), column);
            }
            column.store(dimension.dimensionValue(), value);
        }
    }
}
