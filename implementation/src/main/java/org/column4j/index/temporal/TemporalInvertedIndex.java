package org.column4j.index.temporal;

import org.column4j.index.Dimension;
import org.column4j.index.temporal.column.DimensionColumn;
import org.column4j.index.temporal.column.TemporalDimensionColumn;
import org.column4j.index.temporal.meta.ColumnMeta;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import javax.annotation.Nonnull;
import java.util.*;

public class TemporalInvertedIndex implements InvertedIndex {

    private static final int[] EMPTY = { };

    private final MutableIntObjectMap<ColumnMeta> metas = new IntObjectHashMap<>();

    private final Map<CharSequence, DimensionColumn> columns = new HashMap<>();

    @Nonnull
    @Override
    public int[] lookup(@Nonnull Collection<Dimension> query) {
        MutableIntSet result = new IntHashSet();
        boolean init = false;
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
    public ColSearchResult[] lookup(@Nonnull Collection<Dimension> query, long lower, long upper) {
        int[] columns = lookup(query);
        ColSearchResult[] result = new ColSearchResult[columns.length];
        for (int i = 0; i < columns.length; i++) {
            var colMeta = metas.get(columns[i]);
            int[] range = colMeta.searchInterval(lower, upper);
            if (range != null) {
                result[i] = new ColSearchResult(
                        colMeta.columnId,
                        colMeta.getData()[range[0]],
                        colMeta.getData()[range[1]]
                );
            }
        }

        return result;
    }

    @Override
    public void insertColumnRecord(@Nonnull Collection<Dimension> dimensions, int colId) {
        for (var dimension : dimensions) {
            DimensionColumn column = columns.get(dimension.dimensionName());
            if (column == null) {
                column = new TemporalDimensionColumn();
                columns.put(dimension.dimensionName(), column);
            }
            column.storeColRecord(dimension.dimensionValue(), colId);
        }
    }

    @Override
    public void insertChunkRecord(int colId, int chunkId, long offset, int capacity) {
        ColumnMeta meta = metas.get(colId);
        if (meta == null) {
            meta = new ColumnMeta(colId);
            metas.put(colId, meta);
        }
        meta.addChunk(chunkId, offset, capacity);
    }
}
