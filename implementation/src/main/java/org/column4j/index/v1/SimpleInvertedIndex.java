package org.column4j.index.v1;

import org.column4j.index.Dimension;
import org.column4j.index.InvertedIndex;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * Functional prototype of index implementation
 */
public class SimpleInvertedIndex implements InvertedIndex {

    private static final int[] EMPTY = { };

    private final Map<CharSequence, IndexColumn> columnsIndex = new HashMap<>();

    @Nonnull
    @Override
    public int[] lookup(@Nonnull Collection<Dimension> query) {
        Set<Integer> result = new TreeSet<>();
        boolean init = false;
        for (var dimension : query) {

            IndexColumn column = columnsIndex.get(dimension.dimensionName());
            if (column == null) {
                return EMPTY;
            }

            Collection<Integer> columnResult = column.lookupDimension(dimension.dimensionValue());
            if (columnResult.isEmpty()) {
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
        int[] res = new int[result.size()];
        int idx = 0;
        for (int el : result) {
            res[idx++] = el;
        }
        return res;
    }

    @Override
    public void insertValue(@Nonnull Collection<Dimension> dimensions, int value) {
        for (var dimension : dimensions) {
            IndexColumn column = columnsIndex.get(dimension.dimensionName());
            if (column == null) {
                column = new DimensionColumn();
                columnsIndex.put(dimension.dimensionName(), column);
            }
            column.insertValue(dimension.dimensionValue(), value);
        }
    }


    private static class DimensionColumn implements IndexColumn {
        Map<CharSequence, Set<Integer>> dimensionValues = new HashMap<>();

        @Nonnull
        @Override
        public Collection<Integer> lookupDimension(@Nonnull CharSequence dimensionValue) {
            return dimensionValues.getOrDefault(dimensionValue, Collections.emptySet());
        }

        @Override
        public void insertValue(@Nonnull CharSequence dmensionValue, int id) {
            Set<Integer> values = dimensionValues.get(dmensionValue);
            if (values == null) {
                values = new TreeSet<>();
                dimensionValues.put(dmensionValue, values);
            }
            values.add(id);
        }
    }
}
