package org.column4j.index.v3.inverted.column;

import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class TemporalDimensionColumn implements DimensionColumn {

    private final Map<CharSequence, MutableIntList> dataPointers = new HashMap<>();


    @Override
    public void storeColRecord(CharSequence dimValue, int colIndex) {
        MutableIntList pointers = dataPointers.get(dimValue);
        if (pointers == null) {
            pointers = new IntArrayList(10);
            dataPointers.put(dimValue, pointers);
        }
        if (!pointers.contains(colIndex)) {
            pointers.add(colIndex);
        }
    }

    @Nullable
    @Override
    public int[] lookup(CharSequence dimName) {
        MutableIntList pointers = dataPointers.get(dimName);
        if (pointers == null) {
            return null;
        }
        // maybe store in caller-provided accumulator instead
        return pointers.toArray();
    }

}
