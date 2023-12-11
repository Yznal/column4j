package org.column4j.index.v3.inverted.column;

import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class TemporalDimensionColumn implements DimensionColumn {

    private final Map<CharSequence, RoaringBitmap> dataPointers = new HashMap<>();


    @Override
    public void storeColRecord(CharSequence dimValue, int colIndex) {
        RoaringBitmap pointers = dataPointers.get(dimValue);
        if (pointers == null) {
            pointers = new RoaringBitmap();
            dataPointers.put(dimValue, pointers);
        }
        pointers.add(colIndex);
    }

    @Nullable
    @Override
    public RoaringBitmap lookup(CharSequence dimName) {
        RoaringBitmap pointers = dataPointers.get(dimName);
        if (pointers == null) {
            return RoaringBitmap.
        }
        // maybe store in caller-provided accumulator instead
        return pointers;
    }
}
