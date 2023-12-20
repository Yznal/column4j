package org.column4j.index.v3.chunk.primitive.impl.hash;

import org.column4j.index.v3.chunk.primitive.mutable.MutableFloat64ChunkIndex;
import org.eclipse.collections.api.map.primitive.MutableDoubleObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.DoubleObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import javax.annotation.Nullable;

public class HashFloat64ChunkIndex extends HashChunkIndex implements MutableFloat64ChunkIndex {
    MutableDoubleObjectMap<MutableIntSet> hashMap = new DoubleObjectHashMap<>();

    @Override
    public boolean contains(double value) {
        return hashMap.containsKey(value);
    }

    @Nullable
    @Override
    public int[] lookupValues(double value) {
        var set = hashMap.get(value);
        if (set == null) {
            return null;
        }
        return set.toArray();
    }

    @Override
    public void insertRecord(int offset, double value) {
        var set = hashMap.get(value);
        if (set == null) {
            set = new IntHashSet();
        }
        set.add(offset);
    }
}
