package org.column4j.index.v3.chunk.primitive.impl.hash;

import org.column4j.index.v3.chunk.primitive.mutable.MutableInt64ChunkIndex;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import javax.annotation.Nullable;

public class HashInt64ChunkIndex extends HashChunkIndex implements MutableInt64ChunkIndex {
    MutableLongObjectMap<MutableIntSet> hashMap = new LongObjectHashMap<>();

    @Override
    public boolean contains(long value) {
        return hashMap.containsKey(value);
    }

    @Nullable
    @Override
    public int[] lookupValues(long value) {
        var set = hashMap.get(value);
        if (set == null) {
            return null;
        }
        return set.toArray();
    }

    @Override
    public void insertRecord(int offset, long value) {
        var set = hashMap.get(value);
        if (set == null) {
            set = new IntHashSet();
            hashMap.put(value, set);
        }
        set.add(offset);
    }
}
