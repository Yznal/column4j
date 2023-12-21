package org.column4j.index.v3.chunk.primitive.impl.hash;

import org.column4j.index.v3.chunk.primitive.mutable.MutableInt32ChunkIndex;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import javax.annotation.Nullable;

public class HashInt32ChunkIndex extends HashChunkIndex implements MutableInt32ChunkIndex {

    MutableIntObjectMap<MutableIntSet> hashMap = new IntObjectHashMap<>();

    @Override
    public boolean contains(int value) {
        return hashMap.containsKey(value);
    }

    @Nullable
    @Override
    public int[] lookupValues(int value) {
        var set = hashMap.get(value);
        if (set == null) {
            return null;
        }
        return set.toArray();
    }

    @Override
    public void insertRecord(int offset, int value) {
        var set = hashMap.get(value);
        if (set == null) {
            set = new IntHashSet();
            hashMap.put(value, set);
        }
        set.add(offset);
    }
}
