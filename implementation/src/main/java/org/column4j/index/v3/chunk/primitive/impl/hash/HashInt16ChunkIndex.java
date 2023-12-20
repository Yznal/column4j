package org.column4j.index.v3.chunk.primitive.impl.hash;

import org.column4j.index.v3.chunk.primitive.mutable.MutableInt16ChunkIndex;
import org.eclipse.collections.api.map.primitive.MutableShortObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.ShortObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import javax.annotation.Nullable;

public class HashInt16ChunkIndex extends HashChunkIndex implements MutableInt16ChunkIndex {
    MutableShortObjectMap<MutableIntSet> hashMap = new ShortObjectHashMap<>();

    @Override
    public boolean contains(short value) {
        return hashMap.containsKey(value);
    }

    @Nullable
    @Override
    public int[] lookupValues(short value) {
        var set = hashMap.get(value);
        if (set == null) {
            return null;
        }
        return set.toArray();
    }

    @Override
    public void insertRecord(int offset, short value) {
        var set = hashMap.get(value);
        if (set == null) {
            set = new IntHashSet();
        }
        set.add(offset);
    }
}
