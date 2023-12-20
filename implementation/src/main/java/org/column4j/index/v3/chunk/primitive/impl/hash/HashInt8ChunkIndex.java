package org.column4j.index.v3.chunk.primitive.impl.hash;

import org.column4j.index.v3.chunk.primitive.mutable.MutableInt8ChunkIndex;
import org.eclipse.collections.api.map.primitive.MutableByteObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.ByteObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import javax.annotation.Nullable;

public class HashInt8ChunkIndex extends HashChunkIndex implements MutableInt8ChunkIndex {
    MutableByteObjectMap<MutableIntSet> hashMap = new ByteObjectHashMap<>();

    @Override
    public boolean contains(byte value) {
        return hashMap.containsKey(value);
    }

    @Nullable
    @Override
    public int[] lookupValues(byte value) {
        var set = hashMap.get(value);
        if (set == null) {
            return null;
        }
        return set.toArray();
    }

    @Override
    public void insertRecord(int offset, byte value) {
        var set = hashMap.get(value);
        if (set == null) {
            set = new IntHashSet();
        }
        set.add(offset);
    }
}

