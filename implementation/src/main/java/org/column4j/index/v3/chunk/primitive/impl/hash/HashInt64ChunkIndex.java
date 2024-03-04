package org.column4j.index.v3.chunk.primitive.impl.hash;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.column4j.index.v3.chunk.primitive.mutable.MutableInt64ChunkIndex;

import javax.annotation.Nullable;

public class HashInt64ChunkIndex extends HashChunkIndex implements MutableInt64ChunkIndex {
    Long2ObjectMap<IntSet> hashMap = new Long2ObjectOpenHashMap<>();

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
        return set.toArray(new int[0]);
    }

    @Override
    public void insertRecord(int offset, long value) {
        var set = hashMap.get(value);
        if (set == null) {
            set = new IntOpenHashSet();
            hashMap.put(value, set);
        }
        set.add(offset);
    }
}
