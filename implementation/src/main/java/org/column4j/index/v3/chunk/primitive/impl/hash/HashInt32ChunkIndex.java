package org.column4j.index.v3.chunk.primitive.impl.hash;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.column4j.index.v3.chunk.primitive.mutable.MutableInt32ChunkIndex;

import javax.annotation.Nullable;

public class HashInt32ChunkIndex extends HashChunkIndex implements MutableInt32ChunkIndex {

    Int2ObjectMap<IntSet> hashMap = new Int2ObjectOpenHashMap<>();

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
        return set.toArray(new int[0]);
    }

    @Override
    public void insertRecord(int offset, int value) {
        var set = hashMap.get(value);
        if (set == null) {
            set = new IntOpenHashSet();
            hashMap.put(value, set);
        }
        set.add(offset);
    }
}
