package org.column4j.index.v3.chunk.primitive.impl.hash;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.shorts.Short2ObjectMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.column4j.index.v3.chunk.primitive.mutable.MutableInt16ChunkIndex;

import javax.annotation.Nullable;

public class HashInt16ChunkIndex extends HashChunkIndex implements MutableInt16ChunkIndex {
    Short2ObjectMap<IntSet> hashMap = new Short2ObjectOpenHashMap<>();

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
        return set.toArray(new int[0]);
    }

    @Override
    public void insertRecord(int offset, short value) {
        var set = hashMap.get(value);
        if (set == null) {
            set = new IntOpenHashSet();
            hashMap.put(value, set);
        }
        set.add(offset);
    }
}
