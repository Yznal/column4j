package org.column4j.index.v3.chunk.primitive.impl.hash;

import it.unimi.dsi.fastutil.bytes.Byte2ObjectMap;
import it.unimi.dsi.fastutil.bytes.Byte2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.column4j.index.v3.chunk.primitive.mutable.MutableInt8ChunkIndex;

import javax.annotation.Nullable;

public class HashInt8ChunkIndex extends HashChunkIndex implements MutableInt8ChunkIndex {
    Byte2ObjectMap<IntSet> hashMap = new Byte2ObjectOpenHashMap<>();

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
        return set.toArray(new int[0]);
    }

    @Override
    public void insertRecord(int offset, byte value) {
        var set = hashMap.get(value);
        if (set == null) {
            set = new IntOpenHashSet();
            hashMap.put(value, set);
        }
        set.add(offset);
    }
}

