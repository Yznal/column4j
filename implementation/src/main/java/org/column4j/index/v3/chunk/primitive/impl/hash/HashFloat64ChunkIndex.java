package org.column4j.index.v3.chunk.primitive.impl.hash;

import it.unimi.dsi.fastutil.doubles.Double2ObjectMap;
import it.unimi.dsi.fastutil.doubles.Double2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.column4j.index.v3.chunk.primitive.mutable.MutableFloat64ChunkIndex;

import javax.annotation.Nullable;

public class HashFloat64ChunkIndex extends HashChunkIndex implements MutableFloat64ChunkIndex {
    Double2ObjectMap<IntSet> hashMap = new Double2ObjectOpenHashMap<>();

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
        return set.toArray(new int[0]);
    }

    @Override
    public void insertRecord(int offset, double value) {
        var set = hashMap.get(value);
        if (set == null) {
            set = new IntOpenHashSet();
            hashMap.put(value, set);
        }
        set.add(offset);
    }
}
