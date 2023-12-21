package org.column4j.index.v3.chunk.primitive.impl.hash;

import it.unimi.dsi.fastutil.floats.Float2ObjectMap;
import it.unimi.dsi.fastutil.floats.Float2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.column4j.index.v3.chunk.primitive.mutable.MutableFloat32ChunkIndex;

import javax.annotation.Nullable;

public class HashFloat32ChunkIndex extends HashChunkIndex implements MutableFloat32ChunkIndex {
    Float2ObjectMap<IntSet> hashMap =  new Float2ObjectOpenHashMap<>();

    @Override
    public boolean contains(float value) {
        return hashMap.containsKey(value);
    }

    @Nullable
    @Override
    public int[] lookupValues(float value) {
        var set = hashMap.get(value);
        if (set == null) {
            return null;
        }
        return set.toArray(new int[0]);
    }

    @Override
    public void insertRecord(int offset, float value) {
        var set = hashMap.get(value);
        if (set == null) {
            set = new IntOpenHashSet();
            hashMap.put(value, set);
        }
        set.add(offset);
    }
}
