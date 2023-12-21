package org.column4j.index.v3.chunk.primitive.impl.hash;

import org.column4j.index.v3.chunk.primitive.mutable.MutableFloat32ChunkIndex;
import org.eclipse.collections.api.map.primitive.MutableFloatObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.FloatObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import javax.annotation.Nullable;

public class HashFloat32ChunkIndex extends HashChunkIndex implements MutableFloat32ChunkIndex {
    MutableFloatObjectMap<MutableIntSet> hashMap = new FloatObjectHashMap<>();

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
        return set.toArray();
    }

    @Override
    public void insertRecord(int offset, float value) {
        var set = hashMap.get(value);
        if (set == null) {
            set = new IntHashSet();
            hashMap.put(value, set);
        }
        set.add(offset);
    }
}
