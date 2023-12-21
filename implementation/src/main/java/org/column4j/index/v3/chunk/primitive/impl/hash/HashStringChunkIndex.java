package org.column4j.index.v3.chunk.primitive.impl.hash;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.column4j.index.v3.chunk.primitive.mutable.MutableStringChunkIndex;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class HashStringChunkIndex extends HashChunkIndex implements MutableStringChunkIndex {

    Map<CharSequence, IntSet> hashMap = new HashMap<>();

    @Override
    public void insertRecord(int offset, @Nonnull String value) {
        var set = hashMap.get(value);
        if (set == null) {
            set = new IntOpenHashSet();
            hashMap.put(value, set);
        }
        set.add(offset);
    }
    @Override
    public boolean contains(@Nonnull String value) {
        return hashMap.containsKey(value);
    }

    @Nullable
    @Override
    public int[] lookupValues(@Nonnull String value) {
        var set = hashMap.get(value);
        if (set == null) {
            return null;
        }
        return set.toArray(new int[0]);
    }
}
