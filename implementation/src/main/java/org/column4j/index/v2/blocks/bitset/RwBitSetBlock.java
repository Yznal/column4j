package org.column4j.index.v2.blocks.bitset;

import org.column4j.index.v2.blocks.MutableBlock;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.BitSet;

@ThreadSafe
public class RwBitSetBlock implements MutableBlock<BitSet> {

    private static final long BIT_SIZE_THRESHOLD = 8L * 64L * 1024L * 1024L; // 64Mb

    private final MutableIntObjectMap<BitSet> values = new IntObjectHashMap<>();

    private volatile long estimatedBitSize = 0;

    private volatile boolean finalizable = false;

    @Nullable
    @Override
    public synchronized BitSet lookup(int idx) {
        return values.get(idx);
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public int minKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int maxKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void addRecord(int key, int value) {
        BitSet bs = values.get(key);
        if (bs == null) {
            bs = new BitSet();
            values.put(key, bs);
        }
        int sizeDiff = -bs.size();

        bs.set(value);
        sizeDiff += bs.size();
        estimatedBitSize += sizeDiff;
        checkFinal();
    }

    @Override
    public boolean finalizable() {
        return finalizable;
    }

    @Override
    public MutableIntObjectMap<BitSet> getData() {
        return values;
    }

    private void checkFinal() {
        finalizable = estimatedBitSize >= BIT_SIZE_THRESHOLD;
    }
}
