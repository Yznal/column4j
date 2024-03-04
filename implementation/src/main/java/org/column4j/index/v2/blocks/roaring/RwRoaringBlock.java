package org.column4j.index.v2.blocks.roaring;

import org.column4j.index.v2.blocks.MutableBlock;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class RwRoaringBlock implements MutableBlock<RoaringBitmap> {

    private static final long BYTE_SIZE_THRESHOLD = 64L * 1024L * 1024L; // 64Mb

    private final MutableIntObjectMap<RoaringBitmap> values = new IntObjectHashMap<>();

    private volatile long estimatedByteSize = 0;

    private volatile boolean finalizable = false;

    @Nullable
    @Override
    public synchronized RoaringBitmap lookup(int idx) {
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
        RoaringBitmap rbm = values.get(key);
        if (rbm == null) {
            rbm = new RoaringBitmap();
            values.put(key, rbm);
        }
        // very estimated, need to check
        int sizeDiff = -rbm.getSizeInBytes();

        rbm.add(value);
        sizeDiff += rbm.getSizeInBytes();
        estimatedByteSize += sizeDiff;
        checkFinal();
    }

    @Override
    public boolean finalizable() {
        return finalizable;
    }

    @Override
    public MutableIntObjectMap<RoaringBitmap> getData() {
        return values;
    }

    private void checkFinal() {
        finalizable = estimatedByteSize >= BYTE_SIZE_THRESHOLD;
    }
}
