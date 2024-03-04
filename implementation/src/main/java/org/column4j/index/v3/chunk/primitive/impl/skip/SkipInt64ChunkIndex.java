package org.column4j.index.v3.chunk.primitive.impl.skip;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.column4j.index.v3.chunk.primitive.mutable.MutableInt64ChunkIndex;

import javax.annotation.Nullable;

public class SkipInt64ChunkIndex extends SkipChunkIndex implements MutableInt64ChunkIndex {

    private final long[] dataRef;

    private final long[] segmentsMin;
    private final long[] segmentsMax;

    public SkipInt64ChunkIndex(long[] data, int segmentSize) {
        super(segmentSize);
        this.dataRef = data;

        int segmentCount = data.length % segmentSize == 0
                ? data.length / segmentSize
                : data.length / segmentSize + 1;
        int segmentIdx = segmentCount - 1;
        this.segmentsMin = new long[segmentCount];
        this.segmentsMax = new long[segmentCount];
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (int i = data.length - 1; i >= 0 ; i--) {;
            if (data[i] > max) {
                max = data[i];
            }
            if (data[i] < min) {
                min = data[i];
            }
            if (i % segmentSize == 0) {
                segmentsMin[segmentIdx] = min;
                segmentsMax[segmentIdx] = max;
                min = Long.MAX_VALUE;
                max = Long.MIN_VALUE;
                segmentIdx--;
            }
        }
    }

    public SkipInt64ChunkIndex(long[] data) {
        this(data, DEFAULT_SEGMENT_SIZE);
    }

    @Override
    public boolean contains(long value) {
        for (int s = 0; s < segmentsMin.length; s++) {
            if (value >= segmentsMin[s] && value <= segmentsMax[s]) {
                if (value == segmentsMin[s] || value == segmentsMax[s]){
                    return true;
                }
                for (int i = s*segmentSize; i < (s + 1)*segmentSize && i < dataRef.length; i++) {
                    if (dataRef[i] == value) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Nullable
    @Override
    public int[] lookupValues(long value) {
        IntArrayList res = new IntArrayList();
        for (int s = 0; s < segmentsMin.length; s++) {
            if (value >= segmentsMin[s] && value <= segmentsMax[s]) {
                for (int i = s*segmentSize; i < (s + 1)*segmentSize && i < dataRef.length; i++) {
                    if (dataRef[i] == value) {
                        res.add(i);
                    }
                }
            }
        }
        return res.isEmpty() ? null : res.toArray(new int[0]);
    }

    @Override
    public void insertRecord(int offset, long value) {
        int segment = offset / segmentSize;
        if (value > segmentsMax[segment]) {
            segmentsMax[segment] = value;
        }
        if (value < segmentsMin[segment]) {
            segmentsMin[segment] = value;
        }
    }
}

