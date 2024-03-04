package org.column4j.index.v3.chunk.primitive.impl.skip;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.column4j.index.v3.chunk.primitive.mutable.MutableStringChunkIndex;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

public class SkipStringChunkIndex extends SkipChunkIndex implements MutableStringChunkIndex {

    private final String[] dataRef;

    private final int[] segmentsMin;
    private final int[] segmentsMax;

    public SkipStringChunkIndex(String[] data, int segmentSize) {
        super(segmentSize);
        this.dataRef = data;

        int segmentCount = data.length % segmentSize == 0
                ? data.length / segmentSize
                : data.length / segmentSize + 1;
        int segmentIdx = segmentCount - 1;
        this.segmentsMin = new int[segmentCount];
        this.segmentsMax = new int[segmentCount];
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int i = data.length - 1; i >= 0 ; i--) {
            int valueHash = data[i].hashCode();
            if (valueHash > max) {
                max = valueHash;
            }
            if (valueHash < min) {
                min = valueHash;
            }
            if (i % segmentSize == 0) {
                segmentsMin[segmentIdx] = min;
                segmentsMax[segmentIdx] = max;
                min = Integer.MAX_VALUE;
                max = Integer.MIN_VALUE;
                segmentIdx--;
            }
        }
    }

    public SkipStringChunkIndex(String[] data) {
        this(data, DEFAULT_SEGMENT_SIZE);
    }

    @Override
    public boolean contains(@Nonnull String value) {
        int valueHash = value.hashCode();
        for (int s = 0; s < segmentsMin.length; s++) {
            if (valueHash >= segmentsMin[s] && valueHash <= segmentsMax[s]) {
                for (int i = s*segmentSize; i < (s + 1)*segmentSize && i < dataRef.length; i++) {
                    if (Objects.equals(dataRef[i], value)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Nullable
    @Override
    public int[] lookupValues(@Nonnull String value) {
        IntArrayList res = new IntArrayList();
        int valueHash = value.hashCode();
        for (int s = 0; s < segmentsMin.length; s++) {
            if (valueHash >= segmentsMin[s] && valueHash <= segmentsMax[s]) {
                for (int i = s*segmentSize; i < (s + 1)*segmentSize && i < dataRef.length; i++) {
                    if (Objects.equals(dataRef[i], value)) {
                        res.add(i);
                    }
                }
            }
        }
        return res.isEmpty() ? null : res.toArray(new int[0]);
    }

    @Override
    public void insertRecord(int offset, @Nonnull String value) {
        int segment = offset / segmentSize;
        int valueHash = value.hashCode();
        if (valueHash > segmentsMax[segment]) {
            segmentsMax[segment] = valueHash;
        }
        if (valueHash < segmentsMin[segment]) {
            segmentsMin[segment] = valueHash;
        }
    }

}
