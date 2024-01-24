package org.column4j.index.v3.chunk.primitive.impl.skip;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.column4j.index.v3.chunk.primitive.mutable.MutableInt32ChunkIndex;

import javax.annotation.Nullable;

public class SkipInt32ChunkVIndex extends SkipChunkIndex implements MutableInt32ChunkIndex {

    private static VectorSpecies<Integer> iSpecies = IntVector.SPECIES_PREFERRED;

    private final int[] dataRef;

    private final int[] segmentsMin;
    private final int[] segmentsMax;

    public SkipInt32ChunkVIndex(int[] data, int segmentSize) {
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
                min = Integer.MAX_VALUE;
                max = Integer.MIN_VALUE;
                segmentIdx--;
            }
        }
    }

    public SkipInt32ChunkVIndex(int[] data) {
        this(data, DEFAULT_SEGMENT_SIZE);
    }

    @Override
    public boolean contains(int value) {
        for (int s = 0; s < segmentsMin.length; s++) {

            if (value >= segmentsMin[s] && value <= segmentsMax[s]) {
                if (value == segmentsMin[s] || value == segmentsMax[s]){
                    return true;
                }
                IntVector sample = IntVector.broadcast(iSpecies, value);
                int offset = 0;
                int start = s*segmentSize;
                int limit = Math.min((s + 1)*segmentSize, dataRef.length);
                for (; offset < iSpecies.loopBound(limit - start); offset += iSpecies.length() ) {
                    IntVector vec = IntVector.fromArray(iSpecies, dataRef, start + offset);
                    if (vec.compare(VectorOperators.EQ, sample).anyTrue()) {
                        return true;
                    }
                }
                // remainder
                for (; offset < limit - start; offset++) {
                    if (dataRef[start + offset] == value) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Nullable
    @Override
    public int[] lookupValues(int value) {
        IntArrayList res = new IntArrayList();
        int size = 0;
        for (int s = 0; s < segmentsMin.length; s++) {
            IntVector sample = IntVector.broadcast(iSpecies, value);
            IntVector indices = IntVector.fromArray(iSpecies, createIndicesArray(), 0);
            int offset = 0;
            int start = s*segmentSize;
            int limit = Math.min((s + 1)*segmentSize, dataRef.length);
            int[] iterResult = new int[iSpecies.length()];
            for (; offset < iSpecies.loopBound(limit - start); offset += iSpecies.length() ) {
                IntVector vec = IntVector.fromArray(iSpecies, dataRef, start + offset);
                VectorMask<Integer> mask = vec.compare(VectorOperators.EQ, sample);
                indices.add(IntVector.broadcast(iSpecies, start + offset))
                        .compress(mask)
                        .intoArray(iterResult, 0);
                int trueCount = mask.trueCount();
                res.addElements(size, iterResult, 0, trueCount);
                size += trueCount;
            }
            // remainder
            for (; offset < limit - start; offset++) {
                if (dataRef[start + offset] == value) {
                    res.add(start + offset);
                }
            }
        }
        return res.isEmpty() ? null : res.toArray(new int[0]);
    }

    @Override
    public void insertRecord(int offset, int value) {
        int segment = offset / segmentSize;
        if (value > segmentsMax[segment]) {
            segmentsMax[segment] = value;
        }
        if (value < segmentsMin[segment]) {
            segmentsMin[segment] = value;
        }
    }

    private int[] createIndicesArray() {
        int[] arr = new int[iSpecies.length()];
        for (int i = 0; i < iSpecies.length(); i++) {
            arr[i] = i;
        }
        return arr;
    }
}
