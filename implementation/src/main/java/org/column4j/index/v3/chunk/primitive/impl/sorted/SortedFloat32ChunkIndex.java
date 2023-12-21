package org.column4j.index.v3.chunk.primitive.impl.sorted;

import org.column4j.index.v3.chunk.BinarySearch;
import org.column4j.index.v3.chunk.primitive.Float32ChunkIndex;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;

public class SortedFloat32ChunkIndex extends SortedChunkIndex implements Float32ChunkIndex {

    private final float[] dataRef;

    private final float[] segments;


    public static SortedFloat32ChunkIndex fromChunk(float[] data) {
        return fromChunk(data,  DEFAULT_SEGMENT_SIZE);
    }

    public static SortedFloat32ChunkIndex fromChunk(float[] data, int segmentSize) {
        record Pair(float data, int offset) {}
        Pair[] sorting = new Pair[data.length];
        for (int i = 0; i < data.length; i++) {
            sorting[i] = new Pair(data[i], i);
        }
        Arrays.sort(sorting, Comparator.comparingDouble(a -> a.data));

        int[] offsets = new int[data.length];
        int segmentIdx = 0;
        int segmentCount = data.length % segmentSize == 0
                ? data.length / segmentSize
                : data.length / segmentSize + 1;
        float[] segments = new float[segmentCount];
        for (int i = 0; i < data.length; i++) {;
            offsets[i] = sorting[i].offset;
            if (i % segmentSize == 0) {
                segments[segmentIdx++] = data[offsets[i]];
            }
        }
        return new SortedFloat32ChunkIndex(segmentSize, segments, data, offsets);
    }

    protected SortedFloat32ChunkIndex(int segmentSize, float[] segments, float[] dataRef, int[] offsets) {
        super(offsets, segmentSize);
        this.dataRef = dataRef;
        this.segments = segments;
    }

    @Override
    public boolean contains(float value) {
        if (value < segments[0]) {
            return false;
        }
        int segment = segmentSearch(value);
        if (segment >= 0) {
            return true;
        }
        int segmentResult = searchInSegment(~segment, value);
        return segmentResult >= 0;
    }


    @Nullable
    @Override
    public int[] lookupValues(float value) {
        if (value < segments[0]) {
            return null;
        }

        int segment = segmentSearch(value);
        if (segment >= 0) { // hit segment border
            int left = searchLeft(segment * segmentSize, value);
            int right = searchRight(segment * segmentSize, value);
            return loadRes(left, right);
        }

        segment = ~segment;
        if (segment >= segments.length) {
            return null;
        }
        int probe = probeSegmentsRight(segment, value);
        int start = searchInSegment(segment, value);
        if (start < 0 && probe < 0) {
            return null;
        }

        int end = probe < 0 ? searchRight(start, value) : searchRight(probe, value);
        return loadRes(start, end);
    }

    int[] loadRes(int leftInc, int rightInc) {
        int[] res = new int[rightInc - leftInc + 1];
        System.arraycopy(localOffsets, leftInc, res, 0, res.length);
        return res;
    }

    private int segmentSearch(float value) {
        return BinarySearch.leftSearch(segments, 0, segments.length, value);
    }

    private int probeSegmentsRight(int segment, float value) {
        if (segment == segments.length - 1 || segments[segment + 1] != value) {
            return -1;
        }
        while (segment < segments.length - 1 && segments[segment + 1] == value) {
            segment++;
        }
        return segment;
    }

    private int searchInSegment(int segNum, float value) {
        int start = segNum * segmentSize;
        int end = Math.min(start + segmentSize, dataRef.length);
        return BinarySearch.leftIndirectSearch(localOffsets, dataRef, start, end, value);
    }

    private int searchRight(int idx, float value) {
        while (idx < localOffsets.length - 1 && dataRef[localOffsets[idx + 1]] == value) {
            idx++;
        }
        return idx;
    }

    private int searchLeft(int idx, float value) {
        while (idx > 1 && dataRef[localOffsets[idx - 1]] == value) {
            idx++;
        }
        return idx;
    }

}
