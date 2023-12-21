package org.column4j.index.v3.chunk.primitive.impl.sorted;

import org.column4j.index.v3.chunk.BinarySearch;
import org.column4j.index.v3.chunk.primitive.Int16ChunkIndex;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;

public class SortedInt16ChunkIndex extends SortedChunkIndex implements Int16ChunkIndex {

    private final short[] dataRef;

    private final short[] segments;


    public static SortedInt16ChunkIndex fromChunk(short[] data) {
        return fromChunk(data,  DEFAULT_SEGMENT_SIZE);
    }

    public static SortedInt16ChunkIndex fromChunk(short[] data, int segmentSize) {
        record Pair(short data, int offset) {}
        Pair[] sorting = new Pair[data.length];
        for (int i = 0; i < data.length; i++) {
            sorting[i] = new Pair(data[i], i);
        }
        Arrays.sort(sorting, Comparator.comparingInt(a -> a.data));

        int[] offsets = new int[data.length];
        int segmentIdx = 0;
        int segmentCount = data.length % segmentSize == 0
                ? data.length / segmentSize
                : data.length / segmentSize + 1;
        short[] segments = new short[segmentCount];
        for (int i = 0; i < data.length; i++) {;
            offsets[i] = sorting[i].offset;
            if (i % segmentSize == 0) {
                segments[segmentIdx++] = data[offsets[i]];
            }
        }
        return new SortedInt16ChunkIndex(segmentSize, segments, data, offsets);
    }

    protected SortedInt16ChunkIndex(int segmentSize, short[] segments, short[] dataRef, int[] offsets) {
        super(offsets, segmentSize);
        this.dataRef = dataRef;
        this.segments = segments;
    }

    @Override
    public boolean contains(short value) {
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
    public int[] lookupValues(short value) {
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

    private int segmentSearch(short value) {
        return BinarySearch.leftSearch(segments, 0, segments.length, value);
    }

    private int probeSegmentsRight(int segment, short value) {
        if (segment == segments.length - 1 || segments[segment + 1] != value) {
            return -1;
        }
        while (segment < segments.length - 1 && segments[segment + 1] == value) {
            segment++;
        }
        return segment;
    }

    private int searchInSegment(int segNum, short value) {
        int start = segNum * segmentSize;
        int end = Math.min(start + segmentSize, dataRef.length);
        return BinarySearch.leftIndirectSearch(localOffsets, dataRef, start, end, value);
    }

    private int searchRight(int idx, short value) {
        while (idx < localOffsets.length - 1 && dataRef[localOffsets[idx + 1]] == value) {
            idx++;
        }
        return idx;
    }

    private int searchLeft(int idx, short value) {
        while (idx > 1 && dataRef[localOffsets[idx - 1]] == value) {
            idx++;
        }
        return idx;
    }


}
