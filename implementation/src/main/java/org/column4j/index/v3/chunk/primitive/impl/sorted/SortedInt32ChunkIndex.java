package org.column4j.index.v3.chunk.primitive.impl.sorted;

import org.column4j.index.v3.chunk.primitive.Int32ChunkIndex;

import java.util.Arrays;
import java.util.Comparator;

public class SortedInt32ChunkIndex extends SortedChunkIndex implements Int32ChunkIndex {

    private final int[] data;


    public static SortedInt32ChunkIndex fromChunk(int[] data) {
        return fromChunk(data,  DEFAULT_SEGMENT_SIZE);
    }

    public static SortedInt32ChunkIndex fromChunk(int[] data, int segmentSize) {
        record Pair(int data, int offset) {}
        Pair[] sorting = new Pair[data.length];
        for (int i = 0; i < data.length; i++) {
            sorting[i] = new Pair(data[i], i);
        }
        Arrays.sort(sorting, Comparator.comparingInt(a -> a.data));

        /*
        Theoretically we don't even need to store sorted data[] copy.
        We can store offsets sorted by data and have reference to it
         */
        int[] sortedData = new int[data.length];
        int[] offsets = new int[data.length];
        int segmentIdx = 0;
        int segmentCount = data.length % segmentSize == 0
                ? data.length / segmentSize
                : data.length / segmentSize + 1;
        int[] segments = new int[segmentCount];
        for (int i = 0; i < data.length; i++) {
            sortedData[i] = sorting[i].data;
            offsets[i] = sorting[i].offset;
            if (i % segmentSize == 0) {
                segments[segmentIdx++] = sortedData[i];
            }
        }
        return new SortedInt32ChunkIndex(segmentSize, segments, sortedData, offsets);
    }

    protected SortedInt32ChunkIndex(int segmentSize, int[] segments, int[] data, int[] offsets) {
        super(offsets, segmentSize, segments);
        this.data = data;
    }

    @Override
    public boolean contains(int value) {
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


    @Override
    public int[] lookupValues(int value) {
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

    private int segmentSearch(int value) {
        return leftBinarySearch(segments, value);
    }

    private int probeSegmentsRight(int segment, int value) {
        if (segment == segments.length - 1 || segments[segment + 1] != value) {
            return -1;
        }
        while (segment < segments.length - 1 && segments[segment + 1] == value) {
            segment++;
        }
        return segment;
    }

    private int searchInSegment(int segNum, int value) {
        int start = segNum * segmentSize;
        int end = Math.min(start + segmentSize, data.length);
        return leftBinarySearch(data, start, end, value);
    }

    private int searchRight(int idx, int value) {
        while (idx < data.length - 1 && data[idx + 1] == value) {
            idx++;
        }
        return idx;
    }

    private int searchLeft(int idx, int value) {
        while (idx > 1 && data[idx - 1] == value) {
            idx++;
        }
        return idx;
    }


    private int leftBinarySearch(int[] arr,  int value) {
        return leftBinarySearch(arr, 0, arr.length, value);
    }

    // we need to find leftmost index instead of random
    private int leftBinarySearch(int[] arr, int begin, int end, int value) {
        if (arr[0] == value) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (arr[mid] < value) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || arr[r] != value) {
            return -r;
        }
        return r;
    }

}
