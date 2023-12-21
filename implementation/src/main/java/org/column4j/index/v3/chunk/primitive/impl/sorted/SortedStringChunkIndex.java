package org.column4j.index.v3.chunk.primitive.impl.sorted;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.column4j.index.v3.chunk.primitive.StringChunkIndex;
import org.column4j.utils.BinarySearch;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

public class SortedStringChunkIndex extends SortedChunkIndex implements StringChunkIndex {

    private static final int NULL_HASH = Integer.MIN_VALUE;

    private final Object2IntMap<String> dictEncoding;

    private final String[] dataRef;

    private final int[] segments;


    public static SortedStringChunkIndex fromChunk(String[] data) {
        return fromChunk(data,  DEFAULT_SEGMENT_SIZE);
    }

    public static SortedStringChunkIndex fromChunk(String[] data, int segmentSize) {
        Object2IntMap<String> dictEncoding = new Object2IntOpenHashMap<>();
        int code = 0;
        record Pair(int data, int offset) {}
        Pair[] sorting = new Pair[data.length];
        for (int i = 0; i < data.length; i++) {
            int valCode = dictEncoding.getOrDefault(data[i], -1);
            if (valCode == -1) {
                valCode = code++;
                dictEncoding.put(data[i], valCode);
            }
            sorting[i] = new Pair(valCode, i);
        }
        Arrays.sort(sorting, Comparator.comparingInt(a -> a.data));

        int[] offsets = new int[data.length];
        int segmentIdx = 0;
        int segmentCount = data.length % segmentSize == 0
                ? data.length / segmentSize
                : data.length / segmentSize + 1;
        int[] segments = new int[segmentCount];
        for (int i = 0; i < data.length; i++) {;
            offsets[i] = sorting[i].offset;
            if (i % segmentSize == 0) {
                segments[segmentIdx++] = safeHash(data[offsets[i]]);
            }
        }
        return new SortedStringChunkIndex(segmentSize, segments, data, dictEncoding, offsets);
    }

    protected SortedStringChunkIndex(int segmentSize, int[] segments, String[] dataRef,
                                     Object2IntMap<String> encoding, int[] offsets) {
        super(offsets, segmentSize);
        this.dataRef = dataRef;
        this.segments = segments;
        this.dictEncoding = encoding;
    }

    @Override
    public boolean contains(@Nonnull String value) {
        return dictEncoding.containsKey(value);
    }


    @Nullable
    @Override
    public int[] lookupValues(@Nonnull String value) {
        int valueCode = dictEncoding.getOrDefault(value, -1);
        if (valueCode == -1) {
            return null;
        }

        int segment = segmentSearch(valueCode);
        if (segment >= 0) { // hit segment border
            int left = searchLeft(segment * segmentSize, value);
            int right = searchRight(segment * segmentSize, value);
            return loadRes(left, right);
        }

        segment = ~segment;
        if (segment >= segments.length) {
            return null;
        }
        int probe = probeSegmentsRight(segment, valueCode);
        int start = searchInSegment(segment, valueCode);
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
        return BinarySearch.leftSearch(segments, 0, segments.length, value);
    }

    private int probeSegmentsRight(int segment, int valueCode) {
        if (segment == segments.length - 1 || segments[segment + 1] != valueCode) {
            return -1;
        }
        while (segment < segments.length - 1 && segments[segment + 1] == valueCode) {
            segment++;
        }
        return segment;
    }

    private int searchInSegment(int segNum, int valueCode) {
        int start = segNum * segmentSize;
        int end = Math.min(start + segmentSize, dataRef.length);
        return leftIndirectSearch(localOffsets, dataRef, start, end,valueCode);
    }

    private int searchRight(int idx, String value) {
        while (idx < localOffsets.length - 1 && Objects.equals(dataRef[localOffsets[idx - 1]], value)) {
            idx++;
        }
        return idx;
    }

    private int searchLeft(int idx, String value) {
        while (idx > 1 && Objects.equals(dataRef[localOffsets[idx - 1]], value)) {
            idx++;
        }
        return idx;
    }

    private int leftIndirectSearch(int[] pointers, String[] arr, int begin, int end, int valueCode) {
        if (dictEncoding.getInt(arr[pointers[begin]]) == valueCode) {
            return 0;
        }
        int l = begin, r = end;
        while (r - l > 1) {
            int mid = l + (r - l) / 2;
            if (dictEncoding.getInt(arr[pointers[mid]]) < valueCode) {
                l = mid;
            } else {
                r = mid;
            }
        }
        if (r >= arr.length || dictEncoding.getInt(arr[pointers[r]]) != valueCode) {
            return -r;
        }
        return r;
    }

    private static int safeHash(@Nullable String str) {
        return str == null ? NULL_HASH : str.hashCode();
    }
}
