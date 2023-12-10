package org.column4j.index.v3.chunk;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;

@NotThreadSafe
public class ColumnMeta {

    private static final int DEFAULT_CAPACITY = 10;
    public final int columnId;


    private long[] chunkOffsets;
    private int[] chunkCapacities;
    private int[] chunkIds;
    private int size = 0;
    private int linearThreshold = 100;

    public ColumnMeta(int columnId) {
        this(columnId, DEFAULT_CAPACITY);
    }

    public ColumnMeta(int columnId, int initialCapacity) {
        this.columnId = columnId;
        chunkOffsets = new long[initialCapacity];
        chunkIds = new int[initialCapacity];
        chunkCapacities = new int[initialCapacity];
    }


    public boolean addChunk(int id, long offset, int capacity) {
        ensureCapacity();
        int insertPoint = size;
        if (size > 0 && chunkOffsets[size - 1] > offset) { // broken offsets order
            int idx = Arrays.binarySearch(chunkIds, id + 1);
            if (idx >= 0) { // chunk already present
                return false;
            }
            shiftRight(~idx);
            insertPoint = ~idx;
        }
        chunkIds[insertPoint] = id;
        chunkOffsets[insertPoint] = offset;
        chunkCapacities[insertPoint] = capacity;
        size++;

        return true;
    }

    public void setLinearThreshold(int linearThreshold) {
        this.linearThreshold = linearThreshold;
    }

    public int[] getData() {
        return chunkIds;
    }

    /**
     * Searches chunks that may contain given offsets intervals
     * @param startOffset start offset of data, inclusive
     * @param endOffset end offset of data, inclusive
     * @return array of length 2 with start and end indices of chunkIds array or null in case of empty search result
     */
    @Nullable
    public int[] searchInterval(long startOffset, long endOffset) {
        if (size == 0) {
            return null;
        }
        if (endOffset < startOffset) {
            return null;
        }
        if (startOffset >= (chunkOffsets[size - 1] + chunkCapacities[size - 1]) || endOffset < chunkOffsets[0]) { // out of bounds
            return null;
        }
        if (size > linearThreshold) {
            return searchBinary(startOffset, endOffset);
        } else {
            return searchLinear(startOffset, endOffset);
        }
    }

    @Nonnull
    private int[] searchLinear(long startOffset, long endOffset) {
        int i = 0;
        int start, end;
        // find first block for start
        while (chunkOffsets[i] + chunkCapacities[i] < startOffset) {
            i++;
        }
        start = i;
        while (i < size && chunkOffsets[i] <= endOffset  ) {
            i++;
        }
        end = i;
        return new int[] {start, end};
    }

    @Nonnull
    private int[] searchBinary(long startOffset, long endOffset) {
        int i = 0;
        int start, end;
        start = Arrays.binarySearch(chunkOffsets, 0, size, startOffset);
        if (start < 0) {
            start = ~start;
            if (start > 0
                    && chunkOffsets[start - 1] <= startOffset  // we fell into prev chunk
                    && startOffset <= (chunkOffsets[start - 1] + chunkCapacities[start - 1])) {
                start--;
            }
        }
        end = Arrays.binarySearch(chunkOffsets, 0, size, endOffset);
        if (end < 0) {
            end = ~end;
        } else { // we found exact chunk begin, shift exclusive to next
            end++;
        }
        return new int[] {start, end};
    }

    public boolean empty() {
        return size == 0;
    }

    private void ensureCapacity() {
        if (size == chunkIds.length) {
            int newSize = chunkIds.length * 2;
            long[] cpyOffsets = new long[newSize];
            int[] cpyChunks = new int[newSize];
            int[] cpyCap = new int[newSize];
            System.arraycopy(chunkOffsets, 0, cpyOffsets, 0, chunkOffsets.length);
            System.arraycopy(chunkIds, 0, cpyChunks, 0, chunkIds.length);
            System.arraycopy(chunkCapacities, 0, cpyCap, 0, chunkCapacities.length);
            chunkIds = cpyChunks;
            chunkOffsets = cpyOffsets;
            chunkCapacities = cpyCap;
        }
    }

    private void shiftRight(int idx) {
        for (int i = size; i > idx; i--) {
            chunkOffsets[i] = chunkOffsets[i-1];
            chunkIds[i] = chunkIds[i-1];
            chunkCapacities[i] = chunkCapacities[i-1];
        }
    }


}
