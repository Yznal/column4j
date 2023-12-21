package org.column4j.index.v3.column;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;


public class ColumnChunksIndex {

    private static final int DEFAULT_CAPACITY = 10;


    private int[] chunkOffsets;
    private final int chunkCapacity;
    private int[] chunkIds;
    private int size = 0;
    private int linearThreshold = 20;

    public ColumnChunksIndex(int chunkSize) {
        this(chunkSize, DEFAULT_CAPACITY);
    }

    public ColumnChunksIndex(int chunkSize, int initialCapacity) {
        chunkOffsets = new int[initialCapacity];
        chunkIds = new int[initialCapacity];
        chunkCapacity = chunkSize;
    }


    public boolean addChunk(int id, int offset) {
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
    public int[] searchInterval(int startOffset, int endOffset) {
        if (size == 0) {
            return null;
        }
        if (endOffset < startOffset) {
            return null;
        }
        if (startOffset >= (chunkOffsets[size - 1] + chunkCapacity) || endOffset < chunkOffsets[0]) { // out of bounds
            return null;
        }
        if (size > linearThreshold) {
            return searchBinary(startOffset, endOffset);
        } else {
            return searchLinear(startOffset, endOffset);
        }
    }

    @Nonnull
    private int[] searchLinear(int startOffset, int endOffset) {
        int i = 0;
        int start, end;
        // find first block for start
        while (chunkOffsets[i] + chunkCapacity < startOffset) {
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
    private int[] searchBinary(int startOffset, int endOffset) {
        int i = 0;
        int start, end;
        start = Arrays.binarySearch(chunkOffsets, 0, size, startOffset);
        if (start < 0) {
            start = ~start;
            if (start > 0
                    && chunkOffsets[start - 1] <= startOffset  // we fell into prev chunk
                    && startOffset <= (chunkOffsets[start - 1] + chunkCapacity)) {
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
            int[] cpyOffsets = new int[newSize];
            int[] cpyChunks = new int[newSize];
            System.arraycopy(chunkOffsets, 0, cpyOffsets, 0, chunkOffsets.length);
            System.arraycopy(chunkIds, 0, cpyChunks, 0, chunkIds.length);
            chunkIds = cpyChunks;
            chunkOffsets = cpyOffsets;
        }
    }

    private void shiftRight(int idx) {
        for (int i = size; i > idx; i--) {
            chunkOffsets[i] = chunkOffsets[i-1];
            chunkIds[i] = chunkIds[i-1];
        }
    }


}
