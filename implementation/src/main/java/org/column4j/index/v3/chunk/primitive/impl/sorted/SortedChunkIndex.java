package org.column4j.index.v3.chunk.primitive.impl.sorted;

/**
 * Index on final chunk based on sorting of values.
 * Values are sorted into data array.
 * Preliminary search is performed on segments array - data probes on fixed interval.
 * Primary search is performed on
 */
public abstract class SortedChunkIndex {

    protected static final int DEFAULT_SEGMENT_SIZE = 1024;


    protected final int[] localOffsets;
    protected final int[] segments;

    protected final int segmentSize;

    /**
     *
     * @param chunkOffset global cursor offset where chunk starts, corresponds to data[0]
     * @param localOffsets offsets of sorted stored values
     */
    protected SortedChunkIndex(int[] localOffsets, int segmentSize, int[] segments) {
        this.localOffsets = localOffsets;
        this.segmentSize = segmentSize;
        this.segments = segments;
    }

}
