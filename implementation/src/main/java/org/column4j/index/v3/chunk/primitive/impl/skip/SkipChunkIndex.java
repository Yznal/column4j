package org.column4j.index.v3.chunk.primitive.impl.skip;

public abstract class SkipChunkIndex {

    protected static final int DEFAULT_SEGMENT_SIZE = 1024;

    protected final int segmentSize;

    protected SkipChunkIndex(int segmentSize) {
        this.segmentSize = segmentSize;
    }
}
