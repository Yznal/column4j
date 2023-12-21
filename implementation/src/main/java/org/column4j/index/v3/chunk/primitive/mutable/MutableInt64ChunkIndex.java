package org.column4j.index.v3.chunk.primitive.mutable;

import org.column4j.index.v3.chunk.primitive.Int64ChunkIndex;

public interface MutableInt64ChunkIndex extends MutableChunkIndex, Int64ChunkIndex {

    /**
     * Indexes value at given offset
     * @param offset global value offset
     * @param value value
     */
    void insertRecord(int offset, long value);

}

