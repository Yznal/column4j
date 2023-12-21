package org.column4j.index.v3.chunk.primitive.mutable;

import org.column4j.index.v3.chunk.primitive.Int32ChunkIndex;

public interface MutableInt32ChunkIndex extends MutableChunkIndex, Int32ChunkIndex {

    /**
     * Indexes value at given offset
     * @param offset global value offset
     * @param value value
     */
    void insertRecord(int offset, int value);

}
