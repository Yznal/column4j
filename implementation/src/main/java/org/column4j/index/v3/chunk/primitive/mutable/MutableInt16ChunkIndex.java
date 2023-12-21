package org.column4j.index.v3.chunk.primitive.mutable;

import org.column4j.index.v3.chunk.primitive.Int16ChunkIndex;

public interface MutableInt16ChunkIndex extends MutableChunkIndex, Int16ChunkIndex {

    /**
     * Indexes value at given offset
     * @param offset global value offset
     * @param value value
     */
    void insertRecord(int offset, short value);

}