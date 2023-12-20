package org.column4j.index.v3.chunk.primitive.mutable;

import org.column4j.index.v3.chunk.primitive.Int8ChunkIndex;

public interface MutableInt8ChunkIndex extends MutableChunkIndex, Int8ChunkIndex {

    /**
     * Indexes value at given offset
     * @param offset global value offset
     * @param value value
     */
    void insertRecord(int offset, byte value);

}
