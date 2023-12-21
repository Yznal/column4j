package org.column4j.index.v3.chunk.primitive.mutable;

import org.column4j.index.v3.chunk.primitive.Float64ChunkIndex;

public interface MutableFloat64ChunkIndex extends MutableChunkIndex, Float64ChunkIndex {

    /**
     * Indexes value at given offset
     * @param offset global value offset
     * @param value value
     */
    void insertRecord(int offset, double value);

}
