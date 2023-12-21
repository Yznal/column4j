package org.column4j.index.v3.chunk.primitive.mutable;

import org.column4j.index.v3.chunk.primitive.Float32ChunkIndex;

public interface MutableFloat32ChunkIndex extends MutableChunkIndex, Float32ChunkIndex {

    /**
     * Indexes value at given offset
     * @param offset global value offset
     * @param value value
     */
    void insertRecord(int offset, float value);
}
