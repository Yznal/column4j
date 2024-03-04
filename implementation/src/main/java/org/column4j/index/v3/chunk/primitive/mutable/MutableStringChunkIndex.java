package org.column4j.index.v3.chunk.primitive.mutable;

import org.column4j.index.v3.chunk.primitive.StringChunkIndex;

import javax.annotation.Nonnull;

public interface MutableStringChunkIndex extends MutableChunkIndex, StringChunkIndex {

    /**
     * Indexes value at given offset
     * @param offset global value offset
     * @param value value
     */
    void insertRecord(int offset,@Nonnull String value);
}
