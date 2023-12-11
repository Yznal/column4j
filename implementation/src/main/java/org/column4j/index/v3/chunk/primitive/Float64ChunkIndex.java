package org.column4j.index.v3.chunk.primitive;

import org.column4j.index.v3.chunk.ChunkIndex;

public interface Float64ChunkIndex extends ChunkIndex {

    /**
     * checks if chunk contains value
     * @param value searched value
     * @return check result
     */
    boolean contains(double value);

    /**
     * Indexes value at given offset
     * @param offset global value offset
     * @param value value
     */
    void insertRecord(int offset, double value);


    /**
     * looks up indices of values in chunk
     * @param value searched value
     * @return indices  value
     */
    int[] lookupValue(double value);
}
