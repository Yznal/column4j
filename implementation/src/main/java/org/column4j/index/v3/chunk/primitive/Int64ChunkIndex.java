package org.column4j.index.v3.chunk.primitive;

import org.column4j.index.ChunkIndex;

public interface Int64ChunkIndex extends ChunkIndex {

    /**
     * checks if chunk contains value
     * @param value searched value
     * @return check result
     */
    boolean contains(long value);

    /**
     * Indexes value at given offset
     * @param offset global value offset
     * @param value value
     */
    void insertRecord(int offset, long value);


    /**
     * looks up indices of values in chunk
     * @param value searched value
     * @return indices of segments that contain value
     */
    int[] lookupValues(long value);
}
