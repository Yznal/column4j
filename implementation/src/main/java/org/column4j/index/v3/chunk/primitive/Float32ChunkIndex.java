package org.column4j.index.v3.chunk.primitive;

public interface Float32ChunkIndex {

    /**
     * checks if chunk contains value
     * @param value searched value
     * @return check result
     */
    boolean contains(float value);

    /**
     * Indexes value at given offset
     * @param offset global value offset
     * @param value value
     */
    void insertRecord(int offset, float value);


    /**
     * looks up indices of values in chunk
     * @param value searched value
     * @return indices of value
     */
    int[] lookupValue(float value);
}
