package org.column4j.index.v3.chunk.primitive;

import javax.annotation.Nullable;

public interface Float32ChunkIndex {

    /**
     * checks if chunk contains value
     * @param value searched value
     * @return check result
     */
    boolean contains(float value);



    /**
     * looks up indices of values in chunk
     * @param value searched value
     * @return indices of value
     */
    @Nullable
    int[] lookupValues(float value);
}
