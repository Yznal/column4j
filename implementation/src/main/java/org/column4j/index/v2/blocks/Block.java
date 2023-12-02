package org.column4j.index.v2.blocks;

import javax.annotation.Nullable;

/**
 * A block of bitset data
 * @param <ValType> bitset value type block lookups
 */
public interface Block<ValType> {

    /**
     * Looks up data stored in block by key
     * @param idx key index
     * @return resulting {@link ValType} or null if key is absent
     */
    @Nullable
    ValType lookup(int idx);

    int size();

    int minKey();

    int maxKey();
}
