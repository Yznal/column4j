package org.column4j.index;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Index for lookup of multidimensional values
 */
public interface InvertedIndex {

    /**
     * Looks up indices by dimensions query
     * @param query collection of key-value query {key1=value1, key2=value2, ..., keyN=valueN}
     * @return ids matching query conditions or empty collection
     */
    @Nonnull
    int[] lookup(@Nonnull Collection<Dimension> query);

    /**
     * Inserts new index record for id
     * @param dimensions id dimensions
     * @param value stored value
     */
    void insertValue(@Nonnull Collection<Dimension> dimensions, int value);
}
