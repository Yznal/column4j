package org.column4j.index.v1;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Column of inverted index
 */
public interface IndexColumn {

    /**
     * Performs lookup of ids associated with dimension value
     * @param dimensionValue search key
     * @return ids associated with value
     */
    @Nonnull
    Collection<Integer> lookupDimension(@Nonnull CharSequence dimensionValue);

    /**
     * Adds association of given id with dimension value
     * @param dmensionValue association key
     * @param id association value
     */
    void insertValue(@Nonnull CharSequence dmensionValue, int id);
}
