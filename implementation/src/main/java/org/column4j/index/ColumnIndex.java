package org.column4j.index;

import org.column4j.index.v3.ColSearchResult;

public interface ColumnIndex { // local

    /**
     * Looks up index by dimensions query and provided bounds
     * @param lower lower offset search bound, inclusive
     * @param upper upper offset search bound, inclusive
     * @return ids matching query conditions or empty collection
     */
    ColSearchResult lookup(int lower, int upper);

}
