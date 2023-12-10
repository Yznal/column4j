package org.column4j.index;

import org.column4j.index.v3.ColSearchResult;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface ChunkIndex {

    /**
     * Looks up index by dimensions query and provided bounds
     * @param lower lower offset search bound, inclusive
     * @param upper upper offset search vound, inclusive
     * @return ids matching query conditions or empty collection
     */
    ColSearchResult lookup(int colId, long lower, long upper);


    void insertChunkRecord(int colId, int chunkId, long offset, int capacity);
}
