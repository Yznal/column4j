package org.column4j.index.temporal;

import org.column4j.index.Dimension;

import javax.annotation.Nonnull;
import java.util.Collection;


public interface InvertedIndex {


    /**
     * Looks up ids by dimensions query
     * @param query collection of key-value query {key1=value1, key2=value2, ..., keyN=valueN}
     * @return ids matching query conditions or empty collection
     */
    @Nonnull
    int[] lookup(@Nonnull Collection<Dimension> query);

    /**
     * Looks up index by dimensions query and provided bounds
     * @param query collection of key-value query {key1=value1, key2=value2, ..., keyN=valueN}
     * @param lower lower offset search bound, inclusive
     * @param upper upper offset search vound, inclusive
     * @return ids matching query conditions or empty collection
     */
    ColSearchResult[] lookup(@Nonnull Collection<Dimension> query, long lower, long upper);

    /**
     * Inserts new index record for id
     * @param dimensions id dimensions
     * @param colId stored column pointer
     */
    void insertColumnRecord(@Nonnull Collection<Dimension> dimensions, int colId);

    void insertChunkRecord(int colId, int chunkId, long offset, int capacity);

}
