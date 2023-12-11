package org.column4j.index;

import org.column4j.index.v3.ColSearchResult;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface ChunkIndex extends ColumnIndex {


    void insertChunkRecord(int colId, int chunkId, long offset, int capacity);
}
