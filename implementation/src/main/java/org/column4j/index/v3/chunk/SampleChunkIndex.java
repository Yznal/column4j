package org.column4j.index.v3.chunk;

import org.column4j.index.ChunkIndex;
import org.column4j.index.v3.ColSearchResult;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

public class SampleChunkIndex implements ChunkIndex {

    private final MutableIntObjectMap<ColumnMeta> metas = new IntObjectHashMap<>();


    // todo убрать col id
    @Override
    public ColSearchResult lookup(int colId, long lower, long upper) {
        ColSearchResult result = null;
        var colMeta = metas.get(colId);
        int[] range = colMeta.searchInterval(lower, upper);
        if (range != null) {
            result = new ColSearchResult(
                    colMeta.columnId,
                    colMeta.getData()[range[0]],
                    colMeta.getData()[range[1]]
            );
        }

        return result;
    }

    @Override
    public void insertChunkRecord(int colId, int chunkId, long offset, int capacity) {
        ColumnMeta meta = metas.get(colId);
        if (meta == null) {
            meta = new ColumnMeta(colId);
            metas.put(colId, meta);
        }
        meta.addChunk(chunkId, offset, capacity);
    }
}
