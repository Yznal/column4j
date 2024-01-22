package org.column4j.column.impl.chunk.mutable;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.column4j.column.chunk.mutable.primitive.Int32MutableColumnChunk;
import org.column4j.column.impl.chunk.mutable.primitive.Int32MutableColumnChunkImpl;
import org.column4j.utils.Int32VectorUtils;

import java.util.Objects;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class StringStorage {
    private static final int TOMBSTONE_DEDUPLICATED = -1;

    private final BiMap<String, Integer> deduplicated;
    private final Int32MutableColumnChunk dataChunk;
    private final String tombstone;

    public StringStorage(int size, String tombstone) {
        this.tombstone = tombstone;
        this.deduplicated = HashBiMap.create();
        this.dataChunk = new Int32MutableColumnChunkImpl(size, TOMBSTONE_DEDUPLICATED);
    }

    public void write(int position, String value) {
        if (Objects.equals(tombstone, value)) {
            dataChunk.write(position, TOMBSTONE_DEDUPLICATED);
        } else {
            var index = deduplicated.computeIfAbsent(value, k -> deduplicated.size() + 1);
            dataChunk.write(position, index);
        }
    }

    public String get(int position) {
        var index = dataChunk.get(position);
        if (index == TOMBSTONE_DEDUPLICATED) {
            return tombstone;
        } else {
            return deduplicated.inverse().get(index);
        }
    }

    public String[] getData() {
        var indexToValue = deduplicated.inverse();
        var indexes = dataChunk.getData();
        var data = new String[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            if (indexes[i] == TOMBSTONE_DEDUPLICATED) {
                data[i] = tombstone;
            } else {
                data[i] = indexToValue.get(indexes[i]);
            }
        }
        return data;
    }

    public int indexOfNotTombstone(int firstIndex, int lastIndex) {
        return Int32VectorUtils.indexOfAnother(dataChunk.getData(), TOMBSTONE_DEDUPLICATED, firstIndex, lastIndex);
    }

    public int lastIndexOfNotTombstone(int firstIndex, int lastIndex) {
        return Int32VectorUtils.lastIndexOfAnother(dataChunk.getData(), TOMBSTONE_DEDUPLICATED, firstIndex, lastIndex);
    }
}
