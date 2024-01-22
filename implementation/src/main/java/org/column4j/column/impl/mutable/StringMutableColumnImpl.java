package org.column4j.column.impl.mutable;

import org.column4j.column.ColumnType;
import org.column4j.column.chunk.ColumnChunk;
import org.column4j.column.chunk.mutable.StringMutableColumnChunk;
import org.column4j.column.impl.chunk.mutable.StringMutableColumnChunkImpl;
import org.column4j.column.mutable.StringMutableColumn;
import org.column4j.column.statistic.StringStatistic;
import org.column4j.column.statistic.Statistic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class StringMutableColumnImpl implements StringMutableColumn {
    private final int maxChunkSize;
    private final String tombstone;
    private final List<StringMutableColumnChunk> chunks;

    public StringMutableColumnImpl(int maxChunkSize, String tombstone) {
        this.maxChunkSize = maxChunkSize;
        this.tombstone = tombstone;
        this.chunks = new ArrayList<>();
        this.chunks.add(new StringMutableColumnChunkImpl(maxChunkSize, tombstone));
    }

    @Override
    public int size() {
        return chunks.stream()
                .map(ColumnChunk::getStatistic)
                .mapToInt(Statistic::getCount)
                .sum();
    }

    @Override
    public ColumnType type() {
        return ColumnType.STRING;
    }

    @Override
    public List<ColumnChunk<String[], StringStatistic>> getChunks() {
        return Collections.unmodifiableList(chunks);
    }

    @Override
    public void tombstone(int position) {
        write(position, tombstone);
    }

    @Override
    public StringMutableColumnChunk getChunk(int index) {
        return chunks.size() > index ? chunks.get(index) : null;
    }

    @Override
    public void write(int position, String value) {
        var chunkIndex = position / maxChunkSize;
        var chunkPosition = position % maxChunkSize;

        while (chunkIndex >= chunks.size()) {
            chunks.add(new StringMutableColumnChunkImpl(maxChunkSize, tombstone));
        }
        var chunk = chunks.get(chunkIndex);
        chunk.write(chunkPosition, value);
    }

    @Override
    public String get(int position) {
        var chunkIndex = position / maxChunkSize;
        var chunkPosition = position % maxChunkSize;

        if (chunkIndex >= chunks.size()) {
            return tombstone;
        }
        var chunk = chunks.get(chunkIndex);
        return chunk.get(chunkPosition);
    }

    @Override
    public String[] getByIndexes(int[] indexes) {
        var buffer = new String[indexes.length];
        readByIndexes(indexes, buffer);
        return buffer;
    }

    @Override
    public String[] getByIndexes(int from, int to) {
        var buffer = new String[to - from + 1];
        readByIndexes(from, to, buffer);
        return buffer;
    }

    @Override
    public void readByIndexes(int[] indexes, String[] buffer) {
        if (indexes.length > buffer.length) {
            throw new IllegalArgumentException("indexes length should be less or equals to buffer length");
        }
        for (int i = 0; i < indexes.length; i++) {
            buffer[i] = get(indexes[i]);
        }
    }

    @Override
    public void readByIndexes(int from, int to, String[] buffer) {
        var size = to - from + 1;
        if (size > buffer.length) {
            throw new IllegalArgumentException("Bound length should be less or equals to buffer length");
        }
        if (from > to) {
            throw new IllegalArgumentException("'from' should be less or equals to 'to'");
        }
        for (int i = 0; from <= to; from++, i++) {
            buffer[i] = get(from);
        }
    }
}
