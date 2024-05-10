package org.column4j.column.impl.mutable.primitive;

import org.column4j.column.ColumnType;
import org.column4j.column.chunk.ColumnChunk;
import org.column4j.column.chunk.mutable.primitive.Int16MutableColumnChunk;
import org.column4j.column.impl.chunk.mutable.primitive.Int16MutableColumnChunkImpl;
import org.column4j.column.mutable.primitive.Int16MutableColumn;
import org.column4j.column.statistic.Int16Statistic;
import org.column4j.column.statistic.Statistic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Int16MutableColumnImpl implements Int16MutableColumn {
    private final int maxChunkSize;
    private final short tombstone;
    private final List<Int16MutableColumnChunk> chunks;

    public Int16MutableColumnImpl(int maxChunkSize, short tombstone) {
        this.maxChunkSize = maxChunkSize;
        this.tombstone = tombstone;
        this.chunks = new ArrayList<>();
        this.chunks.add(new Int16MutableColumnChunkImpl(maxChunkSize, tombstone));
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
        return ColumnType.INT16;
    }

    @Override
    public List<ColumnChunk<short[], Int16Statistic>> getChunks() {
        return Collections.unmodifiableList(chunks);
    }

    @Override
    public void tombstone(int position) {
        write(position, tombstone);
    }

    @Override
    public Int16MutableColumnChunk getChunk(int index) {
        return chunks.size() > index ? chunks.get(index) : null;
    }

    @Override
    public void write(int position, short value) {
        var chunkIndex = position / maxChunkSize;
        var chunkPosition = position % maxChunkSize;

        while (chunkIndex >= chunks.size()) {
            chunks.add(new Int16MutableColumnChunkImpl(maxChunkSize, tombstone));
        }
        var chunk = chunks.get(chunkIndex);
        chunk.write(chunkPosition, value);
    }

    @Override
    public short get(int position) {
        var chunkIndex = position / maxChunkSize;
        var chunkPosition = position % maxChunkSize;

        if (chunkIndex >= chunks.size()) {
            return tombstone;
        }
        var chunk = chunks.get(chunkIndex);
        return chunk.get(chunkPosition);
    }

    @Override
    public short[] getByIndexes(int[] indexes) {
        var buffer = new short[indexes.length];
        readByIndexes(indexes, buffer);
        return buffer;
    }

    @Override
    public short[] getByIndexes(int from, int to) {
        var buffer = new short[to - from + 1];
        readByIndexes(from, to, buffer);
        return buffer;
    }

    @Override
    public void readByIndexes(int[] indexes, short[] buffer) {
        if (indexes.length > buffer.length) {
            throw new IllegalArgumentException("indexes length should be less or equals to buffer length");
        }
        for (int i = 0; i < indexes.length; i++) {
            buffer[i] = get(indexes[i]);
        }
    }

    @Override
    public void readByIndexes(int from, int to, short[] buffer) {
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

    @Override
    public final short getTombstone() {
        return tombstone;
    }

    @Override
    public final int countChunks() {
        return chunks.size();
    }

    @Override
    public final int chunkSize() {
        return maxChunkSize;
    }
}
