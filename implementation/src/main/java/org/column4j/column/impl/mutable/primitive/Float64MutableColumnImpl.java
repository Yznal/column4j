package org.column4j.column.impl.mutable.primitive;

import org.column4j.column.ColumnType;
import org.column4j.column.chunk.ColumnChunk;
import org.column4j.column.chunk.mutable.primitive.Float64MutableColumnChunk;
import org.column4j.column.impl.chunk.mutable.primitive.Float64MutableColumnChunkImpl;
import org.column4j.column.mutable.primitive.Float64MutableColumn;
import org.column4j.column.statistic.Float64Statistic;
import org.column4j.column.statistic.Statistic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Float64MutableColumnImpl implements Float64MutableColumn {
    private final int maxChunkSize;
    private final double tombstone;
    private final List<Float64MutableColumnChunk> chunks;

    public Float64MutableColumnImpl(int maxChunkSize, double tombstone) {
        this.maxChunkSize = maxChunkSize;
        this.tombstone = tombstone;
        this.chunks = new ArrayList<>();
        this.chunks.add(new Float64MutableColumnChunkImpl(maxChunkSize, tombstone));
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
        return ColumnType.FLOAT64;
    }

    @Override
    public List<ColumnChunk<double[], Float64Statistic>> getChunks() {
        return Collections.unmodifiableList(chunks);
    }

    @Override
    public void tombstone(int position) {
        write(position, tombstone);
    }

    @Override
    public Float64MutableColumnChunk getChunk(int index) {
        return chunks.size() > index ? chunks.get(index) : null;
    }

    @Override
    public void write(int position, double value) {
        var chunkIndex = position / maxChunkSize;
        var chunkPosition = position % maxChunkSize;

        while (chunkIndex >= chunks.size()) {
            chunks.add(new Float64MutableColumnChunkImpl(maxChunkSize, tombstone));
        }
        var chunk = chunks.get(chunkIndex);
        chunk.write(chunkPosition, value);
    }

    @Override
    public double get(int position) {
        var chunkIndex = position / maxChunkSize;
        var chunkPosition = position % maxChunkSize;

        if (chunkIndex >= chunks.size()) {
            return tombstone;
        }
        var chunk = chunks.get(chunkIndex);
        return chunk.get(chunkPosition);
    }

    @Override
    public double[] getByIndexes(int[] indexes) {
        var buffer = new double[indexes.length];
        readByIndexes(indexes, buffer);
        return buffer;
    }

    @Override
    public double[] getByIndexes(int from, int to) {
        var buffer = new double[to - from + 1];
        readByIndexes(from, to, buffer);
        return buffer;
    }


    @Override
    public void readByIndexes(int[] indexes, double[] buffer) {
        if (indexes.length > buffer.length) {
            throw new IllegalArgumentException("indexes length should be less or equals to buffer length");
        }
        for (int i = 0; i < indexes.length; i++) {
            buffer[i] = get(indexes[i]);
        }
    }

    @Override
    public void readByIndexes(int from, int to, double[] buffer) {
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
    public void writeByIndexes(int[] indexes, double[] values) {
        if (indexes.length > values.length) {
            throw new IllegalArgumentException("indexes length should be less or equals to buffer length");
        }
        for (int i = 0; i < indexes.length; i++) {
            write(indexes[i], values[i]);
        }
    }

    @Override
    public void writeByIndexes(int from, int to, double[] values) {
        var size = to - from + 1;
        if (size > values.length) {
            throw new IllegalArgumentException("Bound length should be less or equals to buffer length");
        }
        if (from > to) {
            throw new IllegalArgumentException("'from' should be less or equals to 'to'");
        }
        for (int i = 0; from <= to; from++, i++) {
            write(from, values[i]);
        }
    }

    @Override
    public final double getTombstone() {
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
