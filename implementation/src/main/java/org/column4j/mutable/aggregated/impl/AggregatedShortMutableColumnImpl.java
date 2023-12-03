package org.column4j.mutable.aggregated.impl;

import org.column4j.ColumnType;
import org.column4j.mutable.aggregated.AggregatedShortMutableColumn;
import org.column4j.mutable.aggregated.statistic.ShortStatistic;
import org.column4j.mutable.primitive.ShortMutableColumn;
import org.column4j.utils.ShortStatisticUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class AggregatedShortMutableColumnImpl implements AggregatedShortMutableColumn {

    private final int chunkSize;
    private final short tombstone;
    private final ShortMutableColumn mutableColumn;
    private final Map<Integer, ShortStatistic> chunkedColumnStatistics;

    public AggregatedShortMutableColumnImpl(ShortMutableColumn mutableColumn, int chunkSize) {
        this.mutableColumn = mutableColumn;
        this.chunkSize = chunkSize;
        this.tombstone = mutableColumn.getTombstone();
        var dataSize = mutableColumn.size();
        var chunks = Math.max(1, (dataSize + chunkSize / 2) / chunkSize);
        this.chunkedColumnStatistics = new HashMap<>(chunks, 1);
        initStatistic(chunks, dataSize);
    }

    private void initStatistic(int chunks, int dataSize) {
        var data = mutableColumn.getData();
        for (var chunk = 0; chunk < chunks; chunk++) {
            var from = chunk * chunkSize;
            var to = (chunk + 1) * chunkSize;
            ShortStatistic chunkStatistic;
            if (from >= dataSize) {
                chunkStatistic = new ShortStatistic(tombstone);
            } else {
                chunkStatistic = ShortStatisticUtils.collectStatistic(data, dataSize, tombstone, from, to);
            }
            chunkedColumnStatistics.put(chunk, chunkStatistic);
        }
    }

    @Override
    public int firstRowIndex() {
        return mutableColumn.firstRowIndex();
    }

    @Override
    public int capacity() {
        return mutableColumn.capacity();
    }

    @Override
    public int size() {
        return mutableColumn.size();
    }

    @Override
    public ColumnType type() {
        return mutableColumn.type();
    }

    @Override
    public short[] getData() {
        return mutableColumn.getData();
    }

    @Override
    public void tombstone(int position) {
        var oldValue = getValueOrTombstone(position);
        mutableColumn.tombstone(position);
        onValueSet(position, oldValue, tombstone);
    }

    private void onValueSet(int position, short oldValue, short newValue) {
        var intStat = getChunkStatistic(position);
        var data = getData();
        intStat.onValueSet(data, position, oldValue, newValue);
    }

    private ShortStatistic getChunkStatistic(int position) {
        var chunk = position / chunkSize;
        return chunkedColumnStatistics.computeIfAbsent(
                chunk,
                it -> new ShortStatistic(tombstone)
        );
    }

    private short getValueOrTombstone(int position) {
        var oldData = getData();
        return oldData.length > position ? oldData[position] : tombstone;
    }

    @Override
    public void write(int position, short value) {
        var oldValue = getValueOrTombstone(position);
        mutableColumn.write(position, value);
        onValueSet(position, oldValue, value);
    }

    @Override
    public short getTombstone() {
        return tombstone;
    }

    @Override
    public int getChunkSize() {
        return chunkSize;
    }

    @Override
    public Map<Integer, ShortStatistic> getChunkedColumnStatistics() {
        return Collections.unmodifiableMap(chunkedColumnStatistics);
    }
}
