package org.column4j.mutable.aggregated.impl;

import org.column4j.ColumnType;
import org.column4j.mutable.aggregated.AggregatedIntMutableColumn;
import org.column4j.mutable.aggregated.statistic.IntStatistic;
import org.column4j.mutable.primitive.IntMutableColumn;
import org.column4j.utils.IntStatisticUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class AggregatedIntMutableColumnImpl implements AggregatedIntMutableColumn {

    private final int chunkSize;
    private final int tombstone;
    private final IntMutableColumn mutableColumn;
    private final Map<Integer, IntStatistic> chunkedColumnStatistics;

    public AggregatedIntMutableColumnImpl(IntMutableColumn mutableColumn, int chunkSize) {
        this.mutableColumn = mutableColumn;
        this.chunkSize = chunkSize;
        this.tombstone = mutableColumn.getTombstone();
        var dataSize = mutableColumn.size();
        var chunks = Math.max(1, (dataSize + chunkSize / 2) / chunkSize);
        this.chunkedColumnStatistics = new HashMap<>(chunks, 1);
        initStats(chunks, dataSize);
    }

    private void initStats(int chunks, int dataSize) {
        var data = mutableColumn.getData();
        for (var chunk = 0; chunk < chunks; chunk++) {
            var from = chunk * chunkSize;
            var to = (chunk + 1) * chunkSize;
            IntStatistic intStatistic;
            if (from >= dataSize) {
                intStatistic = new IntStatistic(tombstone);
            } else {
                intStatistic = IntStatisticUtils.collectStatistic(data, dataSize, tombstone, from, to);
            }
            chunkedColumnStatistics.put(chunk, intStatistic);
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
    public int[] getData() {
        return mutableColumn.getData();
    }

    @Override
    public void tombstone(int position) {
        var oldValue = getValueOrTombstone(position);
        mutableColumn.tombstone(position);
        onValueSet(position, oldValue, tombstone);
    }

    private void onValueSet(int position, int oldValue, int newValue) {
        var intStat = getIntStats(position);
        var data = getData();
        intStat.onValueSet(data, position, oldValue, newValue);
    }

    private IntStatistic getIntStats(int position) {
        var chunk = position / chunkSize;
        return chunkedColumnStatistics.computeIfAbsent(
                chunk,
                it -> new IntStatistic(tombstone)
        );
    }

    private int getValueOrTombstone(int position) {
        var oldData = getData();
        return oldData.length > position ? oldData[position] : tombstone;
    }

    @Override
    public void write(int position, int value) {
        var oldValue = getValueOrTombstone(position);
        mutableColumn.write(position, value);
        onValueSet(position, oldValue, value);
    }

    @Override
    public int getTombstone() {
        return tombstone;
    }

    @Override
    public int getChunkSize() {
        return chunkSize;
    }

    @Override
    public Map<Integer, IntStatistic> getChunkedColumnStatistics() {
        return Collections.unmodifiableMap(chunkedColumnStatistics);
    }
}
