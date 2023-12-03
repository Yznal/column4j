package org.column4j.mutable.aggregated.impl;

import org.column4j.ColumnType;
import org.column4j.mutable.aggregated.AggregatedDoubleMutableColumn;
import org.column4j.mutable.aggregated.statistic.DoubleStatistic;
import org.column4j.mutable.primitive.DoubleMutableColumn;
import org.column4j.utils.DoubleStatisticUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class AggregatedDoubleMutableColumnImpl implements AggregatedDoubleMutableColumn {

    private final int chunkSize;
    private final double tombstone;
    private final DoubleMutableColumn mutableColumn;
    private final Map<Integer, DoubleStatistic> chunkedColumnStatistics;

    public AggregatedDoubleMutableColumnImpl(DoubleMutableColumn mutableColumn, int chunkSize) {
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
            DoubleStatistic chunkStatistic;
            if (from >= dataSize) {
                chunkStatistic = new DoubleStatistic(tombstone);
            } else {
                chunkStatistic = DoubleStatisticUtils.collectStatistic(data, dataSize, tombstone, from, to);
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
    public double[] getData() {
        return mutableColumn.getData();
    }

    @Override
    public void tombstone(int position) {
        var oldValue = getValueOrTombstone(position);
        mutableColumn.tombstone(position);
        onValueSet(position, oldValue, tombstone);
    }

    private void onValueSet(int position, double oldValue, double newValue) {
        var intStat = getChunkStatistic(position);
        var data = getData();
        intStat.onValueSet(data, position, oldValue, newValue);
    }

    private DoubleStatistic getChunkStatistic(int position) {
        var chunk = position / chunkSize;
        return chunkedColumnStatistics.computeIfAbsent(
                chunk,
                it -> new DoubleStatistic(tombstone)
        );
    }

    private double getValueOrTombstone(int position) {
        var oldData = getData();
        return oldData.length > position ? oldData[position] : tombstone;
    }

    @Override
    public void write(int position, double value) {
        var oldValue = getValueOrTombstone(position);
        mutableColumn.write(position, value);
        onValueSet(position, oldValue, value);
    }

    @Override
    public double getTombstone() {
        return tombstone;
    }

    @Override
    public int getChunkSize() {
        return chunkSize;
    }

    @Override
    public Map<Integer, DoubleStatistic> getChunkedColumnStatistics() {
        return Collections.unmodifiableMap(chunkedColumnStatistics);
    }
}
