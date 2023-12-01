package org.column4j.mutable.aggregated.impl;

import org.column4j.ColumnType;
import org.column4j.utils.VectorAPIUtils;
import org.column4j.mutable.aggregated.AggregatedIntMutableColumn;
import org.column4j.mutable.aggregated.IntStatistic;
import org.column4j.mutable.primitive.IntMutableColumn;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class AggregatedIntMutableColumnImpl implements AggregatedIntMutableColumn {

    private final int bucketSize;
    private final int tombstone;
    private final IntMutableColumn intMutableColumn;
    private final Map<Integer, IntStatistic> bucketColumnStatistics;

    public AggregatedIntMutableColumnImpl(IntMutableColumn intMutableColumn, int bucketSize) {
        this.intMutableColumn = intMutableColumn;
        this.bucketSize = bucketSize;
        this.tombstone = intMutableColumn.getTombstone();
        var dataSize = intMutableColumn.size();
        var buckets = Math.max(1, (dataSize + bucketSize / 2) / bucketSize);
        this.bucketColumnStatistics = new HashMap<>(buckets, 1);
        initStats(buckets, dataSize);
    }

    private void initStats(int buckets, int dataSize) {
        var data = intMutableColumn.getData();
        for (int bucket = 0; bucket < buckets; bucket++) {
            int from = bucket * bucketSize;
            int to = (bucket + 1) * bucketSize;
            IntStatistic intStatistic;
            if (from >= dataSize) {
                intStatistic = new IntStatistic(tombstone);
            } else {
                intStatistic = VectorAPIUtils.collectIntStats(data, dataSize, tombstone, from, to);
            }
            bucketColumnStatistics.put(bucket, intStatistic);
        }
    }

    @Override
    public int firstRowIndex() {
        return intMutableColumn.firstRowIndex();
    }

    @Override
    public int capacity() {
        return intMutableColumn.capacity();
    }

    @Override
    public int size() {
        return intMutableColumn.size();
    }

    @Override
    public ColumnType type() {
        return intMutableColumn.type();
    }

    @Override
    public int[] getData() {
        return intMutableColumn.getData();
    }

    @Override
    public void tombstone(int position) {
        var oldValue = getValueOrTombstone(position);
        intMutableColumn.tombstone(position);
        onValueSet(position, oldValue, tombstone);
    }

    private void onValueSet(int position, int oldValue, int newValue) {
        var intStat = getIntStats(position);
        var data = getData();
        intStat.onValueSet(data, position, oldValue, newValue);
    }

    private IntStatistic getIntStats(int position) {
        var bucket = position / bucketSize;
        return bucketColumnStatistics.computeIfAbsent(
                bucket,
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
        intMutableColumn.write(position, value);
        onValueSet(position, oldValue, value);
    }

    @Override
    public int getTombstone() {
        return tombstone;
    }

    @Override
    public int getBucketSize() {
        return bucketSize;
    }

    @Override
    public Map<Integer, IntStatistic> getBucketColumnStatistics() {
        return Collections.unmodifiableMap(bucketColumnStatistics);
    }
}
