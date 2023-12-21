package org.column4j.column.impl.chunk.mutable.primitive;

import org.column4j.column.chunk.mutable.primitive.Int64MutableColumnChunk;
import org.column4j.column.impl.statistic.Int64StatisticImpl;
import org.column4j.column.statistic.Int64Statistic;

import java.util.Arrays;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Int64MutableColumnChunkImpl implements Int64MutableColumnChunk {

    private final int size;
    private final long[] data;
    private final long tombstone;
    private final Int64Statistic statistic;

    public Int64MutableColumnChunkImpl(int size, long tombstone) {
        this.size = size;
        this.tombstone = tombstone;
        this.data = this.allocate();
        this.statistic = new Int64StatisticImpl(data, tombstone);
    }

    @Override
    public void write(int position, long value) {
        var data = this.data;
        var oldValue = data[position];
        if (oldValue == value) {
            return;
        }
        data[position] = value;
        if (oldValue == tombstone) {
            statistic.onValueAdded(position, value);
        } else if (value == tombstone) {
            statistic.onValueRemoved(position, oldValue);
        } else {
            statistic.onValueChanged(position, oldValue, value);
        }
    }

    @Override
    public long[] getData() {
        return this.data;
    }

    @Override
    public Int64Statistic getStatistic() {
        return statistic;
    }

    private long[] allocate() {
        var data = new long[size];
        Arrays.fill(data, tombstone);
        return data;
    }

    @Override
    public long get(int position) {
        var data = this.data;
        return data[position];
    }
}
