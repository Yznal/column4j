package org.column4j.column.impl.chunk.mutable.primitive;

import org.column4j.column.chunk.mutable.primitive.Int32MutableColumnChunk;
import org.column4j.column.impl.statistic.Int32StatisticImpl;
import org.column4j.column.statistic.Int32Statistic;

import java.util.Arrays;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Int32MutableColumnChunkImpl implements Int32MutableColumnChunk {

    private final int size;
    private final int[] data;
    private final int tombstone;
    private final Int32Statistic statistic;

    public Int32MutableColumnChunkImpl(int size, int tombstone) {
        this.size = size;
        this.tombstone = tombstone;
        this.data = this.allocate();
        this.statistic = new Int32StatisticImpl(data, tombstone);
    }

    @Override
    public void write(int position, int value) {
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
    public int[] getData() {
        return this.data;
    }

    @Override
    public Int32Statistic getStatistic() {
        return statistic;
    }

    private int[] allocate() {
        var data = new int[size];
        if (tombstone != 0) {
            Arrays.fill(data, tombstone);
        }
        return data;
    }

    @Override
    public int get(int position) {
        var data = this.data;
        return data[position];
    }
}
