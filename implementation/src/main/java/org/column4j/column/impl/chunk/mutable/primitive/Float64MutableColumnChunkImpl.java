package org.column4j.column.impl.chunk.mutable.primitive;

import org.column4j.column.chunk.mutable.primitive.Float64MutableColumnChunk;
import org.column4j.column.impl.statistic.Float64StatisticImpl;
import org.column4j.column.statistic.Float64Statistic;

import java.util.Arrays;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Float64MutableColumnChunkImpl implements Float64MutableColumnChunk {
    private final int size;
    private final double[] data;
    private final double tombstone;
    private final Float64Statistic statistic;

    public Float64MutableColumnChunkImpl(int size, double tombstone) {
        this.size = size;
        this.tombstone = tombstone;
        this.data = this.allocate();
        this.statistic = new Float64StatisticImpl(data, tombstone);
    }

    @Override
    public void write(int position, double value) {
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
    public double[] getData() {
        return this.data;
    }

    @Override
    public Float64Statistic getStatistic() {
        return statistic;
    }

    private double[] allocate() {
        var data = new double[size];
        Arrays.fill(data, tombstone);
        return data;
    }

    @Override
    public double get(int position) {
        var data = this.data;
        return data[position];
    }
}
