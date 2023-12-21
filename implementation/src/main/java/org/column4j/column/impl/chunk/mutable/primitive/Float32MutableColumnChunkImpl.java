package org.column4j.column.impl.chunk.mutable.primitive;

import org.column4j.column.chunk.mutable.primitive.Float32MutableColumnChunk;
import org.column4j.column.impl.statistic.Float32StatisticImpl;
import org.column4j.column.statistic.Float32Statistic;

import java.util.Arrays;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Float32MutableColumnChunkImpl implements Float32MutableColumnChunk {
    private final int size;
    private final float[] data;
    private final float tombstone;
    private final Float32Statistic statistic;

    public Float32MutableColumnChunkImpl(int size, float tombstone) {
        this.size = size;
        this.tombstone = tombstone;
        this.data = this.allocate();
        this.statistic = new Float32StatisticImpl(this.data, tombstone);
    }

    @Override
    public void write(int position, float value) {
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
    public float[] getData() {
        return this.data;
    }

    @Override
    public Float32Statistic getStatistic() {
        return statistic;
    }

    private float[] allocate() {
        var data = new float[size];
        Arrays.fill(data, tombstone);
        return data;
    }

    @Override
    public float get(int position) {
        var data = this.data;
        return data[position];
    }
}
