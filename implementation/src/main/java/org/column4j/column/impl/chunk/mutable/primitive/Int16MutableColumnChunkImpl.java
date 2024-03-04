package org.column4j.column.impl.chunk.mutable.primitive;

import org.column4j.column.chunk.mutable.primitive.Int16MutableColumnChunk;
import org.column4j.column.impl.statistic.Int16StatisticImpl;
import org.column4j.column.statistic.Int16Statistic;

import java.util.Arrays;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Int16MutableColumnChunkImpl implements Int16MutableColumnChunk {

    private final int size;
    private final short[] data;
    private final short tombstone;
    private final Int16Statistic statistic;

    public Int16MutableColumnChunkImpl(int size, short tombstone) {
        this.size = size;
        this.tombstone = tombstone;
        this.data = this.allocate();
        this.statistic = new Int16StatisticImpl(data, tombstone);
    }

    @Override
    public void write(int position, short value) {
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
    public short[] getData() {
        return this.data;
    }

    @Override
    public Int16Statistic getStatistic() {
        return statistic;
    }

    private short[] allocate() {
        var data = new short[size];
        if (tombstone != (short) 0) {
            Arrays.fill(data, tombstone);
        }
        return data;
    }

    @Override
    public short get(int position) {
        var data = this.data;
        return data[position];
    }
}
