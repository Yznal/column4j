package org.column4j.column.impl.chunk.mutable.primitive;

import org.column4j.column.chunk.mutable.primitive.Int8MutableColumnChunk;
import org.column4j.column.impl.statistic.Int8StatisticImpl;
import org.column4j.column.statistic.Int8Statistic;

import java.util.Arrays;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Int8MutableColumnChunkImpl implements Int8MutableColumnChunk {

    private final int size;
    private final byte[] data;
    private final byte tombstone;
    private final Int8Statistic statistic;

    public Int8MutableColumnChunkImpl(int size, byte tombstone) {
        this.size = size;
        this.tombstone = tombstone;
        this.data = this.allocate();
        this.statistic = new Int8StatisticImpl(data, tombstone);
    }

    @Override
    public void write(int position, byte value) {
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
    public byte[] getData() {
        return this.data;
    }

    @Override
    public Int8Statistic getStatistic() {
        return statistic;
    }

    private byte[] allocate() {
        var data = new byte[size];
        if (tombstone != (byte) 0) {
            Arrays.fill(data, tombstone);
        }
        return data;
    }

    @Override
    public byte get(int position) {
        var data = this.data;
        return data[position];
    }
}
