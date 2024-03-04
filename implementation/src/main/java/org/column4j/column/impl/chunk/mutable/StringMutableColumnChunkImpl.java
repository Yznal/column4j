package org.column4j.column.impl.chunk.mutable;

import org.column4j.column.chunk.mutable.StringMutableColumnChunk;
import org.column4j.column.impl.statistic.StringStatisticImpl;
import org.column4j.column.statistic.StringStatistic;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class StringMutableColumnChunkImpl implements StringMutableColumnChunk {
    private final int size;
    private final String[] data;
    private final String tombstone;
    private final StringStatistic statistic;

    public StringMutableColumnChunkImpl(int size, String tombstone) {
        this.size = size;
        this.tombstone = tombstone;
        this.data = this.allocate();
        this.statistic = new StringStatisticImpl(data, tombstone);
    }

    @Override
    public void write(int position, String value) {
        var data = this.data;
        var oldValue = data[position];
        if (Objects.equals(oldValue, value)) {
            return;
        }
        data[position] = value;
        if (Objects.equals(oldValue, tombstone)) {
            statistic.onValueAdded(position, value);
        } else if (Objects.equals(value, tombstone)) {
            statistic.onValueRemoved(position, oldValue);
        } else {
            statistic.onValueChanged(position, oldValue, value);
        }
    }

    @Override
    public String[] getData() {
        return this.data;
    }

    @Override
    public StringStatistic getStatistic() {
        return statistic;
    }

    private String[] allocate() {
        var data = new String[size];
        if (tombstone != null) {
            Arrays.fill(data, tombstone);
        }
        return data;
    }

    @Override
    public String get(int position) {
        var data = this.data;
        return data[position];
    }
}
