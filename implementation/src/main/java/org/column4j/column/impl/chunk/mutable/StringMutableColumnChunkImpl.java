package org.column4j.column.impl.chunk.mutable;

import org.column4j.column.chunk.mutable.StringMutableColumnChunk;
import org.column4j.column.impl.statistic.StringStatisticImpl;
import org.column4j.column.statistic.StringStatistic;

import java.util.Objects;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class StringMutableColumnChunkImpl implements StringMutableColumnChunk {
    private final StringStorage data;
    private final String tombstone;
    private final StringStatistic statistic;

    public StringMutableColumnChunkImpl(int size, String tombstone) {
        this.tombstone = tombstone;
        this.data = new StringStorage(size, tombstone);
        this.statistic = new StringStatisticImpl(data, tombstone);
    }

    @Override
    public void write(int position, String value) {
        var oldValue = data.get(position);
        if (Objects.equals(oldValue, value)) {
            return;
        }
        data.write(position, value);
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
        return this.data.getData();
    }

    @Override
    public StringStatistic getStatistic() {
        return statistic;
    }

    @Override
    public String get(int position) {
        return data.get(position);
    }
}
