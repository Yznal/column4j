package org.column4j.column.impl.chunk.mutable;

import com.google.common.base.Suppliers;
import org.column4j.column.chunk.mutable.StringMutableColumnChunk;
import org.column4j.column.impl.statistic.StringStatisticImpl;
import org.column4j.column.statistic.StringStatistic;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class StringMutableColumnChunkImpl implements StringMutableColumnChunk {
    private final int size;
    private final AtomicBoolean initialized;
    private final Supplier<String[]> dataSupplier;
    private final String tombstone;
    private final StringStatistic statistic;

    public StringMutableColumnChunkImpl(int size, String tombstone) {
        this.size = size;
        this.initialized = new AtomicBoolean(false);
        this.dataSupplier = Suppliers.memoize(this::allocate);
        this.tombstone = tombstone;
        this.statistic = new StringStatisticImpl(dataSupplier, tombstone);
    }

    @Override
    public void tombstone(int position) {
        write(position, tombstone);
    }

    @Override
    public void write(int position, String value) {
        if (position < 0 || position >= size) {
            throw new IllegalArgumentException("Out of range %d".formatted(position));
        }
        if (Objects.equals(value, tombstone) && !initialized.get()) {
            return;
        }
        var data = dataSupplier.get();
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
        return dataSupplier.get();
    }

    @Override
    public StringStatistic getStatistic() {
        return statistic;
    }

    private String[] allocate() {
        var data = new String[size];
        Arrays.fill(data, tombstone);
        initialized.set(true);
        return data;
    }

    @Override
    public String get(int position) {
        if(!initialized.get()) {
            return tombstone;
        }
        var data = dataSupplier.get();
        return data[position];
    }
}
