package org.column4j.column.impl.chunk.mutable.primitive;

import com.google.common.base.Suppliers;
import org.column4j.column.chunk.mutable.primitive.Int64MutableColumnChunk;
import org.column4j.column.impl.statistic.Int64StatisticImpl;
import org.column4j.column.statistic.Int64Statistic;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Int64MutableColumnChunkImpl implements Int64MutableColumnChunk {

    private final int size;
    private final AtomicBoolean initialized;
    private final Supplier<long[]> dataSupplier;
    private final long tombstone;
    private final Int64Statistic statistic;

    public Int64MutableColumnChunkImpl(int size, long tombstone) {
        this.size = size;
        this.initialized = new AtomicBoolean(false);
        this.dataSupplier = Suppliers.memoize(this::allocate);
        this.tombstone = tombstone;
        this.statistic = new Int64StatisticImpl(dataSupplier, tombstone);
    }

    @Override
    public void tombstone(int position) {
        write(position, tombstone);
    }

    @Override
    public void write(int position, long value) {
        if (position < 0 || position >= size) {
            throw new IllegalArgumentException("Out of range %d".formatted(position));
        }
        if (value == tombstone && !initialized.get()) {
            return;
        }
        var data = dataSupplier.get();
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
        return dataSupplier.get();
    }

    @Override
    public Int64Statistic getStatistic() {
        return statistic;
    }

    private long[] allocate() {
        var data = new long[size];
        Arrays.fill(data, tombstone);
        initialized.set(true);
        return data;
    }

    @Override
    public long get(int position) {
        if(!initialized.get()) {
            return tombstone;
        }
        var data = dataSupplier.get();
        return data[position];
    }
}
