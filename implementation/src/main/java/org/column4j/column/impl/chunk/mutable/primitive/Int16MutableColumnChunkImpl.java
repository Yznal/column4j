package org.column4j.column.impl.chunk.mutable.primitive;

import com.google.common.base.Suppliers;
import org.column4j.column.chunk.mutable.primitive.Int16MutableColumnChunk;
import org.column4j.column.impl.statistic.Int16StatisticImpl;
import org.column4j.column.statistic.Int16Statistic;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Int16MutableColumnChunkImpl implements Int16MutableColumnChunk {

    private final int size;
    private final AtomicBoolean initialized;
    private final Supplier<short[]> dataSupplier;
    private final short tombstone;
    private final Int16Statistic statistic;

    public Int16MutableColumnChunkImpl(int size, short tombstone) {
        this.size = size;
        this.initialized = new AtomicBoolean(false);
        this.dataSupplier = Suppliers.memoize(this::allocate);
        this.tombstone = tombstone;
        this.statistic = new Int16StatisticImpl(dataSupplier, tombstone);
    }

    @Override
    public void tombstone(int position) {
        write(position, tombstone);
    }

    @Override
    public void write(int position, short value) {
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
    public short[] getData() {
        return dataSupplier.get();
    }

    @Override
    public Int16Statistic getStatistic() {
        return statistic;
    }

    private short[] allocate() {
        var data = new short[size];
        Arrays.fill(data, tombstone);
        initialized.set(true);
        return data;
    }

    @Override
    public short get(int position) {
        if(!initialized.get()) {
            return tombstone;
        }
        var data = dataSupplier.get();
        return data[position];
    }
}
