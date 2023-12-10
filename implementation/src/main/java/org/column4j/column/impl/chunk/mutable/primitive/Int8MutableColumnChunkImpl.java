package org.column4j.column.impl.chunk.mutable.primitive;

import com.google.common.base.Suppliers;
import org.column4j.column.chunk.mutable.primitive.Int8MutableColumnChunk;
import org.column4j.column.impl.statistic.Int8StatisticImpl;
import org.column4j.column.statistic.Int8Statistic;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Int8MutableColumnChunkImpl implements Int8MutableColumnChunk {

    private final int size;
    private final AtomicBoolean initialized;
    private final Supplier<byte[]> dataSupplier;
    private final byte tombstone;
    private final Int8Statistic statistic;

    public Int8MutableColumnChunkImpl(int size, byte tombstone) {
        this.size = size;
        this.initialized = new AtomicBoolean(false);
        this.dataSupplier = Suppliers.memoize(this::allocate);
        this.tombstone = tombstone;
        this.statistic = new Int8StatisticImpl(dataSupplier, tombstone);
    }

    @Override
    public void tombstone(int position) {
        write(position, tombstone);
    }

    @Override
    public void write(int position, byte value) {
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
    public byte[] getData() {
        return dataSupplier.get();
    }

    @Override
    public Int8Statistic getStatistic() {
        return statistic;
    }

    private byte[] allocate() {
        var data = new byte[size];
        Arrays.fill(data, tombstone);
        initialized.set(true);
        return data;
    }

    @Override
    public byte get(int position) {
        if(!initialized.get()) {
            return tombstone;
        }
        var data = dataSupplier.get();
        return data[position];
    }
}
