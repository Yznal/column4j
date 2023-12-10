package org.column4j.column.impl.chunk.mutable.primitive;

import com.google.common.base.Suppliers;
import org.column4j.column.chunk.mutable.primitive.Float64MutableColumnChunk;
import org.column4j.column.impl.statistic.Float64StatisticImpl;
import org.column4j.column.statistic.Float64Statistic;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Float64MutableColumnChunkImpl implements Float64MutableColumnChunk {
    private final int size;
    private final AtomicBoolean initialized;
    private final Supplier<double[]> dataSupplier;
    private final double tombstone;
    private final Float64Statistic statistic;

    public Float64MutableColumnChunkImpl(int size, double tombstone) {
        this.size = size;
        this.initialized = new AtomicBoolean(false);
        this.dataSupplier = Suppliers.memoize(this::allocate);
        this.tombstone = tombstone;
        this.statistic = new Float64StatisticImpl(dataSupplier, tombstone);
    }

    @Override
    public void tombstone(int position) {
        write(position, tombstone);
    }

    @Override
    public void write(int position, double value) {
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
    public double[] getData() {
        return dataSupplier.get();
    }

    @Override
    public Float64Statistic getStatistic() {
        return statistic;
    }

    private double[] allocate() {
        var data = new double[size];
        Arrays.fill(data, tombstone);
        initialized.set(true);
        return data;
    }

    @Override
    public double get(int position) {
        if (!initialized.get()) {
            return tombstone;
        }
        var data = dataSupplier.get();
        return data[position];
    }
}
