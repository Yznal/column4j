package org.column4j.column.impl.statistic;

import org.column4j.column.statistic.Int8Statistic;
import org.column4j.utils.Int8VectorUtils;

import java.util.function.Supplier;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Int8StatisticImpl implements Int8Statistic {
    private final Supplier<byte[]> dataSupplier;
    private final byte tombstone;

    private byte firstValue;
    private int firstIndex;
    private byte lastValue;
    private int lastIndex;
    private byte min;
    private byte max;
    private byte sum;
    private int count;

    public Int8StatisticImpl(Supplier<byte[]> dataSupplier, byte tombstone) {
        this.dataSupplier = dataSupplier;
        this.tombstone = tombstone;

        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
        this.min = Byte.MAX_VALUE;
        this.max = Byte.MIN_VALUE;
    }

    @Override
    public void onValueChanged(int position, byte oldValue, byte newValue) {
        sum += newValue;
        sum -= oldValue;

        if (position == firstIndex) {
            firstValue = newValue;
        }
        if (position == lastIndex) {
            lastValue = newValue;
        }
        if (newValue < min) {
            min = newValue;
        } else if (min == oldValue) {
            var data = dataSupplier.get();
            min = Int8VectorUtils.min(data, tombstone, firstIndex, lastIndex + 1);
        }
        if (newValue > max) {
            max = newValue;
        } else if (max == oldValue) {
            var data = dataSupplier.get();
            max = Int8VectorUtils.max(data, tombstone, firstIndex, lastIndex + 1);
        }
    }

    @Override
    public void onValueAdded(int position, byte newValue) {
        sum += newValue;
        count++;

        if (firstIndex == -1 || position < firstIndex) {
            firstIndex = position;
            firstValue = newValue;
        }
        if (lastIndex == -1 || position > lastIndex) {
            lastIndex = position;
            lastValue = newValue;
        }

        min = min > newValue ? newValue : min;
        max = max < newValue ? newValue : max;
    }

    @Override
    public void onValueRemoved(int position, byte oldValue) {
        sum -= oldValue;
        count--;

        if (firstIndex == position && lastIndex == position) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == position) {
            var data = dataSupplier.get();
            firstIndex = Int8VectorUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == position) {
            var data = dataSupplier.get();
            lastIndex = Int8VectorUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }

        if (min == oldValue) {
            if (firstIndex == -1) {
                min = Byte.MAX_VALUE;
            } else {
                var data = dataSupplier.get();
                min = Int8VectorUtils.min(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
        if (max == oldValue) {
            if (firstIndex == -1) {
                max = Byte.MIN_VALUE;
            } else {
                var data = dataSupplier.get();
                max = Int8VectorUtils.max(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
    }

    @Override
    public byte getFirstValue() {
        return firstValue;
    }

    @Override
    public byte getLastValue() {
        return lastValue;
    }

    @Override
    public byte getMin() {
        return min;
    }

    @Override
    public byte getMax() {
        return max;
    }

    @Override
    public int getFirstIndex() {
        return firstIndex;
    }

    @Override
    public int getLastIndex() {
        return lastIndex;
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public long getSum() {
        return sum;
    }
}
