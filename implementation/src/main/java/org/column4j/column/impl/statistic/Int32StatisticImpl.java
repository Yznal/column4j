package org.column4j.column.impl.statistic;

import org.column4j.column.statistic.Int32Statistic;
import org.column4j.utils.Int32VectorUtils;

import java.util.function.Supplier;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Int32StatisticImpl implements Int32Statistic {
    private final Supplier<int[]> dataSupplier;
    private final int tombstone;

    private int firstValue;
    private int firstIndex;
    private int lastValue;
    private int lastIndex;
    private int min;
    private int max;
    private long sum;
    private int count;

    public Int32StatisticImpl(Supplier<int[]> dataSupplier, int tombstone) {
        this.dataSupplier = dataSupplier;
        this.tombstone = tombstone;

        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
        this.min = Integer.MAX_VALUE;
        this.max = Integer.MIN_VALUE;
    }

    @Override
    public void onValueChanged(int position, int oldValue, int newValue) {
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
            min = Int32VectorUtils.min(data, tombstone, firstIndex, lastIndex + 1);
        }
        if (newValue > max) {
            max = newValue;
        } else if (max == oldValue) {
            var data = dataSupplier.get();
            max = Int32VectorUtils.max(data, tombstone, firstIndex, lastIndex + 1);
        }
    }

    @Override
    public void onValueAdded(int position, int newValue) {
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

        min = Math.min(min, newValue);
        max = Math.max(max, newValue);
    }

    @Override
    public void onValueRemoved(int position, int oldValue) {
        sum -= oldValue;
        count--;

        if (firstIndex == position && lastIndex == position) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == position) {
            var data = dataSupplier.get();
            firstIndex = Int32VectorUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == position) {
            var data = dataSupplier.get();
            lastIndex = Int32VectorUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }

        if (min == oldValue) {
            if (firstIndex == -1) {
                min = Integer.MAX_VALUE;
            } else {
                var data = dataSupplier.get();
                min = Int32VectorUtils.min(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
        if (max == oldValue) {
            if (firstIndex == -1) {
                max = Integer.MIN_VALUE;
            } else {
                var data = dataSupplier.get();
                max = Int32VectorUtils.max(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
    }

    @Override
    public int getFirstValue() {
        return firstValue;
    }

    @Override
    public int getLastValue() {
        return lastValue;
    }

    @Override
    public int getMin() {
        return min;
    }

    @Override
    public int getMax() {
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
