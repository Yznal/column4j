package org.column4j.column.impl.statistic;

import org.column4j.column.statistic.Float64Statistic;
import org.column4j.utils.Float64VectorUtils;

import java.util.function.Supplier;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Float64StatisticImpl implements Float64Statistic {
    private final Supplier<double[]> dataSupplier;
    private final double tombstone;

    private double firstValue;
    private int firstIndex;
    private double lastValue;
    private int lastIndex;
    private double min;
    private double max;
    private double sum;
    private int count;

    public Float64StatisticImpl(Supplier<double[]> dataSupplier, double tombstone) {
        this.dataSupplier = dataSupplier;
        this.tombstone = tombstone;

        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
        this.min = Double.MAX_VALUE;
        this.max = Double.MIN_VALUE;
    }

    @Override
    public void onValueChanged(int position, double oldValue, double newValue) {
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
            min = Float64VectorUtils.min(data, tombstone, firstIndex, lastIndex + 1);
        }
        if (newValue > max) {
            max = newValue;
        } else if (max == oldValue) {
            var data = dataSupplier.get();
            max = Float64VectorUtils.max(data, tombstone, firstIndex, lastIndex + 1);
        }
    }

    @Override
    public void onValueAdded(int position, double newValue) {
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
    public void onValueRemoved(int position, double oldValue) {
        sum -= oldValue;
        count--;

        if (firstIndex == position && lastIndex == position) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == position) {
            var data = dataSupplier.get();
            firstIndex = Float64VectorUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == position) {
            var data = dataSupplier.get();
            lastIndex = Float64VectorUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }

        if (min == oldValue) {
            if (firstIndex == -1) {
                min = Float.MAX_VALUE;
            } else {
                var data = dataSupplier.get();
                min = Float64VectorUtils.min(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
        if (max == oldValue) {
            if (firstIndex == -1) {
                max = Float.MIN_VALUE;
            } else {
                var data = dataSupplier.get();
                max = Float64VectorUtils.max(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
    }

    @Override
    public double getFirstValue() {
        return firstValue;
    }

    @Override
    public double getLastValue() {
        return lastValue;
    }

    @Override
    public double getMin() {
        return min;
    }

    @Override
    public double getMax() {
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
    public double getSum() {
        return sum;
    }
}
