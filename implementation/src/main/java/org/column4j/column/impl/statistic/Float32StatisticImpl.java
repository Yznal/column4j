package org.column4j.column.impl.statistic;

import org.column4j.column.statistic.Float32Statistic;
import org.column4j.utils.Float32VectorUtils;

import java.util.function.Supplier;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Float32StatisticImpl implements Float32Statistic {
    private final Supplier<float[]> dataSupplier;
    private final float tombstone;

    private float firstValue;
    private int firstIndex;
    private float lastValue;
    private int lastIndex;
    private float min;
    private float max;
    private double sum;
    private int count;

    public Float32StatisticImpl(Supplier<float[]> dataSupplier, float tombstone) {
        this.dataSupplier = dataSupplier;
        this.tombstone = tombstone;

        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
        this.min = Float.MAX_VALUE;
        this.max = Float.MIN_VALUE;
    }

    @Override
    public void onValueChanged(int position, float oldValue, float newValue) {
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
            min = Float32VectorUtils.min(data, tombstone, firstIndex, lastIndex + 1);
        }
        if (newValue > max) {
            max = newValue;
        } else if (max == oldValue) {
            var data = dataSupplier.get();
            max = Float32VectorUtils.max(data, tombstone, firstIndex, lastIndex + 1);
        }
    }

    @Override
    public void onValueAdded(int position, float newValue) {
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
    public void onValueRemoved(int position, float oldValue) {
        sum -= oldValue;
        count--;

        if (firstIndex == position && lastIndex == position) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == position) {
            var data = dataSupplier.get();
            firstIndex = Float32VectorUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == position) {
            var data = dataSupplier.get();
            lastIndex = Float32VectorUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }

        if (min == oldValue) {
            if (firstIndex == -1) {
                min = Float.MAX_VALUE;
            } else {
                var data = dataSupplier.get();
                min = Float32VectorUtils.min(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
        if (max == oldValue) {
            if (firstIndex == -1) {
                max = Float.MIN_VALUE;
            } else {
                var data = dataSupplier.get();
                max = Float32VectorUtils.max(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
    }

    @Override
    public float getFirstValue() {
        return firstValue;
    }

    @Override
    public float getLastValue() {
        return lastValue;
    }

    @Override
    public float getMin() {
        return min;
    }

    @Override
    public float getMax() {
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
