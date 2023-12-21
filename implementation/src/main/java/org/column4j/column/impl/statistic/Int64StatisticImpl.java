package org.column4j.column.impl.statistic;

import org.column4j.column.statistic.Int64Statistic;
import org.column4j.utils.Int64VectorUtils;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Int64StatisticImpl implements Int64Statistic {
    private final long[] data;
    private final long tombstone;

    private long firstValue;
    private int firstIndex;
    private long lastValue;
    private int lastIndex;
    private long min;
    private long max;
    private long sum;
    private int count;

    public Int64StatisticImpl(long[] data, long tombstone) {
        this.data = data;
        this.tombstone = tombstone;

        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
        this.min = Long.MAX_VALUE;
        this.max = Long.MIN_VALUE;
    }

    @Override
    public void onValueChanged(int position, long oldValue, long newValue) {
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
            var data = this.data;
            min = Int64VectorUtils.min(data, tombstone, firstIndex, lastIndex + 1);
        }
        if (newValue > max) {
            max = newValue;
        } else if (max == oldValue) {
            var data = this.data;
            max = Int64VectorUtils.max(data, tombstone, firstIndex, lastIndex + 1);
        }
    }

    @Override
    public void onValueAdded(int position, long newValue) {
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
    public void onValueRemoved(int position, long oldValue) {
        sum -= oldValue;
        count--;

        if (firstIndex == position && lastIndex == position) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == position) {
            var data = this.data;
            firstIndex = Int64VectorUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == position) {
            var data = this.data;
            lastIndex = Int64VectorUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }

        if (min == oldValue) {
            if (firstIndex == -1) {
                min = Long.MAX_VALUE;
            } else {
                var data = this.data;
                min = Int64VectorUtils.min(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
        if (max == oldValue) {
            if (firstIndex == -1) {
                max = Long.MIN_VALUE;
            } else {
                var data = this.data;
                max = Int64VectorUtils.max(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
    }

    @Override
    public long getFirstValue() {
        return firstValue;
    }

    @Override
    public long getLastValue() {
        return lastValue;
    }

    @Override
    public long getMin() {
        return min;
    }

    @Override
    public long getMax() {
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
