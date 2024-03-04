package org.column4j.column.impl.statistic;

import org.column4j.column.statistic.Int16Statistic;
import org.column4j.utils.Int16VectorUtils;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Int16StatisticImpl implements Int16Statistic {
    private final short[] data;
    private final short tombstone;

    private short firstValue;
    private int firstIndex;
    private short lastValue;
    private int lastIndex;
    private short min;
    private short max;
    private short sum;
    private int count;

    public Int16StatisticImpl(short[] data, short tombstone) {
        this.data = data;
        this.tombstone = tombstone;

        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
        this.min = Short.MAX_VALUE;
        this.max = Short.MIN_VALUE;
    }

    @Override
    public void onValueChanged(int position, short oldValue, short newValue) {
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
            min = Int16VectorUtils.min(data, tombstone, firstIndex, lastIndex + 1);
        }
        if (newValue > max) {
            max = newValue;
        } else if (max == oldValue) {
            var data = this.data;
            max = Int16VectorUtils.max(data, tombstone, firstIndex, lastIndex + 1);
        }
    }

    @Override
    public void onValueAdded(int position, short newValue) {
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
    public void onValueRemoved(int position, short oldValue) {
        sum -= oldValue;
        count--;

        if (firstIndex == position && lastIndex == position) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == position) {
            var data = this.data;
            firstIndex = Int16VectorUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == position) {
            var data = this.data;
            lastIndex = Int16VectorUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }

        if (min == oldValue) {
            if (firstIndex == -1) {
                min = Short.MAX_VALUE;
            } else {
                var data = this.data;
                min = Int16VectorUtils.min(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
        if (max == oldValue) {
            if (firstIndex == -1) {
                max = Short.MIN_VALUE;
            } else {
                var data = this.data;
                max = Int16VectorUtils.max(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
    }

    @Override
    public short getFirstValue() {
        return firstValue;
    }

    @Override
    public short getLastValue() {
        return lastValue;
    }

    @Override
    public short getMin() {
        return min;
    }

    @Override
    public short getMax() {
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
