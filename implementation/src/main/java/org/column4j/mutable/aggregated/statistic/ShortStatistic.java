package org.column4j.mutable.aggregated.statistic;

import org.column4j.utils.ShortStatisticUtils;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class ShortStatistic {

    private final short tombstone;

    private short firstValue;
    private int firstIndex;
    private short lastValue;
    private int lastIndex;
    private short min;
    private short max;
    private long sum;
    private int count;

    public ShortStatistic(short tombstone) {
        this.tombstone = tombstone;
        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
        this.min = Short.MAX_VALUE;
        this.max = Short.MIN_VALUE;
    }

    /**
     * Update statistics on value change
     * @param data source array
     * @param index index of changed element
     * @param oldValue old element value
     * @param newValue new element value
     */
    public void onValueSet(short[] data, int index, short oldValue, short newValue) {
        if (oldValue == newValue) {
            return;
        }
        if (oldValue == tombstone) {
            onValueAdded(index, newValue);
            return;
        }
        if (newValue == tombstone) {
            onValueRemoved(data, index, oldValue);
            return;
        }
        onValueChanged(data, index, oldValue, newValue);
    }

    private void onValueAdded(int index, short value) {
        sum += value;
        count++;

        if (firstIndex == -1 || index < firstIndex) {
            firstIndex = index;
            firstValue = value;
        }
        if (lastIndex == -1 || index > lastIndex) {
            lastIndex = index;
            lastValue = value;
        }

        min = min < value ? min : value;
        max = max > value ? max : value;
    }

    private void onValueRemoved(short[] data, int index, short value) {
        sum -= value;
        count--;

        if (firstIndex == index && lastIndex == index) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == index) {
            firstIndex = ShortStatisticUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == index) {
            lastIndex = ShortStatisticUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }

        if (min == value) {
            if (firstIndex == -1) {
                min = Short.MAX_VALUE;
            } else {
                min = ShortStatisticUtils.min(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
        if (max == value) {
            if (firstIndex == -1) {
                max = Short.MIN_VALUE;
            } else {
                max = ShortStatisticUtils.max(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
    }

    private void onValueChanged(short[] data, int index, short oldValue, short newValue) {
        sum += newValue - oldValue;

        if (index == firstIndex) {
            firstValue = newValue;
        }
        if (index == lastIndex) {
            lastValue = newValue;
        }
        if (newValue < min) {
            min = newValue;
        } else if (min == oldValue) {
            min = ShortStatisticUtils.min(data, tombstone, firstIndex, lastIndex + 1);
        }
        if (newValue > max) {
            max = newValue;
        } else if (max == oldValue) {
            max = ShortStatisticUtils.max(data, tombstone, firstIndex, lastIndex + 1);
        }
    }

    public short getFirstValue() {
        return firstValue;
    }

    public int getFirstIndex() {
        return firstIndex;
    }

    public short getLastValue() {
        return lastValue;
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public short getMin() {
        return min;
    }

    public short getMax() {
        return max;
    }

    public long getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    public static Builder builder(short tombstone) {
        return new Builder(tombstone);
    }

    public static class Builder {
        private final ShortStatistic statistic;

        public Builder(short tombstone) {
            this.statistic = new ShortStatistic(tombstone);
        }

        public Builder firstValue(short value) {
            this.statistic.firstValue = value;
            return this;
        }

        public Builder firstIndex(int value) {
            this.statistic.firstIndex = value;
            return this;
        }

        public Builder lastValue(short value) {
            this.statistic.lastValue = value;
            return this;
        }

        public Builder lastIndex(int value) {
            this.statistic.lastIndex = value;
            return this;
        }

        public Builder min(short value) {
            this.statistic.min = value;
            return this;
        }

        public Builder max(short value) {
            this.statistic.max = value;
            return this;
        }

        public Builder sum(long value) {
            this.statistic.sum = value;
            return this;
        }

        public Builder count(int value) {
            this.statistic.count = value;
            return this;
        }

        public ShortStatistic build() {
            return statistic;
        }
    }
}
