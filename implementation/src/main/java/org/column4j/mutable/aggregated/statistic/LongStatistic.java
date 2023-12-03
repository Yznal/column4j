package org.column4j.mutable.aggregated.statistic;

import org.column4j.utils.LongStatisticUtils;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class LongStatistic {

    private final long tombstone;

    private long firstValue;
    private int firstIndex;
    private long lastValue;
    private int lastIndex;
    private long min;
    private long max;
    private long sum;
    private int count;

    public LongStatistic(long tombstone) {
        this.tombstone = tombstone;
        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
        this.min = Long.MAX_VALUE;
        this.max = Long.MIN_VALUE;
    }

    /**
     * Update statistics on value change
     * @param data source array
     * @param index index of changed element
     * @param oldValue old element value
     * @param newValue new element value
     */
    public void onValueSet(long[] data, int index, long oldValue, long newValue) {
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

    private void onValueAdded(int index, long value) {
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

        min = Math.min(min, value);
        max = Math.max(max, value);
    }

    private void onValueRemoved(long[] data, int index, long value) {
        sum -= value;
        count--;

        if (firstIndex == index && lastIndex == index) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == index) {
            firstIndex = LongStatisticUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == index) {
            lastIndex = LongStatisticUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }

        if (min == value) {
            if (firstIndex == -1) {
                min = Long.MAX_VALUE;
            } else {
                min = LongStatisticUtils.min(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
        if (max == value) {
            if (firstIndex == -1) {
                max = Long.MIN_VALUE;
            } else {
                max = LongStatisticUtils.max(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
    }

    private void onValueChanged(long[] data, int index, long oldValue, long newValue) {
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
            min = LongStatisticUtils.min(data, tombstone, firstIndex, lastIndex + 1);
        }
        if (newValue > max) {
            max = newValue;
        } else if (max == oldValue) {
            max = LongStatisticUtils.max(data, tombstone, firstIndex, lastIndex + 1);
        }
    }

    public long getFirstValue() {
        return firstValue;
    }

    public int getFirstIndex() {
        return firstIndex;
    }

    public long getLastValue() {
        return lastValue;
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public long getMin() {
        return min;
    }

    public long getMax() {
        return max;
    }

    public long getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    public static Builder builder(long tombstone) {
        return new Builder(tombstone);
    }

    public static class Builder {
        private final LongStatistic statistic;

        public Builder(long tombstone) {
            this.statistic = new LongStatistic(tombstone);
        }

        public Builder firstValue(long value) {
            this.statistic.firstValue = value;
            return this;
        }

        public Builder firstIndex(int value) {
            this.statistic.firstIndex = value;
            return this;
        }

        public Builder lastValue(long value) {
            this.statistic.lastValue = value;
            return this;
        }

        public Builder lastIndex(int value) {
            this.statistic.lastIndex = value;
            return this;
        }

        public Builder min(long value) {
            this.statistic.min = value;
            return this;
        }

        public Builder max(long value) {
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

        public LongStatistic build() {
            return statistic;
        }
    }
}
