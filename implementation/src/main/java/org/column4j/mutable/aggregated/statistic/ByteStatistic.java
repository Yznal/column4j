package org.column4j.mutable.aggregated.statistic;

import org.column4j.utils.ByteStatisticUtils;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class ByteStatistic {

    private final byte tombstone;

    private byte firstValue;
    private int firstIndex;
    private byte lastValue;
    private int lastIndex;
    private byte min;
    private byte max;
    private long sum;
    private int count;

    public ByteStatistic(byte tombstone) {
        this.tombstone = tombstone;
        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
        this.min = Byte.MAX_VALUE;
        this.max = Byte.MIN_VALUE;
    }

    /**
     * Update statistics on value change
     * @param data source array
     * @param index index of changed element
     * @param oldValue old element value
     * @param newValue new element value
     */
    public void onValueSet(byte[] data, int index, byte oldValue, byte newValue) {
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

    private void onValueAdded(int index, byte value) {
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

    private void onValueRemoved(byte[] data, int index, byte value) {
        sum -= value;
        count--;

        if (firstIndex == index && lastIndex == index) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == index) {
            firstIndex = ByteStatisticUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == index) {
            lastIndex = ByteStatisticUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }

        if (min == value) {
            if (firstIndex == -1) {
                min = Byte.MAX_VALUE;
            } else {
                min = ByteStatisticUtils.min(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
        if (max == value) {
            if (firstIndex == -1) {
                max = Byte.MIN_VALUE;
            } else {
                max = ByteStatisticUtils.max(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
    }

    private void onValueChanged(byte[] data, int index, byte oldValue, byte newValue) {
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
            min = ByteStatisticUtils.min(data, tombstone, firstIndex, lastIndex + 1);
        }
        if (newValue > max) {
            max = newValue;
        } else if (max == oldValue) {
            max = ByteStatisticUtils.max(data, tombstone, firstIndex, lastIndex + 1);
        }
    }

    public byte getFirstValue() {
        return firstValue;
    }

    public int getFirstIndex() {
        return firstIndex;
    }

    public byte getLastValue() {
        return lastValue;
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public byte getMin() {
        return min;
    }

    public byte getMax() {
        return max;
    }

    public long getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    public static Builder builder(byte tombstone) {
        return new Builder(tombstone);
    }

    public static class Builder {
        private final ByteStatistic statistic;

        public Builder(byte tombstone) {
            this.statistic = new ByteStatistic(tombstone);
        }

        public Builder firstValue(byte value) {
            this.statistic.firstValue = value;
            return this;
        }

        public Builder firstIndex(int value) {
            this.statistic.firstIndex = value;
            return this;
        }

        public Builder lastValue(byte value) {
            this.statistic.lastValue = value;
            return this;
        }

        public Builder lastIndex(int value) {
            this.statistic.lastIndex = value;
            return this;
        }

        public Builder min(byte value) {
            this.statistic.min = value;
            return this;
        }

        public Builder max(byte value) {
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

        public ByteStatistic build() {
            return statistic;
        }
    }
}
