package org.column4j.mutable.aggregated;

import org.column4j.utils.VectorAPIUtils;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class IntStatistic {

    private final int tombstone;

    private int firstValue;
    private int firstIndex;
    private int lastValue;
    private int lastIndex;
    private int min;
    private int max;
    private long sum;
    private int count;

    public IntStatistic(int tombstone) {
        this.tombstone = tombstone;
        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
        this.min = Integer.MAX_VALUE;
        this.max = Integer.MIN_VALUE;
    }

    public void onValueSet(int[] data, int index, int oldValue, int newValue) {
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

    private void onValueAdded(int index, int value) {
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

    private void onValueRemoved(int[] data, int index, int value) {
        sum -= value;
        count--;

        if (firstIndex == index && lastIndex == index) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == index) {
            firstIndex = VectorAPIUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == index) {
            lastIndex = VectorAPIUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }

        if (min == value) {
            if (firstIndex == -1) {
                min = Integer.MAX_VALUE;
            } else {
                min = VectorAPIUtils.min(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
        if (max == value) {
            if (firstIndex == -1) {
                max = Integer.MIN_VALUE;
            } else {
                max = VectorAPIUtils.max(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
    }

    private void onValueChanged(int[] data, int index, int oldValue, int newValue) {
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
            min = VectorAPIUtils.min(data, tombstone, firstIndex, lastIndex + 1);
        }
        if (newValue > max) {
            max = newValue;
        } else if (max == oldValue) {
            max = VectorAPIUtils.max(data, tombstone, firstIndex, lastIndex + 1);
        }
    }

    public int getFirstValue() {
        return firstValue;
    }

    public int getFirstIndex() {
        return firstIndex;
    }

    public int getLastValue() {
        return lastValue;
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

    public long getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    public static Builder builder(int tombstone) {
        return new Builder(tombstone);
    }

    public static class Builder {
        private final IntStatistic intStats;

        public Builder(int tombstone) {
            this.intStats = new IntStatistic(tombstone);
        }

        public Builder firstValue(int value) {
            this.intStats.firstValue = value;
            return this;
        }

        public Builder firstIndex(int value) {
            this.intStats.firstIndex = value;
            return this;
        }

        public Builder lastValue(int value) {
            this.intStats.lastValue = value;
            return this;
        }

        public Builder lastIndex(int value) {
            this.intStats.lastIndex = value;
            return this;
        }

        public Builder min(int value) {
            this.intStats.min = value;
            return this;
        }

        public Builder max(int value) {
            this.intStats.max = value;
            return this;
        }

        public Builder sum(int value) {
            this.intStats.sum = value;
            return this;
        }

        public Builder count(int value) {
            this.intStats.count = value;
            return this;
        }

        public IntStatistic build() {
            return intStats;
        }
    }
}
