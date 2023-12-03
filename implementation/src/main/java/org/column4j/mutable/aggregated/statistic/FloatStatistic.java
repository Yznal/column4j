package org.column4j.mutable.aggregated.statistic;

import org.column4j.utils.FloatStatisticUtils;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class FloatStatistic {

    private final float tombstone;

    private float firstValue;
    private int firstIndex;
    private float lastValue;
    private int lastIndex;
    private float min;
    private float max;
    private double sum;
    private int count;

    public FloatStatistic(float tombstone) {
        this.tombstone = tombstone;
        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
        this.min = Float.MAX_VALUE;
        this.max = Float.MIN_VALUE;
    }

    /**
     * Update statistics on value change
     * @param data source array
     * @param index index of changed element
     * @param oldValue old element value
     * @param newValue new element value
     */
    public void onValueSet(float[] data, int index, float oldValue, float newValue) {
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

    private void onValueAdded(int index, float value) {
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

    private void onValueRemoved(float[] data, int index, float value) {
        sum -= value;
        count--;

        if (firstIndex == index && lastIndex == index) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == index) {
            firstIndex = FloatStatisticUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == index) {
            lastIndex = FloatStatisticUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }

        if (min == value) {
            if (firstIndex == -1) {
                min = Float.MAX_VALUE;
            } else {
                min = FloatStatisticUtils.min(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
        if (max == value) {
            if (firstIndex == -1) {
                max = Float.MIN_VALUE;
            } else {
                max = FloatStatisticUtils.max(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
    }

    private void onValueChanged(float[] data, int index, float oldValue, float newValue) {
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
            min = FloatStatisticUtils.min(data, tombstone, firstIndex, lastIndex + 1);
        }
        if (newValue > max) {
            max = newValue;
        } else if (max == oldValue) {
            max = FloatStatisticUtils.max(data, tombstone, firstIndex, lastIndex + 1);
        }
    }

    public float getFirstValue() {
        return firstValue;
    }

    public int getFirstIndex() {
        return firstIndex;
    }

    public float getLastValue() {
        return lastValue;
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public float getMin() {
        return min;
    }

    public float getMax() {
        return max;
    }

    public double getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    public static Builder builder(float tombstone) {
        return new Builder(tombstone);
    }

    public static class Builder {
        private final FloatStatistic statistic;

        public Builder(float tombstone) {
            this.statistic = new FloatStatistic(tombstone);
        }

        public Builder firstValue(float value) {
            this.statistic.firstValue = value;
            return this;
        }

        public Builder firstIndex(int value) {
            this.statistic.firstIndex = value;
            return this;
        }

        public Builder lastValue(float value) {
            this.statistic.lastValue = value;
            return this;
        }

        public Builder lastIndex(int value) {
            this.statistic.lastIndex = value;
            return this;
        }

        public Builder min(float value) {
            this.statistic.min = value;
            return this;
        }

        public Builder max(float value) {
            this.statistic.max = value;
            return this;
        }

        public Builder sum(double value) {
            this.statistic.sum = value;
            return this;
        }

        public Builder count(int value) {
            this.statistic.count = value;
            return this;
        }

        public FloatStatistic build() {
            return statistic;
        }
    }
}
