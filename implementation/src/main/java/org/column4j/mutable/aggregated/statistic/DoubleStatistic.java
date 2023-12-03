package org.column4j.mutable.aggregated.statistic;

import org.column4j.utils.DoubleStatisticUtils;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class DoubleStatistic {

    private final double tombstone;

    private double firstValue;
    private int firstIndex;
    private double lastValue;
    private int lastIndex;
    private double min;
    private double max;
    private double sum;
    private int count;

    public DoubleStatistic(double tombstone) {
        this.tombstone = tombstone;
        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
        this.min = Double.MAX_VALUE;
        this.max = Double.MIN_VALUE;
    }

    /**
     * Update statistics on value change
     * @param data source array
     * @param index index of changed element
     * @param oldValue old element value
     * @param newValue new element value
     */
    public void onValueSet(double[] data, int index, double oldValue, double newValue) {
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

    private void onValueAdded(int index, double value) {
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

    private void onValueRemoved(double[] data, int index, double value) {
        sum -= value;
        count--;

        if (firstIndex == index && lastIndex == index) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == index) {
            firstIndex = DoubleStatisticUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == index) {
            lastIndex = DoubleStatisticUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }

        if (min == value) {
            if (firstIndex == -1) {
                min = Double.MAX_VALUE;
            } else {
                min = DoubleStatisticUtils.min(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
        if (max == value) {
            if (firstIndex == -1) {
                max = Double.MIN_VALUE;
            } else {
                max = DoubleStatisticUtils.max(data, tombstone, firstIndex, lastIndex + 1);
            }
        }
    }

    private void onValueChanged(double[] data, int index, double oldValue, double newValue) {
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
            min = DoubleStatisticUtils.min(data, tombstone, firstIndex, lastIndex + 1);
        }
        if (newValue > max) {
            max = newValue;
        } else if (max == oldValue) {
            max = DoubleStatisticUtils.max(data, tombstone, firstIndex, lastIndex + 1);
        }
    }

    public double getFirstValue() {
        return firstValue;
    }

    public int getFirstIndex() {
        return firstIndex;
    }

    public double getLastValue() {
        return lastValue;
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    public static Builder builder(double tombstone) {
        return new Builder(tombstone);
    }

    public static class Builder {
        private final DoubleStatistic statistic;

        public Builder(double tombstone) {
            this.statistic = new DoubleStatistic(tombstone);
        }

        public Builder firstValue(double value) {
            this.statistic.firstValue = value;
            return this;
        }

        public Builder firstIndex(int value) {
            this.statistic.firstIndex = value;
            return this;
        }

        public Builder lastValue(double value) {
            this.statistic.lastValue = value;
            return this;
        }

        public Builder lastIndex(int value) {
            this.statistic.lastIndex = value;
            return this;
        }

        public Builder min(double value) {
            this.statistic.min = value;
            return this;
        }

        public Builder max(double value) {
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

        public DoubleStatistic build() {
            return statistic;
        }
    }
}
