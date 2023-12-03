package org.column4j.mutable.aggregated.statistic;

import org.column4j.utils.StringStatisticUtils;

import java.util.Objects;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class StringStatistic {

    private final String tombstone;

    private String firstValue;
    private int firstIndex;
    private String lastValue;
    private int lastIndex;
    private int count;

    public StringStatistic(String tombstone) {
        this.tombstone = tombstone;
        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
    }

    /**
     * Update statistics on value change
     * @param data source array
     * @param index index of changed element
     * @param oldValue old element value
     * @param newValue new element value
     */
    public void onValueSet(String[] data, int index, String oldValue, String newValue) {
        if (Objects.equals(oldValue, newValue)) {
            return;
        }
        if (Objects.equals(oldValue, tombstone)) {
            onValueAdded(index, newValue);
            return;
        }
        if (Objects.equals(newValue, tombstone)) {
            onValueRemoved(data, index);
            return;
        }
        onValueChanged(index, newValue);
    }

    private void onValueAdded(int index, String value) {
        count++;

        if (firstIndex == -1 || index < firstIndex) {
            firstIndex = index;
            firstValue = value;
        }
        if (lastIndex == -1 || index > lastIndex) {
            lastIndex = index;
            lastValue = value;
        }
    }

    private void onValueRemoved(String[] data, int index) {
        count--;

        if (firstIndex == index && lastIndex == index) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == index) {
            firstIndex = StringStatisticUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == index) {
            lastIndex = StringStatisticUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }
    }

    private void onValueChanged(int index, String newValue) {
        if (index == firstIndex) {
            firstValue = newValue;
        }
        if (index == lastIndex) {
            lastValue = newValue;
        }
    }

    public String getFirstValue() {
        return firstValue;
    }

    public int getFirstIndex() {
        return firstIndex;
    }

    public String getLastValue() {
        return lastValue;
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public int getCount() {
        return count;
    }

    public static Builder builder(String tombstone) {
        return new Builder(tombstone);
    }

    public static class Builder {
        private final StringStatistic statistic;

        public Builder(String tombstone) {
            this.statistic = new StringStatistic(tombstone);
        }

        public Builder firstValue(String value) {
            this.statistic.firstValue = value;
            return this;
        }

        public Builder firstIndex(int value) {
            this.statistic.firstIndex = value;
            return this;
        }

        public Builder lastValue(String value) {
            this.statistic.lastValue = value;
            return this;
        }

        public Builder lastIndex(int value) {
            this.statistic.lastIndex = value;
            return this;
        }

        public Builder count(int value) {
            this.statistic.count = value;
            return this;
        }

        public StringStatistic build() {
            return statistic;
        }
    }
}
