package org.column4j.column.impl.statistic;

import org.column4j.column.statistic.StringStatistic;
import org.column4j.utils.StringUtils;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class StringStatisticImpl implements StringStatistic {
    private final String[] data;
    private final String tombstone;

    private String firstValue;
    private int firstIndex;
    private String lastValue;
    private int lastIndex;
    private int count;

    public StringStatisticImpl(String[] data, String tombstone) {
        this.data = data;
        this.tombstone = tombstone;

        this.firstIndex = -1;
        this.firstValue = tombstone;
        this.lastIndex = -1;
        this.lastValue = tombstone;
    }

    @Override
    public void onValueChanged(int position, String oldValue, String newValue) {
        if (position == firstIndex) {
            firstValue = newValue;
        }
        if (position == lastIndex) {
            lastValue = newValue;
        }
    }

    @Override
    public void onValueAdded(int position, String newValue) {
        count++;

        if (firstIndex == -1 || position < firstIndex) {
            firstIndex = position;
            firstValue = newValue;
        }
        if (lastIndex == -1 || position > lastIndex) {
            lastIndex = position;
            lastValue = newValue;
        }
    }

    @Override
    public void onValueRemoved(int position, String oldValue) {
        count--;

        if (firstIndex == position && lastIndex == position) {
            firstIndex = -1;
            firstValue = tombstone;
            lastIndex = -1;
            lastValue = tombstone;
        } else if (firstIndex == position) {
            var data = this.data;
            firstIndex = StringUtils.indexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            firstValue = data[firstIndex];
        } else if (lastIndex == position) {
            var data = this.data;
            lastIndex = StringUtils.lastIndexOfAnother(data, tombstone, firstIndex, lastIndex + 1);
            lastValue = data[lastIndex];
        }
    }

    @Override
    public String getFirstValue() {
        return firstValue;
    }

    @Override
    public String getLastValue() {
        return lastValue;
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

}
