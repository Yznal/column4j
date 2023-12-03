package org.column4j.utils;

import org.column4j.mutable.aggregated.statistic.StringStatistic;

import java.util.Objects;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public final class StringStatisticUtils {


    private StringStatisticUtils() {
    }

    /**
     * Collect statistics of String array
     *
     * @param data      source String array
     * @param dataSize  right array bound (exclusive)
     * @param tombstone ignored value
     * @param from      left statistic bound (inclusive)
     * @param to        right statistic bound (exclusive)
     * @return String array statistics
     */
    public static StringStatistic collectStatistic(String[] data, int dataSize, String tombstone, int from, int to) {
        to = Math.min(to, dataSize);

        int firstIndex = -1;
        int lastIndex = -1;
        int count = 0;

        for (; from < to; from++) {
            var value = data[from];
            if (Objects.equals(value, tombstone)) {
                continue;
            }
            count++;
            if (firstIndex == -1) {
                firstIndex = from;
            }
            lastIndex = from;
        }

        return StringStatistic.builder(tombstone)
                .firstValue(firstIndex == -1 ? tombstone : data[firstIndex])
                .firstIndex(firstIndex)
                .lastValue(lastIndex == -1 ? tombstone : data[lastIndex])
                .lastIndex(lastIndex)
                .count(count)
                .build();
    }

    /**
     * Find index of first element what not equals to passed value
     *
     * @param data  source String array
     * @param value value for filter
     * @param from  left bound (inclusive)
     * @param to    right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int indexOfAnother(String[] data, String value, int from, int to) {
        // tail
        for (; from < to; from++) {
            if (!Objects.equals(value, data[from])) {
                return from;
            }
        }
        return -1;
    }

    /**
     * Find index of last element what not equals to passed value
     *
     * @param data  source String array
     * @param value value for filter
     * @param from  left bound (inclusive)
     * @param to    right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int lastIndexOfAnother(String[] data, String value, int from, int to) {
        if(to == data.length) {
            to--;
        }
        // tail
        for (; to >= from; to--) {
            if (!Objects.equals(value, data[to])) {
                return to;
            }
        }
        return -1;
    }
}
