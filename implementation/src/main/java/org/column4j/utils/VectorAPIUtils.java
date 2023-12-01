package org.column4j.utils;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.column4j.mutable.aggregated.IntStatistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public final class VectorAPIUtils {

    public static final VectorSpecies<Integer> SPECIES_PREFERRED = IntVector.SPECIES_PREFERRED;
    public static final int SPECIES_LENGTH = SPECIES_PREFERRED.length();

    private VectorAPIUtils() {
    }

    /**
     * Get maximum value in array in passed bounds using VectorAPI
     *
     * @param data source array
     * @param tombstone ignored value
     * @param from from index (inclusive)
     * @param to to index (exclusive)
     * @return maximum on array or {@link Integer#MIN_VALUE}
     */
    public static int max(int[] data, int tombstone, int from, int to) {
        var intVector = IntVector.broadcast(SPECIES_PREFERRED, Integer.MIN_VALUE);
        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextIntVector = IntVector.fromArray(SPECIES_PREFERRED, data, from);
            var tombstoneMask = nextIntVector.eq(tombstone).not();
            intVector = intVector.lanewise(VectorOperators.MAX, nextIntVector, tombstoneMask);
        }
        var max = intVector.reduceLanes(VectorOperators.MAX);
        // tail
        for (; from < to; from++) {
            var value = data[from];
            max = value == tombstone ? max : Math.max(max, value);
        }
        return max;
    }

    /**
     * Get minimum value in array in passed bounds using VectorAPI
     *
     * @param data source array
     * @param tombstone ignored value
     * @param from from index (inclusive)
     * @param to to index (exclusive)
     * @return minimum on array or {@link Integer#MAX_VALUE}
     */
    public static int min(int[] data, int tombstone, int from, int to) {
        var intVector = IntVector.broadcast(SPECIES_PREFERRED, Integer.MAX_VALUE);
        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextIntVector = IntVector.fromArray(SPECIES_PREFERRED, data, from);
            var tombstoneMask = nextIntVector.eq(tombstone).not();
            intVector = intVector.lanewise(VectorOperators.MIN, nextIntVector, tombstoneMask);
        }
        var min = intVector.reduceLanes(VectorOperators.MIN);
        // tail
        for (; from < to; from++) {
            var value = data[from];
            min = value == tombstone ? min : Math.min(min, value);
        }
        return min;
    }

    /**
     * Collect statistics of integer array
     *
     * @param data source int array
     * @param dataSize right array bound (exclusive)
     * @param tombstone ignored value
     * @param from left statistic bound (inclusive)
     * @param to right statistic bound (exclusive)
     * @return integer array statistics
     */
    public static IntStatistic collectIntStats(int[] data, int dataSize, int tombstone, int from, int to) {
        to = Math.min(to, dataSize);

        int firstIndex = -1;
        int lastIndex = -1;
        int count = 0;

        var minIntVector = IntVector.broadcast(SPECIES_PREFERRED, Integer.MAX_VALUE);
        var maxIntVector = IntVector.broadcast(SPECIES_PREFERRED, Integer.MIN_VALUE);
        var sumIntVector = IntVector.zero(SPECIES_PREFERRED);

        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextIntVector = IntVector.fromArray(SPECIES_PREFERRED, data, from);
            var tombstoneMask = nextIntVector.eq(tombstone).not();
            minIntVector = minIntVector.lanewise(VectorOperators.MIN, nextIntVector, tombstoneMask);
            maxIntVector = maxIntVector.lanewise(VectorOperators.MAX, nextIntVector, tombstoneMask);
            sumIntVector = sumIntVector.lanewise(VectorOperators.ADD, nextIntVector, tombstoneMask);
            count += tombstoneMask.trueCount();
            if (tombstoneMask.anyTrue()) {
                if (firstIndex == -1) {
                    firstIndex = from + tombstoneMask.firstTrue();
                }
                lastIndex = from + tombstoneMask.lastTrue();
            }
        }

        var min = minIntVector.reduceLanes(VectorOperators.MIN);
        var max = maxIntVector.reduceLanes(VectorOperators.MAX);
        var sum = sumIntVector.reduceLanes(VectorOperators.ADD);

        // tail
        for (; from < to; from++) {
            var value = data[from];
            if (value == tombstone) {
                continue;
            }
            max = Math.max(max, value);
            min = Math.min(min, value);
            sum += value;
            count++;
            if (firstIndex == -1) {
                firstIndex = from;
            }
            lastIndex = from;
        }

        return IntStatistic.builder(tombstone)
                .firstValue(firstIndex == -1 ? tombstone : data[firstIndex])
                .firstIndex(firstIndex)
                .lastValue(lastIndex == -1 ? tombstone : data[lastIndex])
                .lastIndex(lastIndex)
                .max(max)
                .min(min)
                .sum(sum)
                .count(count)
                .build();
    }

    /**
     * Find index of first element what not equals to passed value
     *
     * @param data array for search
     * @param value value for filter
     * @param from left bound (inclusive)
     * @param to right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int indexOfAnother(int[] data, int value, int from, int to) {
        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var intVector = IntVector.fromArray(SPECIES_PREFERRED, data, from);
            var valueMask = intVector.eq(value).not();
            if(valueMask.anyTrue()) {
                return from + valueMask.firstTrue();
            }
        }
        // tail
        for (; from < to; from++) {
            if(value != data[from]) {
                return from;
            }
        }
        return -1;
    }

    /**
     * Find index of last element what not equals to passed value
     *
     * @param data array for search
     * @param value value for filter
     * @param from left bound (inclusive)
     * @param to right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int lastIndexOfAnother(int[] data, int value, int from, int to) {
        for (; to > from && from + SPECIES_LENGTH <= to; to -= SPECIES_LENGTH) {
            var intVector = IntVector.fromArray(SPECIES_PREFERRED, data, to - SPECIES_LENGTH);
            var valueMask = intVector.eq(value).not();
            if(valueMask.anyTrue()) {
                return to - SPECIES_LENGTH + valueMask.lastTrue();
            }
        }
        // tail
        for (; to >= from; to--) {
            if(value != data[to]) {
                return to;
            }
        }
        return -1;
    }
}
