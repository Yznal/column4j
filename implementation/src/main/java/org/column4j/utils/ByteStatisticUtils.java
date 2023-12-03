package org.column4j.utils;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.column4j.mutable.aggregated.statistic.ByteStatistic;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public final class ByteStatisticUtils {

    public static final VectorSpecies<Byte> SPECIES_PREFERRED = ByteVector.SPECIES_PREFERRED;
    public static final int SPECIES_LENGTH = SPECIES_PREFERRED.length();

    private ByteStatisticUtils() {
    }

    /**
     * Get maximum value in array in passed bounds using VectorAPI
     *
     * @param data      source byte array
     * @param tombstone ignored value
     * @param from      from index (inclusive)
     * @param to        to index (exclusive)
     * @return maximum on array or {@link Byte#MIN_VALUE}
     */
    public static byte max(byte[] data, byte tombstone, int from, int to) {
        var maxVector = ByteVector.broadcast(SPECIES_PREFERRED, Byte.MIN_VALUE);
        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextVector = ByteVector.fromArray(SPECIES_PREFERRED, data, from);
            var tombstoneMask = nextVector.eq(tombstone).not();
            maxVector = maxVector.lanewise(VectorOperators.MAX, nextVector, tombstoneMask);
        }
        var max = maxVector.reduceLanes(VectorOperators.MAX);
        // tail
        for (; from < to; from++) {
            var value = data[from];
            if (value != tombstone) {
                max = max > value ? max : value;
            }
        }
        return max;
    }

    /**
     * Get minimum value in array in passed bounds using VectorAPI
     *
     * @param data      source byte array
     * @param tombstone ignored value
     * @param from      from index (inclusive)
     * @param to        to index (exclusive)
     * @return minimum on array or {@link Byte#MAX_VALUE}
     */
    public static byte min(byte[] data, byte tombstone, int from, int to) {
        var minVector = ByteVector.broadcast(SPECIES_PREFERRED, Byte.MAX_VALUE);
        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextVector = ByteVector.fromArray(SPECIES_PREFERRED, data, from);
            var tombstoneMask = nextVector.eq(tombstone).not();
            minVector = minVector.lanewise(VectorOperators.MIN, nextVector, tombstoneMask);
        }
        var min = minVector.reduceLanes(VectorOperators.MIN);
        // tail
        for (; from < to; from++) {
            var value = data[from];
            if (value != tombstone) {
                min = min < value ? min : value;
            }
        }
        return min;
    }

    /**
     * Collect statistics of byte array
     *
     * @param data      source byte array
     * @param dataSize  right array bound (exclusive)
     * @param tombstone ignored value
     * @param from      left statistic bound (inclusive)
     * @param to        right statistic bound (exclusive)
     * @return byte array statistics
     */
    public static ByteStatistic collectStatistic(byte[] data, int dataSize, byte tombstone, int from, int to) {
        to = Math.min(to, dataSize);

        int firstIndex = -1;
        int lastIndex = -1;
        int count = 0;
        long sum = 0;

        var minVector = ByteVector.broadcast(SPECIES_PREFERRED, Byte.MAX_VALUE);
        var maxVector = ByteVector.broadcast(SPECIES_PREFERRED, Byte.MIN_VALUE);

        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextVector = ByteVector.fromArray(SPECIES_PREFERRED, data, from);
            var tombstoneMask = nextVector.eq(tombstone).not();
            minVector = minVector.lanewise(VectorOperators.MIN, nextVector, tombstoneMask);
            maxVector = maxVector.lanewise(VectorOperators.MAX, nextVector, tombstoneMask);
            // XXX: Overflow
            sum += nextVector.reduceLanesToLong(VectorOperators.ADD, tombstoneMask);
            count += tombstoneMask.trueCount();
            if (tombstoneMask.anyTrue()) {
                if (firstIndex == -1) {
                    firstIndex = from + tombstoneMask.firstTrue();
                }
                lastIndex = from + tombstoneMask.lastTrue();
            }
        }

        var min = minVector.reduceLanes(VectorOperators.MIN);
        var max = maxVector.reduceLanes(VectorOperators.MAX);

        // tail
        for (; from < to; from++) {
            var value = data[from];
            if (value == tombstone) {
                continue;
            }
            max = max > value ? max : value;
            min = min < value ? min : value;
            sum += value;
            count++;
            if (firstIndex == -1) {
                firstIndex = from;
            }
            lastIndex = from;
        }

        return ByteStatistic.builder(tombstone)
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
     * @param data  source byte array
     * @param value value for filter
     * @param from  left bound (inclusive)
     * @param to    right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int indexOfAnother(byte[] data, byte value, int from, int to) {
        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextVector = ByteVector.fromArray(SPECIES_PREFERRED, data, from);
            var valueMask = nextVector.eq(value).not();
            if (valueMask.anyTrue()) {
                return from + valueMask.firstTrue();
            }
        }
        // tail
        for (; from < to; from++) {
            if (value != data[from]) {
                return from;
            }
        }
        return -1;
    }

    /**
     * Find index of last element what not equals to passed value
     *
     * @param data  source byte array
     * @param value value for filter
     * @param from  left bound (inclusive)
     * @param to    right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int lastIndexOfAnother(byte[] data, byte value, int from, int to) {
        for (; to > from && from + SPECIES_LENGTH <= to; to -= SPECIES_LENGTH) {
            var nextVector = ByteVector.fromArray(SPECIES_PREFERRED, data, to - SPECIES_LENGTH);
            var valueMask = nextVector.eq(value).not();
            if (valueMask.anyTrue()) {
                return to - SPECIES_LENGTH + valueMask.lastTrue();
            }
        }
        if(to == data.length) {
            to--;
        }
        // tail
        for (; to >= from; to--) {
            if (value != data[to]) {
                return to;
            }
        }
        return -1;
    }
}
