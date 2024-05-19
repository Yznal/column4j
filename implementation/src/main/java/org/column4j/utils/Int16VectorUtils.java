package org.column4j.utils;

import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public final class Int16VectorUtils {

    public static final VectorSpecies<Short> SPECIES_PREFERRED = ShortVector.SPECIES_PREFERRED;
    public static final int SPECIES_LENGTH = SPECIES_PREFERRED.length();

    private Int16VectorUtils() {
    }

    /**
     * Get maximum value in array in passed bounds using VectorAPI
     *
     * @param data      source short array
     * @param tombstone ignored value
     * @param from      from index (inclusive)
     * @param to        to index (exclusive)
     * @return maximum on array or {@link Short#MIN_VALUE}
     */
    public static short max(short[] data, short tombstone, int from, int to) {
        var maxVector = ShortVector.broadcast(SPECIES_PREFERRED, Short.MIN_VALUE);
        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextVector = ShortVector.fromArray(SPECIES_PREFERRED, data, from);
            var tombstoneMask = nextVector.eq(tombstone).not();
            maxVector = maxVector.lanewise(VectorOperators.MAX, nextVector, tombstoneMask);
        }
        var max = maxVector.reduceLanes(VectorOperators.MAX);
        // tail
        for (; from < to; from++) {
            var value = data[from];
            if(max != value) {
                max = max < value ? value : max;
            }
        }
        return max;
    }

    /**
     * Get minimum value in array in passed bounds using VectorAPI
     *
     * @param data      source array
     * @param tombstone ignored value
     * @param from      from index (inclusive)
     * @param to        to index (exclusive)
     * @return minimum on array or {@link Short#MAX_VALUE}
     */
    public static short min(short[] data, short tombstone, int from, int to) {
        var minVector = ShortVector.broadcast(SPECIES_PREFERRED, Short.MAX_VALUE);
        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextVector = ShortVector.fromArray(SPECIES_PREFERRED, data, from);
            var tombstoneMask = nextVector.eq(tombstone).not();
            minVector = minVector.lanewise(VectorOperators.MIN, nextVector, tombstoneMask);
        }
        var min = minVector.reduceLanes(VectorOperators.MIN);
        // tail
        for (; from < to; from++) {
            var value = data[from];
            if(min != value) {
                min = min > value ? value : min;
            }
        }
        return min;
    }

    /**
     * Find index of first element what not equals to passed value
     *
     * @param data  source short array
     * @param value value for filter
     * @param from  left bound (inclusive)
     * @param to    right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int indexOfAnother(short[] data, short value, int from, int to) {
        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextVector = ShortVector.fromArray(SPECIES_PREFERRED, data, from);
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
     * @param data  source short array
     * @param value value for filter
     * @param from  left bound (inclusive)
     * @param to    right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int lastIndexOfAnother(short[] data, short value, int from, int to) {
        to--;

        for (; to >= from && from + SPECIES_LENGTH < to; to -= SPECIES_LENGTH) {
            var nextVector = ShortVector.fromArray(SPECIES_PREFERRED, data, to + 1 - SPECIES_LENGTH);
            var valueMask = nextVector.eq(value).not();
            if (valueMask.anyTrue()) {
                return to + 1 - SPECIES_LENGTH + valueMask.lastTrue();
            }
        }

        // tail
        for (; to >= from; to--) {
            if (value != data[to]) {
                return to;
            }
        }
        return -1;
    }

    /**
     * Find index of first element what equals to passed value
     *
     * @param data  source short array
     * @param value value for filter
     * @param from  left bound (inclusive)
     * @param to    right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int indexOf(short[] data, short value, int from, int to) {
        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextVector = ShortVector.fromArray(SPECIES_PREFERRED, data, from);
            var valueMask = nextVector.eq(value);
            if (valueMask.anyTrue()) {
                return from + valueMask.firstTrue();
            }
        }
        // tail
        for (; from < to; from++) {
            if (value == data[from]) {
                return from;
            }
        }
        return -1;
    }

    /**
     * Find index of last element what equals to passed value
     *
     * @param data  source short array
     * @param value value for filter
     * @param from  left bound (inclusive)
     * @param to    right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int lastIndexOf(short[] data, short value, int from, int to) {
        to--;

        for (; to >= from && from + SPECIES_LENGTH < to; to -= SPECIES_LENGTH) {
            var nextVector = ShortVector.fromArray(SPECIES_PREFERRED, data, to + 1 - SPECIES_LENGTH);
            var valueMask = nextVector.eq(value);
            if (valueMask.anyTrue()) {
                return to + 1 - SPECIES_LENGTH + valueMask.lastTrue();
            }
        }

        // tail
        for (; to >= from; to--) {
            if (value == data[to]) {
                return to;
            }
        }
        return -1;
    }

    /**
     * Sums elements in two arrays
     *
     * @param data1  first array
     * @param data2  second array
     * @param offset1 offset for start of first array
     * @param offset2 offset for start of second array
     * @param elements count of elements to sum
     * @return array of sums
     */
    public static short[] sum(short[] data1, short[] data2, int offset1, int offset2, int elements) {
        short[] finalResult = new short[elements];

        int i = 0;
        for (; i + SPECIES_LENGTH < elements; i += SPECIES_LENGTH) {
            var mask1 = SPECIES_PREFERRED.indexInRange(offset1 + i, data1.length);
            var mask2 = SPECIES_PREFERRED.indexInRange(offset2 + i, data2.length);
            var v1 = ShortVector.fromArray(SPECIES_PREFERRED, data1, offset1 + i, mask1);
            var v2 = ShortVector.fromArray(SPECIES_PREFERRED, data2, offset2 + i, mask2);
            var result = v1.add(v2, mask2);
            result.intoArray(finalResult, i, mask1);
        }

        for (; i < elements; ++i) {
            finalResult[i] = (short)(data1[offset1 + i] + data2[offset2 + i]);
        }

        return finalResult;
    }

    /**
     * Multiplies elements in two arrays
     *
     * @param data1  first array
     * @param data2  second array
     * @param offset1 offset for start of first array
     * @param offset2 offset for start of second array
     * @param elements count of elements to multiply
     * @return array of multiplications
     */
    public static short[] mul(short[] data1, short[] data2, int offset1, int offset2, int elements) {
        short[] finalResult = new short[elements];

        int i = 0;
        for (; i + SPECIES_LENGTH < elements; i += SPECIES_LENGTH) {
            var mask1 = SPECIES_PREFERRED.indexInRange(offset1 + i, data1.length);
            var mask2 = SPECIES_PREFERRED.indexInRange(offset2 + i, data2.length);
            var v1 = ShortVector.fromArray(SPECIES_PREFERRED, data1, offset1 + i, mask1);
            var v2 = ShortVector.fromArray(SPECIES_PREFERRED, data2, offset2 + i, mask2);
            var result = v1.mul(v2, mask2);
            result.intoArray(finalResult, i, mask1);
        }

        for (; i < elements; ++i) {
            finalResult[i] = (short)(data1[offset1 + i] * data2[offset2 + i]);
        }

        return finalResult;
    }

}
