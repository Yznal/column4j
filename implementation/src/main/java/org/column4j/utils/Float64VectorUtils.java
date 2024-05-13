package org.column4j.utils;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public final class Float64VectorUtils {

    public static final VectorSpecies<Double> SPECIES_PREFERRED = DoubleVector.SPECIES_PREFERRED;
    public static final int SPECIES_LENGTH = SPECIES_PREFERRED.length();

    private Float64VectorUtils() {
    }

    /**
     * Get maximum value in array in passed bounds using VectorAPI
     *
     * @param data      source double array
     * @param tombstone ignored value
     * @param from      from index (inclusive)
     * @param to        to index (exclusive)
     * @return maximum on array or {@link Double.MIN_VALUE}
     */
    public static double max(double[] data, double tombstone, int from, int to) {
        var maxVector = DoubleVector.broadcast(SPECIES_PREFERRED, Double.MIN_VALUE);
        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextVector = DoubleVector.fromArray(SPECIES_PREFERRED, data, from);
            var tombstoneMask = nextVector.eq(tombstone).not();
            maxVector = maxVector.lanewise(VectorOperators.MAX, nextVector, tombstoneMask);
        }
        var max = maxVector.reduceLanes(VectorOperators.MAX);
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
     * @param data      source array
     * @param tombstone ignored value
     * @param from      from index (inclusive)
     * @param to        to index (exclusive)
     * @return minimum on array or {@link Double.MAX_VALUE}
     */
    public static double min(double[] data, double tombstone, int from, int to) {
        var minVector = DoubleVector.broadcast(SPECIES_PREFERRED, Double.MAX_VALUE);
        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextVector = DoubleVector.fromArray(SPECIES_PREFERRED, data, from);
            var tombstoneMask = nextVector.eq(tombstone).not();
            minVector = minVector.lanewise(VectorOperators.MIN, nextVector, tombstoneMask);
        }
        var min = minVector.reduceLanes(VectorOperators.MIN);
        // tail
        for (; from < to; from++) {
            var value = data[from];
            min = value == tombstone ? min : Math.min(min, value);
        }
        return min;
    }

    /**
     * Find index of first element what not equals to passed value
     *
     * @param data  source double array
     * @param value value for filter
     * @param from  left bound (inclusive)
     * @param to    right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int indexOfAnother(double[] data, double value, int from, int to) {
        for (; from < to && from + SPECIES_LENGTH <= to; from += SPECIES_LENGTH) {
            var nextVector = DoubleVector.fromArray(SPECIES_PREFERRED, data, from);
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
     * @param data  source double array
     * @param value value for filter
     * @param from  left bound (inclusive)
     * @param to    right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int lastIndexOfAnother(double[] data, double value, int from, int to) {
        for (; to > from && from + SPECIES_LENGTH <= to; to -= SPECIES_LENGTH) {
            var nextVector = DoubleVector.fromArray(SPECIES_PREFERRED, data, to - SPECIES_LENGTH);
            var valueMask = nextVector.eq(value).not();
            if (valueMask.anyTrue()) {
                return to - SPECIES_LENGTH + valueMask.lastTrue();
            }
        }
        if (to == data.length) {
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
    public static double[] sum(double[] data1, double[] data2, int offset1, int offset2, int elements) {
        double[] finalResult = new double[elements];

        int i = 0;
        for (; i + SPECIES_LENGTH < elements; i += SPECIES_LENGTH) {
            var mask1 = SPECIES_PREFERRED.indexInRange(offset1 + i, data1.length);
            var mask2 = SPECIES_PREFERRED.indexInRange(offset2 + i, data2.length);
            var v1 = DoubleVector.fromArray(SPECIES_PREFERRED, data1, offset1 + i, mask1);
            var v2 = DoubleVector.fromArray(SPECIES_PREFERRED, data2, offset2 + i, mask2);
            var result = v1.add(v2, mask2);
            result.intoArray(finalResult, i, mask1);
        }

        for (; i < elements; ++i) {
            finalResult[i] = data1[offset1 + i] + data2[offset2 + i];
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
    public static double[] mul(double[] data1, double[] data2, int offset1, int offset2, int elements) {
        double[] finalResult = new double[elements];

        int i = 0;
        for (; i + SPECIES_LENGTH < elements; i += SPECIES_LENGTH) {
            var mask1 = SPECIES_PREFERRED.indexInRange(offset1 + i, data1.length);
            var mask2 = SPECIES_PREFERRED.indexInRange(offset2 + i, data2.length);
            var v1 = DoubleVector.fromArray(SPECIES_PREFERRED, data1, offset1 + i, mask1);
            var v2 = DoubleVector.fromArray(SPECIES_PREFERRED, data2, offset2 + i, mask2);
            var result = v1.mul(v2, mask2);
            result.intoArray(finalResult, i, mask1);
        }

        for (; i < elements; ++i) {
            finalResult[i] = data1[offset1 + i] * data2[offset2 + i];
        }

        return finalResult;
    }

}
