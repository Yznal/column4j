package org.column4j.utils;

import java.util.Random;
import java.util.Arrays;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


class Float64VectorUtilsTest {
    private final Random random = new Random();
    private final int arraySize = 1000;

    @Test
    void minMaxTest() {
        var array = new double[arraySize];
        double max = Float.MIN_VALUE;
        double min = Float.MAX_VALUE;

        for (int i = 0; i < arraySize; ++i) {
            double value = random.nextFloat(Float.MAX_VALUE);
            array[i] = value;
            if (value > max) {
                max = value;
            }
            if (value < min) {
                min = value;
            }
        }

        assertEquals(max, Float64VectorUtils.max(array, Float.MAX_VALUE, 0, arraySize));
        assertEquals(min, Float64VectorUtils.min(array, Float.MIN_VALUE, 0, arraySize));
    }

    @Test
    void indexOfAnotherTest() {
        double[] array = new double[arraySize];
        Arrays.fill(array, 2);

        assertEquals(0, Float64VectorUtils.indexOfAnother(array, 1, 0, array.length));
        assertEquals(array.length - 1, Float64VectorUtils.lastIndexOfAnother(array, 1, 0, array.length));
        assertEquals(-1, Float64VectorUtils.indexOfAnother(array, 2, 0, array.length));
        assertEquals(-1, Float64VectorUtils.lastIndexOfAnother(array, 2, 0, array.length));

        int index1 = arraySize / 4;
        int index2 = 3 * arraySize / 4;
        array[index1] = 10;
        array[index2] = 10;

        assertEquals(index1, Float64VectorUtils.indexOfAnother(array, 2, 0, array.length));
        assertEquals(index2, Float64VectorUtils.lastIndexOfAnother(array, 2, 0, array.length));
    }

    @Test
    void sumTest() {
        double[] array1 = new double[arraySize];
        double[] array2 = new double[arraySize];

        int start1 = Math.min(10, arraySize / 2);
        int start2 = Math.min(5, arraySize / 4);
        int elements = Math.min(arraySize / 3, arraySize - start1 - 1);

        double[] expected = new double[elements];

        for (int i = 0; i < arraySize; i++) {
            array1[i] = random.nextFloat(Float.MAX_VALUE / 2);
            array2[i] = random.nextFloat(Float.MAX_VALUE / 2);
        }
        for (int i = 0; i < elements; i++) {
            expected[i] = (array1[start1 + i] + array2[start2 + i]);
        }

        double[] result = Float64VectorUtils.sum(array1, array2, start1, start2, elements);

        for (int i = 0; i < elements; i++) {
            assertEquals(expected[i], result[i]);
        }
    }
}
