package org.column4j.utils;

import java.util.Random;
import java.util.Arrays;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


class Int16VectorUtilsTest {
    private final Random random = new Random();
    private final int arraySize = 1000;

    @Test
    void minMaxTest() {
        var array = new short[arraySize];
        int max = Short.MIN_VALUE;
        int min = Short.MAX_VALUE;

        for (int i = 0; i < arraySize; ++i) {
            short value = (short)random.nextInt(Short.MAX_VALUE);
            array[i] = value;
            if (value > max) {
                max = value;
            }
            if (value < min) {
                min = value;
            }
        }

        assertEquals(max, Int16VectorUtils.max(array, Short.MAX_VALUE, 0, arraySize));
        assertEquals(min, Int16VectorUtils.min(array, Short.MIN_VALUE, 0, arraySize));
    }

    @Test
    void indexOfAnotherTest() {
        short[] array = new short[arraySize];
        Arrays.fill(array, (short)2);

        assertEquals(0, Int16VectorUtils.indexOfAnother(array, (short)1, 0, array.length));
        assertEquals(array.length - 1, Int16VectorUtils.lastIndexOfAnother(array, (short)1, 0, array.length));
        assertEquals(-1, Int16VectorUtils.indexOfAnother(array, (short)2, 0, array.length));
        assertEquals(-1, Int16VectorUtils.lastIndexOfAnother(array, (short)2, 0, array.length));

        int index1 = arraySize / 4;
        int index2 = 3 * arraySize / 4;
        array[index1] = 10;
        array[index2] = 10;

        assertEquals(index1, Int16VectorUtils.indexOfAnother(array, (short)2, 0, array.length));
        assertEquals(index2, Int16VectorUtils.lastIndexOfAnother(array, (short)2, 0, array.length));
    }

    @Test
    void sumTest() {
        short[] array1 = new short[arraySize];
        short[] array2 = new short[arraySize];

        int start1 = Math.min(10, arraySize / 2);
        int start2 = Math.min(5, arraySize / 4);
        int elements = Math.min(arraySize / 3, arraySize - start1 - 1);

        short[] expected = new short[elements];

        for (int i = 0; i < arraySize; i++) {
            array1[i] = (short)random.nextInt(Short.MAX_VALUE / 2);
            array2[i] = (short)random.nextInt(Short.MAX_VALUE / 2);
        }
        for (int i = 0; i < elements; i++) {
            expected[i] = (short)(array1[start1 + i] + array2[start2 + i]);
        }

        short[] result = Int16VectorUtils.sum(array1, array2, start1, start2, elements);

        for (int i = 0; i < elements; i++) {
            assertEquals(expected[i], result[i]);
        }
    }
}
