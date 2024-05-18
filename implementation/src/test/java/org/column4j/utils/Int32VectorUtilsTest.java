package org.column4j.utils;

import java.util.Random;
import java.util.Arrays;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


class Int32VectorUtilsTest {
    private final Random random = new Random();
    private final int arraySize = 1000;

    @Test
    void minMaxTest() {
        var array = new int[arraySize];
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;

        for (int i = 0; i < arraySize; ++i) {
            int value = random.nextInt(Integer.MAX_VALUE);
            array[i] = value;
            if (value > max) {
                max = value;
            }
            if (value < min) {
                min = value;
            }
        }

        assertEquals(max, Int32VectorUtils.max(array, Integer.MAX_VALUE, 0, arraySize));
        assertEquals(min, Int32VectorUtils.min(array, Integer.MIN_VALUE, 0, arraySize));
    }

    @Test
    void indexOfAnotherTest() {
        int[] array = new int[arraySize];
        Arrays.fill(array, 2);

        assertEquals(0, Int32VectorUtils.indexOfAnother(array, 1, 0, array.length));
        assertEquals(array.length - 1, Int32VectorUtils.lastIndexOfAnother(array, 1, 0, array.length));
        assertEquals(-1, Int32VectorUtils.indexOfAnother(array, 2, 0, array.length));
        assertEquals(-1, Int32VectorUtils.lastIndexOfAnother(array, 2, 0, array.length));

        int index1 = arraySize / 4;
        int index2 = 3 * arraySize / 4;
        array[index1] = 10;
        array[index2] = 10;

        assertEquals(index1, Int32VectorUtils.indexOfAnother(array, 2, 0, array.length));
        assertEquals(index2, Int32VectorUtils.lastIndexOfAnother(array, 2, 0, array.length));
    }

    @Test
    void sumTest() {
        int[] array1 = new int[arraySize];
        int[] array2 = new int[arraySize];

        int start1 = Math.min(10, arraySize / 2);
        int start2 = Math.min(5, arraySize / 4);
        int elements = Math.min(arraySize / 3, arraySize - start1 - 1);

        int[] expected = new int[elements];

        for (int i = 0; i < arraySize; i++) {
            array1[i] = random.nextInt(Integer.MAX_VALUE / 2);
            array2[i] = random.nextInt(Integer.MAX_VALUE / 2);
        }
        for (int i = 0; i < elements; i++) {
            expected[i] = (array1[start1 + i] + array2[start2 + i]);
        }

        int[] result = Int32VectorUtils.sum(array1, array2, start1, start2, elements);

        for (int i = 0; i < elements; i++) {
            assertEquals(expected[i], result[i]);
        }
    }
}
