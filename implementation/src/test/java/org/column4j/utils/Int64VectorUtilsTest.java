package org.column4j.utils;

import java.util.Random;
import java.util.Arrays;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


class Int64VectorUtilsTest {
    private final Random random = new Random();
    private final int arraySize = 1000;

    @Test
    void minMaxTest() {
        var array = new long[arraySize];
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;

        for (int i = 0; i < arraySize; ++i) {
            long value = random.nextLong();
            array[i] = value;
            if (value > max) {
                max = value;
            }
            if (value < min) {
                min = value;
            }
        }

        assertEquals(max, Int64VectorUtils.max(array, Long.MAX_VALUE, 0, arraySize));
        assertEquals(min, Int64VectorUtils.min(array, Long.MIN_VALUE, 0, arraySize));
    }

    @Test
    void indexOfAnotherTest() {
        long[] array = new long[arraySize];
        Arrays.fill(array, 2);

        assertEquals(0, Int64VectorUtils.indexOfAnother(array, 1, 0, array.length));
        assertEquals(array.length - 1, Int64VectorUtils.lastIndexOfAnother(array, 1, 0, array.length));
        assertEquals(-1, Int64VectorUtils.indexOfAnother(array, 2, 0, array.length));
        assertEquals(-1, Int64VectorUtils.lastIndexOfAnother(array, 2, 0, array.length));

        int index1 = arraySize / 4;
        int index2 = 3 * arraySize / 4;
        array[index1] = 10;
        array[index2] = 10;

        assertEquals(index1, Int64VectorUtils.indexOfAnother(array, 2, 0, array.length));
        assertEquals(index2, Int64VectorUtils.lastIndexOfAnother(array, 2, 0, array.length));
    }

    @Test
    void indexOfTest() {
        long[] array = new long[arraySize];
        Arrays.fill(array, 2);

        assertEquals(-1, Int64VectorUtils.indexOf(array, 1, 0, array.length));
        assertEquals(-1, Int64VectorUtils.lastIndexOf(array, 1, 0, array.length));
        assertEquals(0, Int64VectorUtils.indexOf(array, 2, 0, array.length));
        assertEquals(arraySize - 1, Int64VectorUtils.lastIndexOf(array, 2, 0, array.length));

        int index1 = arraySize / 4;
        int index2 = 3 * arraySize / 4;
        array[index1] = 10;
        array[index2] = 10;

        assertEquals(index1, Int64VectorUtils.indexOf(array, 10, 0, array.length));
        assertEquals(index2, Int64VectorUtils.lastIndexOf(array, 10, 0, array.length));
    }

    @Test
    void sumTest() {
        long[] array1 = new long[arraySize];
        long[] array2 = new long[arraySize];

        int start1 = Math.min(10, arraySize / 2);
        int start2 = Math.min(5, arraySize / 4);
        int elements = Math.min(arraySize / 3, arraySize - start1 - 1);

        long[] expected = new long[elements];

        for (int i = 0; i < arraySize; i++) {
            array1[i] = random.nextLong();
            array2[i] = random.nextLong();
        }
        for (int i = 0; i < elements; i++) {
            expected[i] = (array1[start1 + i] + array2[start2 + i]);
        }

        long[] result = Int64VectorUtils.sum(array1, array2, start1, start2, elements);

        for (int i = 0; i < elements; i++) {
            assertEquals(expected[i], result[i]);
        }
    }
}
