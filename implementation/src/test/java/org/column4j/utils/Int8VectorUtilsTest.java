package org.column4j.utils;

import java.util.Random;
import java.util.Arrays;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


class Int8VectorUtilsTest {
    private final Random random = new Random();
    private final int arraySize = 1000;

    @Test
    void minMaxTest() {
        var array = new byte[arraySize];
        int max = Byte.MIN_VALUE;
        int min = Byte.MAX_VALUE;

        for (int i = 0; i < arraySize; ++i) {
            byte value = (byte)random.nextInt(Byte.MAX_VALUE);
            array[i] = value;
            if (value > max) {
                max = value;
            }
            if (value < min) {
                min = value;
            }
        }

        assertEquals(max, Int8VectorUtils.max(array, Byte.MAX_VALUE, 0, arraySize));
        assertEquals(min, Int8VectorUtils.min(array, Byte.MIN_VALUE, 0, arraySize));
    }

    @Test
    void indexOfAnotherTest() {
        byte[] array = new byte[arraySize];
        Arrays.fill(array, (byte)2);

        assertEquals(0, Int8VectorUtils.indexOfAnother(array, (byte)1, 0, array.length));
        assertEquals(array.length - 1, Int8VectorUtils.lastIndexOfAnother(array, (byte)1, 0, array.length));
        assertEquals(-1, Int8VectorUtils.indexOfAnother(array, (byte)2, 0, array.length));
        assertEquals(-1, Int8VectorUtils.lastIndexOfAnother(array, (byte)2, 0, array.length));

        int index1 = arraySize / 4;
        int index2 = 3 * arraySize / 4;
        array[index1] = 10;
        array[index2] = 10;

        assertEquals(index1, Int8VectorUtils.indexOfAnother(array, (byte)2, 0, array.length));
        assertEquals(index2, Int8VectorUtils.lastIndexOfAnother(array, (byte)2, 0, array.length));
    }

    @Test
    void indexOfTest() {
        byte[] array = new byte[arraySize];
        Arrays.fill(array, (byte)2);

        assertEquals(-1, Int8VectorUtils.indexOf(array, (byte)1, 0, array.length));
        assertEquals(-1, Int8VectorUtils.lastIndexOf(array, (byte)1, 0, array.length));
        assertEquals(0, Int8VectorUtils.indexOf(array, (byte)2, 0, array.length));
        assertEquals(arraySize - 1, Int8VectorUtils.lastIndexOf(array, (byte)2, 0, array.length));

        int index1 = arraySize / 4;
        int index2 = 3 * arraySize / 4;
        array[index1] = 10;
        array[index2] = 10;

        assertEquals(index1, Int8VectorUtils.indexOf(array, (byte)10, 0, array.length));
        assertEquals(index2, Int8VectorUtils.lastIndexOf(array, (byte)10, 0, array.length));
    }

    @Test
    void sumTest() {
        byte[] array1 = new byte[arraySize];
        byte[] array2 = new byte[arraySize];

        int start1 = Math.min(10, arraySize / 2);
        int start2 = Math.min(5, arraySize / 4);
        int elements = Math.min(arraySize / 3, arraySize - start1 - 1);

        byte[] expected = new byte[elements];

        for (int i = 0; i < arraySize; i++) {
            array1[i] = (byte)random.nextInt(Byte.MAX_VALUE / 2);
            array2[i] = (byte)random.nextInt(Byte.MAX_VALUE / 2);
        }
        for (int i = 0; i < elements; i++) {
            expected[i] = (byte)(array1[start1 + i] + array2[start2 + i]);
        }

        byte[] result = Int8VectorUtils.sum(array1, array2, start1, start2, elements);

        for (int i = 0; i < elements; i++) {
            assertEquals(expected[i], result[i]);
        }
    }
}
