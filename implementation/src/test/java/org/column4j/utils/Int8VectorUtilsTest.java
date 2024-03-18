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
    private final int arraySize = 300;

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
        byte[] array = {1, 2, 100, 2, 1, 121, 111, 100};

        assertEquals(2, Int8VectorUtils.indexOfAnother(array, (byte)100, 0, arraySize));
        assertEquals(7, Int8VectorUtils.lastIndexOfAnother(array, (byte)100, 0, arraySize));
    }

    @Test
    void indexOfAnotherWhenArrayHasSameValuesTest() {
        byte[] array = new byte[arraySize];
        Arrays.fill(array, (byte)2);
        assertEquals(-1, Int8VectorUtils.indexOfAnother(array, (byte)2, 0, arraySize));
        assertEquals(-1, Int8VectorUtils.lastIndexOfAnother(array, (byte)2, 0, arraySize));
    }
}
