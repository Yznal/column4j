package org.column4j.utils;

import java.util.Random;
import java.util.Arrays;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


class Float32VectorUtilsTest {
    private final Random random = new Random();
    private final int arraySize = 1000;

    @Test
    void minMaxTest() {
        var array = new float[arraySize];
        float max = Float.MIN_VALUE;
        float min = Float.MAX_VALUE;

        for (int i = 0; i < arraySize; ++i) {
            float value = random.nextFloat();
            array[i] = value;
            if (value > max) {
                max = value;
            }
            if (value < min) {
                min = value;
            }
        }

        assertEquals(max, Float32VectorUtils.max(array, Float.MAX_VALUE, 0, arraySize));
        assertEquals(min, Float32VectorUtils.min(array, Float.MIN_VALUE, 0, arraySize));
    }

    @Test
    void indexOfAnotherTest() {
        float[] array = new float[arraySize];
        Arrays.fill(array, (float)2);

        assertEquals(0, Float32VectorUtils.indexOfAnother(array, (float)1.0, 0, array.length));
        assertEquals(array.length - 1, Float32VectorUtils.lastIndexOfAnother(array, (float)1.0, 0, array.length));
        assertEquals(-1, Float32VectorUtils.indexOfAnother(array, (float)2.0, 0, array.length));
        assertEquals(-1, Float32VectorUtils.lastIndexOfAnother(array, (float)2.0, 0, array.length));

        int index1 = arraySize / 4;
        int index2 = 3 * arraySize / 4;
        array[index1] = 10;
        array[index2] = 10;

        assertEquals(index1, Float32VectorUtils.indexOfAnother(array, (float)2, 0, array.length));
        assertEquals(index2, Float32VectorUtils.lastIndexOfAnother(array, (float)2, 0, array.length));
    }

    @Test
    void indexOfTest() {
        float[] array = new float[arraySize];
        Arrays.fill(array, (float)2);

        assertEquals(-1, Float32VectorUtils.indexOf(array, (float)1.0, 0, array.length));
        assertEquals(-1, Float32VectorUtils.lastIndexOf(array, (float)1.0, 0, array.length));
        assertEquals(0, Float32VectorUtils.indexOf(array, (float)2.0, 0, array.length));
        assertEquals(arraySize - 1, Float32VectorUtils.lastIndexOf(array, (float)2.0, 0, array.length));

        int index1 = arraySize / 4;
        int index2 = 3 * arraySize / 4;
        array[index1] = 10;
        array[index2] = 10;

        assertEquals(index1, Float32VectorUtils.indexOf(array, 10, 0, array.length));
        assertEquals(index2, Float32VectorUtils.lastIndexOf(array, 10, 0, array.length));
    }

    @Test
    void sumTest() {
        float[] array1 = new float[arraySize];
        float[] array2 = new float[arraySize];

        int start1 = Math.min(10, arraySize / 2);
        int start2 = Math.min(5, arraySize / 4);
        int elements = Math.min(arraySize / 3, arraySize - start1 - 1);

        float[] expected = new float[elements];

        for (int i = 0; i < arraySize; i++) {
            array1[i] = random.nextFloat(Float.MAX_VALUE / 2);
            array2[i] = random.nextFloat(Float.MAX_VALUE / 2);
        }
        for (int i = 0; i < elements; i++) {
            expected[i] = array1[start1 + i] + array2[start2 + i];
        }

        float[] result = Float32VectorUtils.sum(array1, array2, start1, start2, elements);

        for (int i = 0; i < elements; i++) {
            assertEquals(expected[i], result[i]);
        }
    }
}
