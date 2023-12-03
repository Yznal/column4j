package org.column4j.utils;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class ByteStatisticUtilsTest {

    @Test
    void testMaxWithEvenSizeArray() {
        var speciesPreferred = ByteVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();

        var arraySize = speciesLength * 2;
        var data = new byte[arraySize];
        assertEquals(-1, ByteStatisticUtils.indexOfAnother(data, (byte)0, 0, arraySize));

        for (int i = 0; i < arraySize; i++) {
            data[i] = 1;
            var max = ByteStatisticUtils.max(data, (byte)0, 0, arraySize);
            assertEquals(1, max);
            data[i] = 0;
        }
    }

    @Test
    void testMaxWithOddSizeArray() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();

        var arraySize = speciesLength * 2 + 1;
        var data = new byte[arraySize];
        assertEquals(-1, ByteStatisticUtils.indexOfAnother(data, (byte)0, 0, arraySize));

        for (int i = 0; i < arraySize; i++) {
            data[i] = 1;
            var max = ByteStatisticUtils.max(data, (byte)0, 0, arraySize);
            assertEquals(1, max);
            data[i] = 0;
        }
    }

    @Test
    void testMinWithEvenSizeArray() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();

        var arraySize = speciesLength * 2;
        var data = new byte[arraySize];
        assertEquals(-1, ByteStatisticUtils.indexOfAnother(data, (byte)0, 0, arraySize));

        for (int i = 0; i < arraySize; i++) {
            data[i] = 1;
            var min = ByteStatisticUtils.min(data, (byte)0, 0, arraySize);
            assertEquals(1, min);
            data[i] = 0;
        }
    }

    @Test
    void testMinWithOddSizeArray() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();

        var arraySize = speciesLength * 2 + 1;
        var data = new byte[arraySize];
        assertEquals(-1, ByteStatisticUtils.indexOfAnother(data, (byte)0, 0, arraySize));

        for (int i = 0; i < arraySize; i++) {
            data[i] = 1;
            var min = ByteStatisticUtils.min(data, (byte)0, 0, arraySize);
            assertEquals(1, min);
            data[i] = 0;
        }
    }

    @Test
    void testIndexOfAnotherWithEvenSizeArray() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();

        var arraySize = speciesLength * 2;
        var data = new byte[arraySize];
        assertEquals(-1, ByteStatisticUtils.indexOfAnother(data, (byte)0, 0, arraySize));

        for (int i = 0; i < arraySize; i++) {
            data[i] = (byte) (i + 1);
            var index = ByteStatisticUtils.indexOfAnother(data, (byte)0, 0, arraySize);
            assertEquals(i, index);
            data[i] = 0;
        }
    }

    @Test
    void testIndexOfAnotherWithOddSizeArray() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();

        var arraySize = speciesLength * 2 + 1;
        var data = new byte[arraySize];
        assertEquals(-1, ByteStatisticUtils.indexOfAnother(data, (byte)0, 0, arraySize));

        for (int i = 0; i < arraySize; i++) {
            data[i] = (byte) (i + 1);
            var index = ByteStatisticUtils.indexOfAnother(data, (byte)0, 0, arraySize);
            assertEquals(i, index);
            data[i] = 0;
        }
    }

    @Test
    void testLastIndexOfAnotherWithEvenSizeArray() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();

        var arraySize = speciesLength * 2;
        var data = new byte[arraySize];
        assertEquals(-1, ByteStatisticUtils.lastIndexOfAnother(data, (byte)0, 0, arraySize));

        for (int i = 0; i < arraySize; i++) {
            data[i] = (byte) (i + 1);
            var index = ByteStatisticUtils.lastIndexOfAnother(data, (byte)0, 0, arraySize);
            assertEquals(i, index);
            data[i] = 0;
        }
    }

    @Test
    void testLastIndexOfAnotherWithOddSizeArray() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();

        var arraySize = speciesLength * 2 + 1;
        var data = new byte[arraySize];
        assertEquals(-1, ByteStatisticUtils.lastIndexOfAnother(data, (byte)0, 0, arraySize));

        for (int i = 0; i < arraySize; i++) {
            data[i] = (byte) (i + 1);
            var index = ByteStatisticUtils.lastIndexOfAnother(data, (byte)0, 0, arraySize);
            assertEquals(i, index);
            data[i] = 0;
        }
    }

}