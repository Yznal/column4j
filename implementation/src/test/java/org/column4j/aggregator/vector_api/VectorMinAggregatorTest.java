package org.column4j.aggregator.vector_api;

import jdk.incubator.vector.IntVector;
import org.column4j.ColumnVector;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class VectorMinAggregatorTest {
    private final Random random = new Random();

    @Test
    void testSumEvenSizeArray() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();
        var testArraySize = speciesLength * 2;

        var data = new int[testArraySize];
        var exceptedMin = Integer.MAX_VALUE;
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextInt();
            exceptedMin = Math.min(exceptedMin, data[i]);
        }

        var column = mock(ColumnVector.class);
        when(column.getData())
                .thenReturn(data);
        when(column.firstRowIndex())
                .thenReturn(0);

        var aggregator = new VectorMinAggregator();
        int actual = aggregator.aggregate(column, 0, testArraySize);

        assertEquals(exceptedMin, actual);
    }

    @Test
    void testSumOddSizeArray() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();
        var testArraySize = speciesLength * 2 + 1;

        var data = new int[testArraySize];
        var exceptedMin = Integer.MAX_VALUE;
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextInt();
            exceptedMin = Math.min(exceptedMin, data[i]);
        }

        var column = mock(ColumnVector.class);
        when(column.getData())
                .thenReturn(data);
        when(column.firstRowIndex())
                .thenReturn(0);

        var aggregator = new VectorMinAggregator();
        int actual = aggregator.aggregate(column, 0, testArraySize);

        assertEquals(exceptedMin, actual);
    }
}