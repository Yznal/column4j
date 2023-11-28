package org.column4j.aggregator.vector_api;

import jdk.incubator.vector.IntVector;
import org.column4j.mutable.primitive.IntMutableColumn;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class VectorSumAggregatorTest {
    private final Random random = new Random();

    @Test
    void testSumEvenSizeArray() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();
        var testArraySize = speciesLength * 2;

        var data = new int[testArraySize];
        var exceptedSum = 0;
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextInt(-1000, 1000);
            exceptedSum += data[i];
        }

        var column = mock(IntMutableColumn.class);
        when(column.getData())
                .thenReturn(data);
        when(column.firstRowIndex())
                .thenReturn(0);

        var vectorSumAggregator = new VectorSumAggregator();
        int actual = vectorSumAggregator.aggregate(column, 0, testArraySize);

        assertEquals(exceptedSum, actual);
    }

    @Test
    void testSumOddSizeArray() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();
        var testArraySize = speciesLength * 2 + 1;

        var data = new int[testArraySize];
        var exceptedSum = 0;
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextInt(-1000, 1000);
            exceptedSum += data[i];
        }

        var column = mock(IntMutableColumn.class);
        when(column.getData())
                .thenReturn(data);
        when(column.firstRowIndex())
                .thenReturn(0);

        var aggregator = new VectorSumAggregator();
        int actual = aggregator.aggregate(column, 0, testArraySize);

        assertEquals(exceptedSum, actual);
    }

    @Test
    void testSumWithTombstoneValues() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();
        var testArraySize = speciesLength * 2;

        var data = new int[testArraySize];
        var tombstone = random.nextInt(-100, 100);
        tombstone = tombstone == 0 ? 1 : tombstone;
        var exceptedSum = 0;
        for (int i = 0; i < data.length; i++) {
            if(i % 2 == 0) {
                data[i] = tombstone;
            } else {
                data[i] = random.nextInt(-1000, 1000);
                if(data[i] == tombstone) {
                    data[i]++;
                }
                exceptedSum += data[i];
            }
        }

        var column = mock(IntMutableColumn.class);
        when(column.getData())
                .thenReturn(data);
        when(column.getTombstone())
                .thenReturn(tombstone);
        when(column.firstRowIndex())
                .thenReturn(0);

        var aggregator = new VectorSumAggregator();
        int actual = aggregator.aggregate(column, 0, testArraySize);

        assertEquals(exceptedSum, actual);
    }
}