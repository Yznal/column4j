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
class VectorMaxAggregatorTest {
    private final Random random = new Random();

    @Test
    void testMaxEvenSizeArray() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();
        var testArraySize = speciesLength * 2;

        var data = new int[testArraySize];
        var exceptedMax = Integer.MIN_VALUE;
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextInt();
            exceptedMax = Math.max(exceptedMax, data[i]);
        }

        var column = mock(IntMutableColumn.class);
        when(column.getData())
                .thenReturn(data);
        when(column.firstRowIndex())
                .thenReturn(0);

        var aggregator = new VectorMaxAggregator();
        int actual = aggregator.aggregate(column, 0, testArraySize);

        assertEquals(exceptedMax, actual);
    }

    @Test
    void testMaxOddSizeArray() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();
        var testArraySize = speciesLength * 2 + 1;

        var data = new int[testArraySize];
        var exceptedMax = Integer.MIN_VALUE;
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextInt();
            exceptedMax = Math.max(exceptedMax, data[i]);
        }

        var column = mock(IntMutableColumn.class);
        when(column.getData())
                .thenReturn(data);
        when(column.firstRowIndex())
                .thenReturn(0);

        var aggregator = new VectorMaxAggregator();
        int actual = aggregator.aggregate(column, 0, testArraySize);

        assertEquals(exceptedMax, actual);
    }
    @Test
    void testMaxWithTombstoneValues() {
        var speciesPreferred = IntVector.SPECIES_PREFERRED;
        var speciesLength = speciesPreferred.length();
        var testArraySize = speciesLength * 2;

        var data = new int[testArraySize];
        var tombstone = random.nextInt(50, 500);
        var exceptedMax = Integer.MIN_VALUE;
        for (int i = 0; i < data.length; i++) {
            if(i % 2 == 0) {
                data[i] = tombstone;
            } else {
                data[i] = random.nextInt(-1000, 1000);
                if(data[i] == tombstone) {
                    data[i]++;
                }
                exceptedMax = Math.max(exceptedMax, data[i]);
            }
        }

        var column = mock(IntMutableColumn.class);
        when(column.getData())
                .thenReturn(data);
        when(column.getTombstone())
                .thenReturn(tombstone);
        when(column.firstRowIndex())
                .thenReturn(0);

        var aggregator = new VectorMaxAggregator();
        int actual = aggregator.aggregate(column, 0, testArraySize);

        assertEquals(exceptedMax, actual);
    }
}