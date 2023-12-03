package org.column4j.mutable.aggregated.impl;

import org.column4j.mutable.primitive.impl.IntMutableColumnImpl;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class AggregatedIntMutableColumnImplIntegrationTest {
    private final Random random = new Random();

    @Test
    void testAggregationRealColumn() {
        var tombstone = random.nextInt(-1000, 1000);
        var intMutableColumn = new IntMutableColumnImpl(tombstone);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, 8);

        assertEmpty(aggregatedIntMutableColumn, tombstone);

        var value = random.nextInt(-1000, 1000);
        // set value
        aggregatedIntMutableColumn.write(3, value);

        var columnStats = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        var intStats = columnStats.get(0);

        assertEquals(value, intStats.getFirstValue());
        assertEquals(value, intStats.getLastValue());
        assertEquals(3, intStats.getFirstIndex());
        assertEquals(3, intStats.getLastIndex());

        assertEquals(value, intStats.getMin());
        assertEquals(value, intStats.getMax());
        assertEquals(1, intStats.getCount());
        assertEquals(value, intStats.getSum());

        // clean up
        aggregatedIntMutableColumn.tombstone(3);

        assertEmpty(aggregatedIntMutableColumn, tombstone);

        // set minimum
        aggregatedIntMutableColumn.write(3, value);
        // rewrite min
        aggregatedIntMutableColumn.write(3, value - 1);
        // change min
        aggregatedIntMutableColumn.write(4, value - 2);

        columnStats = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        intStats = columnStats.get(0);

        assertEquals(value - 1, intStats.getFirstValue());
        assertEquals(value - 2, intStats.getLastValue());
        assertEquals(3, intStats.getFirstIndex());
        assertEquals(4, intStats.getLastIndex());

        assertEquals(value - 2, intStats.getMin());
        assertEquals(value - 1, intStats.getMax());
        assertEquals(2, intStats.getCount());
        assertEquals(value * 2L - 3, intStats.getSum());

        // remove 1st
        aggregatedIntMutableColumn.tombstone(3);
        aggregatedIntMutableColumn.tombstone(4);

        assertEmpty(aggregatedIntMutableColumn, tombstone);

        // write values
        aggregatedIntMutableColumn.write(3, value);
        aggregatedIntMutableColumn.write(4, value - 1);

        // remove last
        aggregatedIntMutableColumn.tombstone(4);
        aggregatedIntMutableColumn.tombstone(3);

        assertEmpty(aggregatedIntMutableColumn, tombstone);

        // write values
        aggregatedIntMutableColumn.write(3, value);
        aggregatedIntMutableColumn.write(4, value - 1);
        aggregatedIntMutableColumn.write(5, value - 2);

        // remove middle
        aggregatedIntMutableColumn.tombstone(4);

        columnStats = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        intStats = columnStats.get(0);

        assertEquals(value, intStats.getFirstValue());
        assertEquals(value - 2, intStats.getLastValue());
        assertEquals(3, intStats.getFirstIndex());
        assertEquals(5, intStats.getLastIndex());

        assertEquals(value - 2, intStats.getMin());
        assertEquals(value, intStats.getMax());
        assertEquals(2, intStats.getCount());
        assertEquals(value * 2L - 2, intStats.getSum());

        aggregatedIntMutableColumn.tombstone(3);
        aggregatedIntMutableColumn.tombstone(5);

        assertEmpty(aggregatedIntMutableColumn, tombstone);

        // change to the same
        aggregatedIntMutableColumn.tombstone(3);
        assertEmpty(aggregatedIntMutableColumn, tombstone);
    }

    private static void assertEmpty(AggregatedIntMutableColumnImpl aggregatedIntMutableColumn, int tombstone) {
        var columnStats = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        var intStats = columnStats.get(0);

        assertEquals(tombstone, intStats.getFirstValue());
        assertEquals(tombstone, intStats.getLastValue());
        assertEquals(-1, intStats.getFirstIndex());
        assertEquals(-1, intStats.getLastIndex());

        assertEquals(Integer.MAX_VALUE, intStats.getMin());
        assertEquals(Integer.MIN_VALUE, intStats.getMax());
        assertEquals(0, intStats.getCount());
        assertEquals(0, intStats.getSum());
    }

}