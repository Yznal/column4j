package org.column4j.mutable.aggregated.impl;

import org.column4j.mutable.primitive.impl.ByteMutableColumnImpl;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class AggregatedByteMutableColumnImplIntegrationTest {
    private final Random random = new Random();

    @Test
    void testAggregationRealColumn() {
        var tombstone = (byte) random.nextInt(-100, 100);
        var intMutableColumn = new ByteMutableColumnImpl(tombstone);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(intMutableColumn, 8);

        assertEmpty(aggregatedByteMutableColumn, tombstone);

        var value = (byte)random.nextInt(-100, 100);
        if(tombstone == value) {
            value++;
        }
        // set value
        aggregatedByteMutableColumn.write(3, value);

        var columnStats = aggregatedByteMutableColumn.getChunkedColumnStatistics();
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
        aggregatedByteMutableColumn.tombstone(3);

        assertEmpty(aggregatedByteMutableColumn, tombstone);

        // set minimum
        aggregatedByteMutableColumn.write(3, value);
        // rewrite min
        aggregatedByteMutableColumn.write(3, (byte) (value - 1));
        // change min
        aggregatedByteMutableColumn.write(4, (byte) (value - 2));

        columnStats = aggregatedByteMutableColumn.getChunkedColumnStatistics();
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
        aggregatedByteMutableColumn.tombstone(3);
        aggregatedByteMutableColumn.tombstone(4);

        assertEmpty(aggregatedByteMutableColumn, tombstone);

        // write values
        aggregatedByteMutableColumn.write(3, value);
        aggregatedByteMutableColumn.write(4, (byte) (value - 1));

        // remove last
        aggregatedByteMutableColumn.tombstone(4);
        aggregatedByteMutableColumn.tombstone(3);

        assertEmpty(aggregatedByteMutableColumn, tombstone);

        // write values
        aggregatedByteMutableColumn.write(3, value);
        aggregatedByteMutableColumn.write(4, (byte) (value - 1));
        aggregatedByteMutableColumn.write(5, (byte) (value - 2));

        // remove middle
        aggregatedByteMutableColumn.tombstone(4);

        columnStats = aggregatedByteMutableColumn.getChunkedColumnStatistics();
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

        aggregatedByteMutableColumn.tombstone(3);
        aggregatedByteMutableColumn.tombstone(5);

        assertEmpty(aggregatedByteMutableColumn, tombstone);

        // change to the same
        aggregatedByteMutableColumn.tombstone(3);
        assertEmpty(aggregatedByteMutableColumn, tombstone);
    }

    private static void assertEmpty(AggregatedByteMutableColumnImpl aggregatedByteMutableColumn, byte tombstone) {
        var columnStats = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        var intStats = columnStats.get(0);

        assertEquals(tombstone, intStats.getFirstValue());
        assertEquals(tombstone, intStats.getLastValue());
        assertEquals(-1, intStats.getFirstIndex());
        assertEquals(-1, intStats.getLastIndex());

        assertEquals(Byte.MAX_VALUE, intStats.getMin());
        assertEquals(Byte.MIN_VALUE, intStats.getMax());
        assertEquals(0, intStats.getCount());
        assertEquals(0, intStats.getSum());
    }

}