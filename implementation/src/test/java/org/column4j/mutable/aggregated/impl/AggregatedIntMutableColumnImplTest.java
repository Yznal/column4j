package org.column4j.mutable.aggregated.impl;

import org.column4j.ColumnType;
import org.column4j.mutable.primitive.IntMutableColumn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class AggregatedIntMutableColumnImplTest {
    private final Random random = new Random();

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024})
    void testAggregateOnInitWithOneChunk(int dataSize) {
        var tombstone = random.nextInt(-dataSize, dataSize);

        int[] data = new int[dataSize];

        int firstIndex = -1;
        int lastIndex = -1;
        var min = Integer.MAX_VALUE;
        var max = Integer.MIN_VALUE;
        int count = 0;
        long sum = 0;

        for (int i = 0; i < dataSize; i++) {
            data[i] = random.nextInt(-dataSize, dataSize);
            var value = data[i];
            if (value == tombstone) {
                continue;
            }
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
            if (firstIndex == -1) {
                firstIndex = i;
            }
            lastIndex = i;
        }

        var intMutableColumn = mock(IntMutableColumn.class);

        when(intMutableColumn.getTombstone())
                .thenReturn(tombstone);

        when(intMutableColumn.getData())
                .thenReturn(data);

        when(intMutableColumn.size())
                .thenReturn(data.length);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, dataSize);
        assertEquals(dataSize, aggregatedIntMutableColumn.getChunkSize());

        var columnStats = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        var statistic = columnStats.get(0);

        if (firstIndex == -1) {
            assertEquals(tombstone, statistic.getFirstValue());
            assertEquals(tombstone, statistic.getLastValue());
            assertEquals(-1, statistic.getFirstIndex());
            assertEquals(-1, statistic.getLastIndex());
        } else {
            assertEquals(firstIndex, statistic.getFirstIndex());
            assertEquals(data[firstIndex], statistic.getFirstValue());

            assertEquals(lastIndex, statistic.getLastIndex());
            assertEquals(data[lastIndex], statistic.getLastValue());
        }

        assertEquals(min, statistic.getMin());
        assertEquals(max, statistic.getMax());
        assertEquals(count, statistic.getCount());
        assertEquals(sum, statistic.getSum());
    }

    @ParameterizedTest
    @ValueSource(ints = {16, 32, 64, 128, 256, 512, 1024})
    void testAggregateOnInitWithSeveralChunks(int dataSize) {
        var tombstone = random.nextInt(-dataSize, dataSize);

        int[] data = new int[dataSize];

        for (int i = 0; i < dataSize; i++) {
            data[i] = random.nextInt(-dataSize, dataSize);
        }

        var intMutableColumn = mock(IntMutableColumn.class);

        when(intMutableColumn.getTombstone())
                .thenReturn(tombstone);

        when(intMutableColumn.getData())
                .thenReturn(data);

        when(intMutableColumn.size())
                .thenReturn(data.length);

        int chunkSize = random.nextInt(4, dataSize / 2);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, chunkSize);
        assertEquals(chunkSize, aggregatedIntMutableColumn.getChunkSize());

        var columnStats = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        var chunks = Math.max(1, (dataSize + chunkSize / 2) / chunkSize);

        for (int chunk = 0; chunk < chunks; chunk++) {
            int firstIndex = -1;
            int lastIndex = -1;
            var min = Integer.MAX_VALUE;
            var max = Integer.MIN_VALUE;
            int count = 0;
            long sum = 0;

            for (int i = 0; i < chunkSize; i++) {
                var index = chunk * chunkSize + i;
                if (index >= data.length) {
                    break;
                }
                var value = data[index];
                if (value == tombstone) {
                    continue;
                }
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
                count++;
                if (firstIndex == -1) {
                    firstIndex = index;
                }
                lastIndex = index;
            }

            var statistic = columnStats.get(chunk);

            if (firstIndex == -1) {
                assertEquals(tombstone, statistic.getFirstValue());
                assertEquals(tombstone, statistic.getLastIndex());
                assertEquals(-1, statistic.getFirstIndex());
                assertEquals(-1, statistic.getLastIndex());
            } else {
                assertEquals(firstIndex, statistic.getFirstIndex());
                assertEquals(data[firstIndex], statistic.getFirstValue());

                assertEquals(lastIndex, statistic.getLastIndex());
                assertEquals(data[lastIndex], statistic.getLastValue());
            }

            assertEquals(min, statistic.getMin());
            assertEquals(max, statistic.getMax());
            assertEquals(count, statistic.getCount());
            assertEquals(sum, statistic.getSum());
        }

    }

    @Test
    void testAggregateOnInitWithEmptyColumn() {
        var intMutableColumn = mock(IntMutableColumn.class);

        var tombstone = random.nextInt();
        when(intMutableColumn.getTombstone())
                .thenReturn(tombstone);

        when(intMutableColumn.getData())
                .thenReturn(new int[0]);

        when(intMutableColumn.size())
                .thenReturn(0);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, 8);
        assertEquals(8, aggregatedIntMutableColumn.getChunkSize());

        var columnStats = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        var statistic = columnStats.get(0);

        assertEquals(tombstone, statistic.getFirstValue());
        assertEquals(tombstone, statistic.getLastValue());
        assertEquals(-1, statistic.getFirstIndex());
        assertEquals(-1, statistic.getLastIndex());

        assertEquals(Integer.MAX_VALUE, statistic.getMin());
        assertEquals(Integer.MIN_VALUE, statistic.getMax());
        assertEquals(0, statistic.getCount());
        assertEquals(0, statistic.getSum());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024})
    void testAggregateOnInitWithTombstoneOnlyColumn(int dataSize) {
        var tombstone = random.nextInt(-dataSize, dataSize);

        int[] data = new int[dataSize];
        Arrays.fill(data, tombstone);

        var intMutableColumn = mock(IntMutableColumn.class);

        when(intMutableColumn.getTombstone())
                .thenReturn(tombstone);

        when(intMutableColumn.getData())
                .thenReturn(data);

        when(intMutableColumn.size())
                .thenReturn(data.length);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, dataSize);
        assertEquals(dataSize, aggregatedIntMutableColumn.getChunkSize());

        var columnStats = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        var statistic = columnStats.get(0);

        assertEquals(tombstone, statistic.getFirstValue());
        assertEquals(tombstone, statistic.getLastValue());
        assertEquals(-1, statistic.getFirstIndex());
        assertEquals(-1, statistic.getLastIndex());

        assertEquals(Integer.MAX_VALUE, statistic.getMin());
        assertEquals(Integer.MIN_VALUE, statistic.getMax());
        assertEquals(0, statistic.getCount());
        assertEquals(0, statistic.getSum());
    }

    @Test
    void testDefaultWrappingMethodCall() {
        var intMutableColumn = mock(IntMutableColumn.class);

        var tombstone = random.nextInt();
        when(intMutableColumn.getTombstone())
                .thenReturn(tombstone);


        var data = new int[0];
        when(intMutableColumn.getData())
                .thenReturn(data);

        when(intMutableColumn.size())
                .thenReturn(0);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, 8);

        when(intMutableColumn.getData())
                .thenReturn(data);

        assertEquals(data, aggregatedIntMutableColumn.getData());

        assertEquals(tombstone, aggregatedIntMutableColumn.getTombstone());

        var size = random.nextInt();
        when(intMutableColumn.size())
                .thenReturn(size);

        assertEquals(size, aggregatedIntMutableColumn.size());
        var firstRowIndex = random.nextInt();
        when(intMutableColumn.firstRowIndex())
                .thenReturn(firstRowIndex);

        assertEquals(firstRowIndex, aggregatedIntMutableColumn.firstRowIndex());

        var capacity = random.nextInt();
        when(intMutableColumn.capacity())
                .thenReturn(capacity);

        assertEquals(capacity, aggregatedIntMutableColumn.capacity());

        var columnType = mock(ColumnType.class);
        when(intMutableColumn.type())
                .thenReturn(columnType);

        assertEquals(columnType, aggregatedIntMutableColumn.type());

    }

    @Test
    void testRewriteValue() {
        var intMutableColumn = mock(IntMutableColumn.class);

        when(intMutableColumn.getTombstone())
                .thenReturn(0);

        var oldValue = random.nextInt(-1000, 1000) + 1;
        var data = new int[]{oldValue};
        when(intMutableColumn.getData())
                .thenReturn(data);

        when(intMutableColumn.size())
                .thenReturn(1);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var statistic = chunkedColumnStatistics.get(0);
        assertEquals(0, statistic.getFirstIndex());
        assertEquals(oldValue, statistic.getFirstValue());
        assertEquals(0, statistic.getLastIndex());
        assertEquals(oldValue, statistic.getLastValue());
        assertEquals(oldValue, statistic.getMin());
        assertEquals(oldValue, statistic.getMax());
        assertEquals(oldValue, statistic.getSum());
        assertEquals(1, statistic.getCount());

        var newValue = oldValue + random.nextInt(1, 1000);

        doAnswer(it -> {
            data[0] = newValue;
            return null;
        })
                .when(intMutableColumn).write(0, newValue);

        aggregatedIntMutableColumn.write(0, newValue);

        chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        statistic = chunkedColumnStatistics.get(0);
        assertEquals(0, statistic.getFirstIndex());
        assertEquals(newValue, statistic.getFirstValue());
        assertEquals(0, statistic.getLastIndex());
        assertEquals(newValue, statistic.getLastValue());
        assertEquals(newValue, statistic.getMin());
        assertEquals(newValue, statistic.getMax());
        assertEquals(newValue, statistic.getSum());
        assertEquals(1, statistic.getCount());

    }

    @Test
    void testRewriteMinToMaxInMiddleValue() {
        var mutableColumn = mock(IntMutableColumn.class);

        when(mutableColumn.getTombstone())
                .thenReturn(0);

        var oldValue = random.nextInt(-1000, 1000) + 1;
        var otherValue = oldValue + 1;
        var data = new int[]{otherValue, oldValue, otherValue};
        when(mutableColumn.getData())
                .thenReturn(data);

        when(mutableColumn.size())
                .thenReturn(3);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(mutableColumn, 8);

        var chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var statistic = chunkedColumnStatistics.get(0);
        assertEquals(0, statistic.getFirstIndex());
        assertEquals(otherValue, statistic.getFirstValue());
        assertEquals(2, statistic.getLastIndex());
        assertEquals(otherValue, statistic.getLastValue());
        assertEquals(oldValue, statistic.getMin());
        assertEquals(otherValue, statistic.getMax());
        assertEquals(oldValue * 3L + 2, statistic.getSum());
        assertEquals(3, statistic.getCount());

        var newValue = oldValue + random.nextInt(2, 1000);

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(mutableColumn).write(1, newValue);

        aggregatedIntMutableColumn.write(1, newValue);

        chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        statistic = chunkedColumnStatistics.get(0);
        assertEquals(0, statistic.getFirstIndex());
        assertEquals(otherValue, statistic.getFirstValue());
        assertEquals(2, statistic.getLastIndex());
        assertEquals(otherValue, statistic.getLastValue());
        assertEquals(otherValue, statistic.getMin());
        assertEquals(newValue, statistic.getMax());
        assertEquals(oldValue * 2L + 2 + newValue, statistic.getSum());
        assertEquals(3, statistic.getCount());

    }

    @Test
    void testRewriteMinToMinInMiddleValue() {
        var mutableColumn = mock(IntMutableColumn.class);

        when(mutableColumn.getTombstone())
                .thenReturn(0);

        var oldValue = random.nextInt(-1000, 1000) + 1;
        var otherValue = oldValue + 1;
        var data = new int[]{otherValue, oldValue, otherValue};
        when(mutableColumn.getData())
                .thenReturn(data);

        when(mutableColumn.size())
                .thenReturn(3);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(mutableColumn, 8);

        var chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var statistic = chunkedColumnStatistics.get(0);
        assertEquals(0, statistic.getFirstIndex());
        assertEquals(otherValue, statistic.getFirstValue());
        assertEquals(2, statistic.getLastIndex());
        assertEquals(otherValue, statistic.getLastValue());
        assertEquals(oldValue, statistic.getMin());
        assertEquals(otherValue, statistic.getMax());
        assertEquals(oldValue * 3L + 2, statistic.getSum());
        assertEquals(3, statistic.getCount());

        var newValue = oldValue - random.nextInt(2, 1000);

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(mutableColumn).write(1, newValue);

        aggregatedIntMutableColumn.write(1, newValue);

        chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        statistic = chunkedColumnStatistics.get(0);
        assertEquals(0, statistic.getFirstIndex());
        assertEquals(otherValue, statistic.getFirstValue());
        assertEquals(2, statistic.getLastIndex());
        assertEquals(otherValue, statistic.getLastValue());
        assertEquals(newValue, statistic.getMin());
        assertEquals(otherValue, statistic.getMax());
        assertEquals(oldValue * 2L + 2 + newValue, statistic.getSum());
        assertEquals(3, statistic.getCount());

    }

    @Test
    void testRewriteMaxToMinInMiddleValue() {
        var intMutableColumn = mock(IntMutableColumn.class);

        when(intMutableColumn.getTombstone())
                .thenReturn(0);

        var oldValue = random.nextInt(-1000, 1000) + 1;
        var otherValue = oldValue - 1;
        var data = new int[]{otherValue, oldValue, otherValue};
        when(intMutableColumn.getData())
                .thenReturn(data);

        when(intMutableColumn.size())
                .thenReturn(3);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var statistic = chunkedColumnStatistics.get(0);
        assertEquals(0, statistic.getFirstIndex());
        assertEquals(otherValue, statistic.getFirstValue());
        assertEquals(2, statistic.getLastIndex());
        assertEquals(otherValue, statistic.getLastValue());
        assertEquals(otherValue, statistic.getMin());
        assertEquals(oldValue, statistic.getMax());
        assertEquals(oldValue * 3L - 2, statistic.getSum());
        assertEquals(3, statistic.getCount());

        var newValue = oldValue - random.nextInt(2, 1000);

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(intMutableColumn).write(1, newValue);

        aggregatedIntMutableColumn.write(1, newValue);

        chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        statistic = chunkedColumnStatistics.get(0);
        assertEquals(0, statistic.getFirstIndex());
        assertEquals(otherValue, statistic.getFirstValue());
        assertEquals(2, statistic.getLastIndex());
        assertEquals(otherValue, statistic.getLastValue());
        assertEquals(newValue, statistic.getMin());
        assertEquals(otherValue, statistic.getMax());
        assertEquals(oldValue * 2L - 2 + newValue, statistic.getSum());
        assertEquals(3, statistic.getCount());

    }

    @Test
    void testRewriteMaxToMaxInMiddleValue() {
        var intMutableColumn = mock(IntMutableColumn.class);

        when(intMutableColumn.getTombstone())
                .thenReturn(0);

        var oldValue = random.nextInt(-1000, 1000) + 1;
        var otherValue = oldValue - 1;
        var data = new int[]{otherValue, oldValue, otherValue};
        when(intMutableColumn.getData())
                .thenReturn(data);

        when(intMutableColumn.size())
                .thenReturn(3);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var statistic = chunkedColumnStatistics.get(0);
        assertEquals(0, statistic.getFirstIndex());
        assertEquals(otherValue, statistic.getFirstValue());
        assertEquals(2, statistic.getLastIndex());
        assertEquals(otherValue, statistic.getLastValue());
        assertEquals(otherValue, statistic.getMin());
        assertEquals(oldValue, statistic.getMax());
        assertEquals(oldValue * 3L - 2, statistic.getSum());
        assertEquals(3, statistic.getCount());

        var newValue = oldValue + random.nextInt(2, 1000);

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(intMutableColumn).write(1, newValue);

        aggregatedIntMutableColumn.write(1, newValue);

        chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        statistic = chunkedColumnStatistics.get(0);
        assertEquals(0, statistic.getFirstIndex());
        assertEquals(otherValue, statistic.getFirstValue());
        assertEquals(2, statistic.getLastIndex());
        assertEquals(otherValue, statistic.getLastValue());
        assertEquals(otherValue, statistic.getMin());
        assertEquals(newValue, statistic.getMax());
        assertEquals(oldValue * 2L - 2 + newValue, statistic.getSum());
        assertEquals(3, statistic.getCount());

    }

    @Test
    void testWriteMaxValue() {
        var intMutableColumn = mock(IntMutableColumn.class);

        when(intMutableColumn.getTombstone())
                .thenReturn(0);

        var baseMax = random.nextInt(-1000, 1000) + 1;
        var data = new int[]{0, baseMax};
        when(intMutableColumn.getData())
                .thenReturn(data);

        when(intMutableColumn.size())
                .thenReturn(2);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var statistic = chunkedColumnStatistics.get(0);

        assertEquals(1, statistic.getFirstIndex());
        assertEquals(baseMax, statistic.getFirstValue());

        assertEquals(1, statistic.getLastIndex());
        assertEquals(baseMax, statistic.getLastValue());

        assertEquals(baseMax, statistic.getMin());
        assertEquals(baseMax, statistic.getMax());

        assertEquals(baseMax, statistic.getSum());
        assertEquals(1, statistic.getCount());

        var newValue = baseMax + random.nextInt(2, 1000);

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(intMutableColumn).write(0, newValue);

        aggregatedIntMutableColumn.write(0, newValue);

        chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        statistic = chunkedColumnStatistics.get(0);

        assertEquals(0, statistic.getFirstIndex());
        assertEquals(newValue, statistic.getFirstValue());

        assertEquals(1, statistic.getLastIndex());
        assertEquals(baseMax, statistic.getLastValue());

        assertEquals(baseMax, statistic.getMin());
        assertEquals(newValue, statistic.getMax());

        assertEquals(baseMax + newValue, statistic.getSum());
        assertEquals(2, statistic.getCount());
    }

    @Test
    void testWriteMinValue() {
        var intMutableColumn = mock(IntMutableColumn.class);

        when(intMutableColumn.getTombstone())
                .thenReturn(0);

        var baseMin = random.nextInt(-1000, 1000) + 1;
        var data = new int[]{baseMin, 0};
        when(intMutableColumn.getData())
                .thenReturn(data);

        when(intMutableColumn.size())
                .thenReturn(2);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var statistic = chunkedColumnStatistics.get(0);

        assertEquals(0, statistic.getFirstIndex());
        assertEquals(baseMin, statistic.getFirstValue());

        assertEquals(0, statistic.getLastIndex());
        assertEquals(baseMin, statistic.getLastValue());

        assertEquals(baseMin, statistic.getMin());
        assertEquals(baseMin, statistic.getMax());

        assertEquals(baseMin, statistic.getSum());
        assertEquals(1, statistic.getCount());

        var newValue = baseMin - random.nextInt(2, 1000);

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(intMutableColumn).write(1, newValue);

        aggregatedIntMutableColumn.write(1, newValue);

        chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        statistic = chunkedColumnStatistics.get(0);
        assertEquals(0, statistic.getFirstIndex());
        assertEquals(baseMin, statistic.getFirstValue());

        assertEquals(1, statistic.getLastIndex());
        assertEquals(newValue, statistic.getLastValue());

        assertEquals(newValue, statistic.getMin());
        assertEquals(baseMin, statistic.getMax());

        assertEquals(baseMin + newValue, statistic.getSum());
        assertEquals(2, statistic.getCount());

    }

    @Test
    void testRemoveLastValue() {
        var intMutableColumn = mock(IntMutableColumn.class);

        var tombstone = 0;
        when(intMutableColumn.getTombstone())
                .thenReturn(tombstone);

        var value = random.nextInt(-1000, 1000) + 1;
        var data = new int[]{value};
        when(intMutableColumn.getData())
                .thenReturn(data);

        when(intMutableColumn.size())
                .thenReturn(1);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var statistic = chunkedColumnStatistics.get(0);

        assertEquals(0, statistic.getFirstIndex());
        assertEquals(value, statistic.getFirstValue());

        assertEquals(0, statistic.getLastIndex());
        assertEquals(value, statistic.getLastValue());

        assertEquals(value, statistic.getMin());
        assertEquals(value, statistic.getMax());

        assertEquals(value, statistic.getSum());
        assertEquals(1, statistic.getCount());

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = tombstone;
            return null;
        })
                .when(intMutableColumn).tombstone(anyInt());

        aggregatedIntMutableColumn.tombstone(0);

        chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        assertEquals(-1, statistic.getFirstIndex());
        assertEquals(tombstone, statistic.getFirstValue());

        assertEquals(-1, statistic.getLastIndex());
        assertEquals(tombstone, statistic.getLastValue());

        assertEquals(Integer.MAX_VALUE, statistic.getMin());
        assertEquals(Integer.MIN_VALUE, statistic.getMax());

        assertEquals(0, statistic.getSum());
        assertEquals(0, statistic.getCount());

    }

    @Test
    void testRemoveMiddleValue() {
        var intMutableColumn = mock(IntMutableColumn.class);

        var tombstone = 0;
        when(intMutableColumn.getTombstone())
                .thenReturn(tombstone);

        var value = random.nextInt(-1000, 1000) + 1;
        var data = new int[]{value};
        when(intMutableColumn.getData())
                .thenReturn(data);

        when(intMutableColumn.size())
                .thenReturn(1);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var statistic = chunkedColumnStatistics.get(0);

        assertEquals(0, statistic.getFirstIndex());
        assertEquals(value, statistic.getFirstValue());

        assertEquals(0, statistic.getLastIndex());
        assertEquals(value, statistic.getLastValue());

        assertEquals(value, statistic.getMin());
        assertEquals(value, statistic.getMax());

        assertEquals(value, statistic.getSum());
        assertEquals(1, statistic.getCount());

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = tombstone;
            return null;
        })
                .when(intMutableColumn).tombstone(anyInt());

        aggregatedIntMutableColumn.tombstone(0);

        chunkedColumnStatistics = aggregatedIntMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        assertEquals(-1, statistic.getFirstIndex());
        assertEquals(tombstone, statistic.getFirstValue());

        assertEquals(-1, statistic.getLastIndex());
        assertEquals(tombstone, statistic.getLastValue());

        assertEquals(Integer.MAX_VALUE, statistic.getMin());
        assertEquals(Integer.MIN_VALUE, statistic.getMax());

        assertEquals(0, statistic.getSum());
        assertEquals(0, statistic.getCount());

    }
}