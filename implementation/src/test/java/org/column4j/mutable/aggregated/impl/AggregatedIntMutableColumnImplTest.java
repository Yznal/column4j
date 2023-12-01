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
    void testAggregateOnInitWithOneBucket(int dataSize) {
        var tombstone = random.nextInt(-dataSize, dataSize);

        int[] data = new int[dataSize];

        int firstIndex = -1;
        int lastIndex = -1;
        var min = Integer.MAX_VALUE;
        var max = Integer.MIN_VALUE;
        int count = 0;
        var sum = 0;

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
        assertEquals(dataSize, aggregatedIntMutableColumn.getBucketSize());

        var columnStats = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        var intStats = columnStats.get(0);

        if (firstIndex == -1) {
            assertEquals(tombstone, intStats.getFirstValue());
            assertEquals(tombstone, intStats.getLastValue());
            assertEquals(-1, intStats.getFirstIndex());
            assertEquals(-1, intStats.getLastIndex());
        } else {
            assertEquals(firstIndex, intStats.getFirstIndex());
            assertEquals(data[firstIndex], intStats.getFirstValue());

            assertEquals(lastIndex, intStats.getLastIndex());
            assertEquals(data[lastIndex], intStats.getLastValue());
        }

        assertEquals(min, intStats.getMin());
        assertEquals(max, intStats.getMax());
        assertEquals(count, intStats.getCount());
        assertEquals(sum, intStats.getSum());
    }

    @ParameterizedTest
    @ValueSource(ints = {16, 32, 64, 128, 256, 512, 1024})
    void testAggregateOnInitWithSeveralBuckets(int dataSize) {
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

        int bucketSize = random.nextInt(4, dataSize / 2);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, bucketSize);
        assertEquals(bucketSize, aggregatedIntMutableColumn.getBucketSize());

        var columnStats = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        var buckets = Math.max(1, (dataSize + bucketSize / 2) / bucketSize);

        for (int bucket = 0; bucket < buckets; bucket++) {
            int firstIndex = -1;
            int lastIndex = -1;
            var min = Integer.MAX_VALUE;
            var max = Integer.MIN_VALUE;
            int count = 0;
            var sum = 0;

            for (int i = 0; i < bucketSize; i++) {
                var index = bucket * bucketSize + i;
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

            var intStats = columnStats.get(bucket);

            if (firstIndex == -1) {
                assertEquals(tombstone, intStats.getFirstValue());
                assertEquals(tombstone, intStats.getLastIndex());
                assertEquals(-1, intStats.getFirstIndex());
                assertEquals(-1, intStats.getLastIndex());
            } else {
                assertEquals(firstIndex, intStats.getFirstIndex());
                assertEquals(data[firstIndex], intStats.getFirstValue());

                assertEquals(lastIndex, intStats.getLastIndex());
                assertEquals(data[lastIndex], intStats.getLastValue());
            }

            assertEquals(min, intStats.getMin());
            assertEquals(max, intStats.getMax());
            assertEquals(count, intStats.getCount());
            assertEquals(sum, intStats.getSum());
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
        assertEquals(8, aggregatedIntMutableColumn.getBucketSize());

        var columnStats = aggregatedIntMutableColumn.getBucketColumnStatistics();
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
        assertEquals(dataSize, aggregatedIntMutableColumn.getBucketSize());

        var columnStats = aggregatedIntMutableColumn.getBucketColumnStatistics();
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

        var bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        var intStatistic = bucketColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(oldValue, intStatistic.getFirstValue());
        assertEquals(0, intStatistic.getLastIndex());
        assertEquals(oldValue, intStatistic.getLastValue());
        assertEquals(oldValue, intStatistic.getMin());
        assertEquals(oldValue, intStatistic.getMax());
        assertEquals(oldValue, intStatistic.getSum());
        assertEquals(1, intStatistic.getCount());

        var newValue = oldValue + random.nextInt(1, 1000);

        doAnswer(it -> {
            data[0] = newValue;
            return null;
        })
                .when(intMutableColumn).write(0, newValue);

        aggregatedIntMutableColumn.write(0, newValue);

        bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        intStatistic = bucketColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(newValue, intStatistic.getFirstValue());
        assertEquals(0, intStatistic.getLastIndex());
        assertEquals(newValue, intStatistic.getLastValue());
        assertEquals(newValue, intStatistic.getMin());
        assertEquals(newValue, intStatistic.getMax());
        assertEquals(newValue, intStatistic.getSum());
        assertEquals(1, intStatistic.getCount());

    }

    @Test
    void testRewriteMinToMaxInMiddleValue() {
        var intMutableColumn = mock(IntMutableColumn.class);

        when(intMutableColumn.getTombstone())
                .thenReturn(0);

        var oldValue = random.nextInt(-1000, 1000) + 1;
        var otherValue = oldValue + 1;
        var data = new int[]{otherValue, oldValue, otherValue};
        when(intMutableColumn.getData())
                .thenReturn(data);

        when(intMutableColumn.size())
                .thenReturn(3);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, 8);

        var bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        var intStatistic = bucketColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(otherValue, intStatistic.getFirstValue());
        assertEquals(2, intStatistic.getLastIndex());
        assertEquals(otherValue, intStatistic.getLastValue());
        assertEquals(oldValue, intStatistic.getMin());
        assertEquals(otherValue, intStatistic.getMax());
        assertEquals(oldValue * 3L + 2, intStatistic.getSum());
        assertEquals(3, intStatistic.getCount());

        var newValue = oldValue + random.nextInt(2, 1000);

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(intMutableColumn).write(1, newValue);

        aggregatedIntMutableColumn.write(1, newValue);

        bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        intStatistic = bucketColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(otherValue, intStatistic.getFirstValue());
        assertEquals(2, intStatistic.getLastIndex());
        assertEquals(otherValue, intStatistic.getLastValue());
        assertEquals(otherValue, intStatistic.getMin());
        assertEquals(newValue, intStatistic.getMax());
        assertEquals(oldValue * 2L + 2 + newValue, intStatistic.getSum());
        assertEquals(3, intStatistic.getCount());

    }

    @Test
    void testRewriteMinToMinInMiddleValue() {
        var intMutableColumn = mock(IntMutableColumn.class);

        when(intMutableColumn.getTombstone())
                .thenReturn(0);

        var oldValue = random.nextInt(-1000, 1000) + 1;
        var otherValue = oldValue + 1;
        var data = new int[]{otherValue, oldValue, otherValue};
        when(intMutableColumn.getData())
                .thenReturn(data);

        when(intMutableColumn.size())
                .thenReturn(3);

        var aggregatedIntMutableColumn = new AggregatedIntMutableColumnImpl(intMutableColumn, 8);

        var bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        var intStatistic = bucketColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(otherValue, intStatistic.getFirstValue());
        assertEquals(2, intStatistic.getLastIndex());
        assertEquals(otherValue, intStatistic.getLastValue());
        assertEquals(oldValue, intStatistic.getMin());
        assertEquals(otherValue, intStatistic.getMax());
        assertEquals(oldValue * 3L + 2, intStatistic.getSum());
        assertEquals(3, intStatistic.getCount());

        var newValue = oldValue - random.nextInt(2, 1000);

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(intMutableColumn).write(1, newValue);

        aggregatedIntMutableColumn.write(1, newValue);

        bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        intStatistic = bucketColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(otherValue, intStatistic.getFirstValue());
        assertEquals(2, intStatistic.getLastIndex());
        assertEquals(otherValue, intStatistic.getLastValue());
        assertEquals(newValue, intStatistic.getMin());
        assertEquals(otherValue, intStatistic.getMax());
        assertEquals(oldValue * 2L + 2 + newValue, intStatistic.getSum());
        assertEquals(3, intStatistic.getCount());

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

        var bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        var intStatistic = bucketColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(otherValue, intStatistic.getFirstValue());
        assertEquals(2, intStatistic.getLastIndex());
        assertEquals(otherValue, intStatistic.getLastValue());
        assertEquals(otherValue, intStatistic.getMin());
        assertEquals(oldValue, intStatistic.getMax());
        assertEquals(oldValue * 3L - 2, intStatistic.getSum());
        assertEquals(3, intStatistic.getCount());

        var newValue = oldValue - random.nextInt(2, 1000);

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(intMutableColumn).write(1, newValue);

        aggregatedIntMutableColumn.write(1, newValue);

        bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        intStatistic = bucketColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(otherValue, intStatistic.getFirstValue());
        assertEquals(2, intStatistic.getLastIndex());
        assertEquals(otherValue, intStatistic.getLastValue());
        assertEquals(newValue, intStatistic.getMin());
        assertEquals(otherValue, intStatistic.getMax());
        assertEquals(oldValue * 2L - 2 + newValue, intStatistic.getSum());
        assertEquals(3, intStatistic.getCount());

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

        var bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        var intStatistic = bucketColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(otherValue, intStatistic.getFirstValue());
        assertEquals(2, intStatistic.getLastIndex());
        assertEquals(otherValue, intStatistic.getLastValue());
        assertEquals(otherValue, intStatistic.getMin());
        assertEquals(oldValue, intStatistic.getMax());
        assertEquals(oldValue * 3L - 2, intStatistic.getSum());
        assertEquals(3, intStatistic.getCount());

        var newValue = oldValue + random.nextInt(2, 1000);

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(intMutableColumn).write(1, newValue);

        aggregatedIntMutableColumn.write(1, newValue);

        bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        intStatistic = bucketColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(otherValue, intStatistic.getFirstValue());
        assertEquals(2, intStatistic.getLastIndex());
        assertEquals(otherValue, intStatistic.getLastValue());
        assertEquals(otherValue, intStatistic.getMin());
        assertEquals(newValue, intStatistic.getMax());
        assertEquals(oldValue * 2L - 2 + newValue, intStatistic.getSum());
        assertEquals(3, intStatistic.getCount());

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

        var bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        var intStatistic = bucketColumnStatistics.get(0);

        assertEquals(1, intStatistic.getFirstIndex());
        assertEquals(baseMax, intStatistic.getFirstValue());

        assertEquals(1, intStatistic.getLastIndex());
        assertEquals(baseMax, intStatistic.getLastValue());

        assertEquals(baseMax, intStatistic.getMin());
        assertEquals(baseMax, intStatistic.getMax());

        assertEquals(baseMax, intStatistic.getSum());
        assertEquals(1, intStatistic.getCount());

        var newValue = baseMax + random.nextInt(2, 1000);

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(intMutableColumn).write(0, newValue);

        aggregatedIntMutableColumn.write(0, newValue);

        bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        intStatistic = bucketColumnStatistics.get(0);

        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(newValue, intStatistic.getFirstValue());

        assertEquals(1, intStatistic.getLastIndex());
        assertEquals(baseMax, intStatistic.getLastValue());

        assertEquals(baseMax, intStatistic.getMin());
        assertEquals(newValue, intStatistic.getMax());

        assertEquals(baseMax + newValue, intStatistic.getSum());
        assertEquals(2, intStatistic.getCount());
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

        var bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        var intStatistic = bucketColumnStatistics.get(0);

        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(baseMin, intStatistic.getFirstValue());

        assertEquals(0, intStatistic.getLastIndex());
        assertEquals(baseMin, intStatistic.getLastValue());

        assertEquals(baseMin, intStatistic.getMin());
        assertEquals(baseMin, intStatistic.getMax());

        assertEquals(baseMin, intStatistic.getSum());
        assertEquals(1, intStatistic.getCount());

        var newValue = baseMin - random.nextInt(2, 1000);

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(intMutableColumn).write(1, newValue);

        aggregatedIntMutableColumn.write(1, newValue);

        bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        intStatistic = bucketColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(baseMin, intStatistic.getFirstValue());

        assertEquals(1, intStatistic.getLastIndex());
        assertEquals(newValue, intStatistic.getLastValue());

        assertEquals(newValue, intStatistic.getMin());
        assertEquals(baseMin, intStatistic.getMax());

        assertEquals(baseMin + newValue, intStatistic.getSum());
        assertEquals(2, intStatistic.getCount());

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

        var bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        var intStatistic = bucketColumnStatistics.get(0);

        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(value, intStatistic.getFirstValue());

        assertEquals(0, intStatistic.getLastIndex());
        assertEquals(value, intStatistic.getLastValue());

        assertEquals(value, intStatistic.getMin());
        assertEquals(value, intStatistic.getMax());

        assertEquals(value, intStatistic.getSum());
        assertEquals(1, intStatistic.getCount());

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = tombstone;
            return null;
        })
                .when(intMutableColumn).tombstone(anyInt());

        aggregatedIntMutableColumn.tombstone(0);

        bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        assertEquals(-1, intStatistic.getFirstIndex());
        assertEquals(tombstone, intStatistic.getFirstValue());

        assertEquals(-1, intStatistic.getLastIndex());
        assertEquals(tombstone, intStatistic.getLastValue());

        assertEquals(Integer.MAX_VALUE, intStatistic.getMin());
        assertEquals(Integer.MIN_VALUE, intStatistic.getMax());

        assertEquals(0, intStatistic.getSum());
        assertEquals(0, intStatistic.getCount());

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

        var bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        var intStatistic = bucketColumnStatistics.get(0);

        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(value, intStatistic.getFirstValue());

        assertEquals(0, intStatistic.getLastIndex());
        assertEquals(value, intStatistic.getLastValue());

        assertEquals(value, intStatistic.getMin());
        assertEquals(value, intStatistic.getMax());

        assertEquals(value, intStatistic.getSum());
        assertEquals(1, intStatistic.getCount());

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = tombstone;
            return null;
        })
                .when(intMutableColumn).tombstone(anyInt());

        aggregatedIntMutableColumn.tombstone(0);

        bucketColumnStatistics = aggregatedIntMutableColumn.getBucketColumnStatistics();
        assertNotNull(bucketColumnStatistics);

        assertEquals(-1, intStatistic.getFirstIndex());
        assertEquals(tombstone, intStatistic.getFirstValue());

        assertEquals(-1, intStatistic.getLastIndex());
        assertEquals(tombstone, intStatistic.getLastValue());

        assertEquals(Integer.MAX_VALUE, intStatistic.getMin());
        assertEquals(Integer.MIN_VALUE, intStatistic.getMax());

        assertEquals(0, intStatistic.getSum());
        assertEquals(0, intStatistic.getCount());

    }
}