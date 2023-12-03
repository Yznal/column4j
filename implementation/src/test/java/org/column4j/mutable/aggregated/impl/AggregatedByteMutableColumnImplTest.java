package org.column4j.mutable.aggregated.impl;

import org.column4j.ColumnType;
import org.column4j.mutable.primitive.ByteMutableColumn;
import org.junit.jupiter.api.Disabled;
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
@Disabled("Need to figure out how to evade overflow on sum")
class AggregatedByteMutableColumnImplTest {
    private final Random random = new Random();

    @ParameterizedTest
    @ValueSource(bytes = {1, 2, 4, 8, 16, 32, 64, 127})
    void testAggregateOnInitWithOneChunk(byte dataSize) {
        var tombstone = (byte) random.nextInt(-dataSize, dataSize);

        var data = new byte[dataSize];

        int firstIndex = -1;
        int lastIndex = -1;
        var min = Byte.MAX_VALUE;
        var max = Byte.MIN_VALUE;
        int count = 0;
        long sum = 0;

        for (int i = 0; i < dataSize; i++) {
            data[i] = (byte) random.nextInt(-dataSize, dataSize);
            var value = data[i];
            if (value == tombstone) {
                continue;
            }
            min = min < value ? min : value;
            max = max > value ? max : value;
            sum += value;
            count++;
            if (firstIndex == -1) {
                firstIndex = i;
            }
            lastIndex = i;
        }

        var mutableColumn = mock(ByteMutableColumn.class);

        when(mutableColumn.getTombstone())
                .thenReturn(tombstone);

        when(mutableColumn.getData())
                .thenReturn(data);

        when(mutableColumn.size())
                .thenReturn(data.length);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(mutableColumn, dataSize);
        assertEquals(dataSize, aggregatedByteMutableColumn.getChunkSize());

        var columnStats = aggregatedByteMutableColumn.getChunkedColumnStatistics();
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
    @ValueSource(bytes = {16, 32, 64, 127})
    void testAggregateOnInitWithSeveralChunks(byte dataSize) {
        var tombstone = (byte) random.nextInt(-dataSize, dataSize);

        var data = new byte[dataSize];

        for (int i = 0; i < dataSize; i++) {
            data[i] = (byte) random.nextInt(-dataSize, dataSize);
        }

        var byteMutableColumn = mock(ByteMutableColumn.class);

        when(byteMutableColumn.getTombstone())
                .thenReturn(tombstone);

        when(byteMutableColumn.getData())
                .thenReturn(data);

        when(byteMutableColumn.size())
                .thenReturn(data.length);

        int chunkSize = random.nextInt(4, dataSize / 2);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(byteMutableColumn, chunkSize);
        assertEquals(chunkSize, aggregatedByteMutableColumn.getChunkSize());

        var columnStats = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        var chunks = Math.max(1, (dataSize + chunkSize / 2) / chunkSize);

        for (int chunk = 0; chunk < chunks; chunk++) {
            int firstIndex = -1;
            int lastIndex = -1;
            var min = Byte.MAX_VALUE;
            var max = Byte.MIN_VALUE;
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
                min = min < value ? min : value;
                max = max > value ? max : value;
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
        var byteMutableColumn = mock(ByteMutableColumn.class);

        var tombstone = (byte) random.nextInt(-100, 100);
        when(byteMutableColumn.getTombstone())
                .thenReturn(tombstone);

        when(byteMutableColumn.getData())
                .thenReturn(new byte[0]);

        when(byteMutableColumn.size())
                .thenReturn(0);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(byteMutableColumn, 8);
        assertEquals(8, aggregatedByteMutableColumn.getChunkSize());

        var columnStats = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        var statistic = columnStats.get(0);

        assertEquals(tombstone, statistic.getFirstValue());
        assertEquals(tombstone, statistic.getLastValue());
        assertEquals(-1, statistic.getFirstIndex());
        assertEquals(-1, statistic.getLastIndex());

        assertEquals(Byte.MAX_VALUE, statistic.getMin());
        assertEquals(Byte.MIN_VALUE, statistic.getMax());
        assertEquals(0, statistic.getCount());
        assertEquals(0, statistic.getSum());
    }

    @ParameterizedTest
    @ValueSource(bytes = {1, 2, 4, 8, 16, 32, 64, 127})
    void testAggregateOnInitWithTombstoneOnlyColumn(int dataSize) {
        var tombstone = (byte) random.nextInt(-dataSize, dataSize);

        var data = new byte[dataSize];
        Arrays.fill(data, tombstone);

        var byteMutableColumn = mock(ByteMutableColumn.class);

        when(byteMutableColumn.getTombstone())
                .thenReturn(tombstone);

        when(byteMutableColumn.getData())
                .thenReturn(data);

        when(byteMutableColumn.size())
                .thenReturn(data.length);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(byteMutableColumn, dataSize);
        assertEquals(dataSize, aggregatedByteMutableColumn.getChunkSize());

        var columnStats = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(columnStats);
        assertFalse(columnStats.isEmpty());

        var statistic = columnStats.get(0);

        assertEquals(tombstone, statistic.getFirstValue());
        assertEquals(tombstone, statistic.getLastValue());
        assertEquals(-1, statistic.getFirstIndex());
        assertEquals(-1, statistic.getLastIndex());

        assertEquals(Byte.MAX_VALUE, statistic.getMin());
        assertEquals(Byte.MIN_VALUE, statistic.getMax());
        assertEquals(0, statistic.getCount());
        assertEquals(0, statistic.getSum());
    }

    @Test
    void testDefaultWrappingMethodCall() {
        var byteMutableColumn = mock(ByteMutableColumn.class);

        var tombstone = (byte) random.nextInt(-100, 100);
        when(byteMutableColumn.getTombstone())
                .thenReturn(tombstone);


        var data = new byte[0];
        when(byteMutableColumn.getData())
                .thenReturn(data);

        when(byteMutableColumn.size())
                .thenReturn(0);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(byteMutableColumn, 8);

        when(byteMutableColumn.getData())
                .thenReturn(data);

        assertEquals(data, aggregatedByteMutableColumn.getData());

        assertEquals(tombstone, aggregatedByteMutableColumn.getTombstone());

        var size = random.nextInt();
        when(byteMutableColumn.size())
                .thenReturn(size);

        assertEquals(size, aggregatedByteMutableColumn.size());
        var firstRowIndex = random.nextInt();
        when(byteMutableColumn.firstRowIndex())
                .thenReturn(firstRowIndex);

        assertEquals(firstRowIndex, aggregatedByteMutableColumn.firstRowIndex());

        var capacity = random.nextInt();
        when(byteMutableColumn.capacity())
                .thenReturn(capacity);

        assertEquals(capacity, aggregatedByteMutableColumn.capacity());

        var columnType = mock(ColumnType.class);
        when(byteMutableColumn.type())
                .thenReturn(columnType);

        assertEquals(columnType, aggregatedByteMutableColumn.type());

    }

    @Test
    void testRewriteValue() {
        var byteMutableColumn = mock(ByteMutableColumn.class);

        when(byteMutableColumn.getTombstone())
                .thenReturn((byte) 0);

        var oldValue = (byte) random.nextInt(-100, 100);
        if (oldValue == 0) {
            oldValue++;
        }
        var data = new byte[]{oldValue};
        when(byteMutableColumn.getData())
                .thenReturn(data);

        when(byteMutableColumn.size())
                .thenReturn(1);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(byteMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var intStatistic = chunkedColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(oldValue, intStatistic.getFirstValue());
        assertEquals(0, intStatistic.getLastIndex());
        assertEquals(oldValue, intStatistic.getLastValue());
        assertEquals(oldValue, intStatistic.getMin());
        assertEquals(oldValue, intStatistic.getMax());
        assertEquals(oldValue, intStatistic.getSum());
        assertEquals(1, intStatistic.getCount());

        var newValue = (byte) (oldValue + random.nextInt(1, 20));

        doAnswer(it -> {
            data[0] = newValue;
            return null;
        })
                .when(byteMutableColumn).write(0, newValue);

        aggregatedByteMutableColumn.write(0, newValue);

        chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        intStatistic = chunkedColumnStatistics.get(0);
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
        var byteMutableColumn = mock(ByteMutableColumn.class);

        when(byteMutableColumn.getTombstone())
                .thenReturn((byte) 0);

        var oldValue = (byte) random.nextInt(-100, 100);
        if (oldValue == 0) {
            oldValue++;
        }
        var otherValue = (byte) (oldValue + 1);
        var data = new byte[]{otherValue, oldValue, otherValue};
        when(byteMutableColumn.getData())
                .thenReturn(data);

        when(byteMutableColumn.size())
                .thenReturn(3);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(byteMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var intStatistic = chunkedColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(otherValue, intStatistic.getFirstValue());
        assertEquals(2, intStatistic.getLastIndex());
        assertEquals(otherValue, intStatistic.getLastValue());
        assertEquals(oldValue, intStatistic.getMin());
        assertEquals(otherValue, intStatistic.getMax());
        assertEquals(oldValue * 3L + 2, intStatistic.getSum());
        assertEquals(3, intStatistic.getCount());

        var newValue = (byte) (oldValue + random.nextInt(2, 20));

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(byteMutableColumn).write(1, newValue);

        aggregatedByteMutableColumn.write(1, newValue);

        chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        intStatistic = chunkedColumnStatistics.get(0);
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
        var byteMutableColumn = mock(ByteMutableColumn.class);

        when(byteMutableColumn.getTombstone())
                .thenReturn((byte) 0);

        var oldValue = (byte) random.nextInt(-100, 100);
        if (oldValue == 0) {
            oldValue++;
        }
        var otherValue = (byte) (oldValue + 1);
        var data = new byte[]{otherValue, oldValue, otherValue};
        when(byteMutableColumn.getData())
                .thenReturn(data);

        when(byteMutableColumn.size())
                .thenReturn(3);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(byteMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var intStatistic = chunkedColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(otherValue, intStatistic.getFirstValue());
        assertEquals(2, intStatistic.getLastIndex());
        assertEquals(otherValue, intStatistic.getLastValue());
        assertEquals(oldValue, intStatistic.getMin());
        assertEquals(otherValue, intStatistic.getMax());
        assertEquals(oldValue * 3L + 2, intStatistic.getSum());
        assertEquals(3, intStatistic.getCount());

        var newValue = (byte) (oldValue - random.nextInt(2, 20));

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(byteMutableColumn).write(1, newValue);

        aggregatedByteMutableColumn.write(1, newValue);

        chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        intStatistic = chunkedColumnStatistics.get(0);
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
        var byteMutableColumn = mock(ByteMutableColumn.class);

        when(byteMutableColumn.getTombstone())
                .thenReturn((byte) 0);

        var oldValue = (byte) random.nextInt(-100, 100);
        if (oldValue == 0) {
            oldValue++;
        }
        var otherValue = (byte) (oldValue - 1);
        var data = new byte[]{otherValue, oldValue, otherValue};
        when(byteMutableColumn.getData())
                .thenReturn(data);

        when(byteMutableColumn.size())
                .thenReturn(3);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(byteMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
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

        var newValue = (byte) (oldValue - random.nextInt(2, 20));

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(byteMutableColumn).write(1, newValue);

        aggregatedByteMutableColumn.write(1, newValue);

        chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
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
        var byteMutableColumn = mock(ByteMutableColumn.class);

        when(byteMutableColumn.getTombstone())
                .thenReturn((byte) 0);

        var oldValue = (byte) random.nextInt(-100, 100);
        if (oldValue == 0) {
            oldValue++;
        }
        var otherValue = (byte) (oldValue - 1);
        var data = new byte[]{otherValue, oldValue, otherValue};
        when(byteMutableColumn.getData())
                .thenReturn(data);

        when(byteMutableColumn.size())
                .thenReturn(3);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(byteMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var intStatistic = chunkedColumnStatistics.get(0);
        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(otherValue, intStatistic.getFirstValue());
        assertEquals(2, intStatistic.getLastIndex());
        assertEquals(otherValue, intStatistic.getLastValue());
        assertEquals(otherValue, intStatistic.getMin());
        assertEquals(oldValue, intStatistic.getMax());
        assertEquals(oldValue * 3L - 2, intStatistic.getSum());
        assertEquals(3, intStatistic.getCount());

        var newValue = (byte) (oldValue + random.nextInt(2, 20));

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(byteMutableColumn).write(1, newValue);

        aggregatedByteMutableColumn.write(1, newValue);

        chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        intStatistic = chunkedColumnStatistics.get(0);
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
        var byteMutableColumn = mock(ByteMutableColumn.class);

        when(byteMutableColumn.getTombstone())
                .thenReturn((byte) 0);

        var baseMax = (byte) random.nextInt(-100, 100);
        if (baseMax == 0) {
            baseMax++;
        }
        var data = new byte[]{0, baseMax};
        when(byteMutableColumn.getData())
                .thenReturn(data);

        when(byteMutableColumn.size())
                .thenReturn(2);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(byteMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var intStatistic = chunkedColumnStatistics.get(0);

        assertEquals(1, intStatistic.getFirstIndex());
        assertEquals(baseMax, intStatistic.getFirstValue());

        assertEquals(1, intStatistic.getLastIndex());
        assertEquals(baseMax, intStatistic.getLastValue());

        assertEquals(baseMax, intStatistic.getMin());
        assertEquals(baseMax, intStatistic.getMax());

        assertEquals(baseMax, intStatistic.getSum());
        assertEquals(1, intStatistic.getCount());

        var newValue = (byte) (baseMax + random.nextInt(2, 20));

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(byteMutableColumn).write(0, newValue);

        aggregatedByteMutableColumn.write(0, newValue);

        chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        intStatistic = chunkedColumnStatistics.get(0);

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
        var byteMutableColumn = mock(ByteMutableColumn.class);

        when(byteMutableColumn.getTombstone())
                .thenReturn((byte) 0);

        var baseMin = (byte) (random.nextInt(-100, 100));
        if (baseMin == 0) {
            baseMin++;
        }
        var data = new byte[]{baseMin, 0};
        when(byteMutableColumn.getData())
                .thenReturn(data);

        when(byteMutableColumn.size())
                .thenReturn(2);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(byteMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var intStatistic = chunkedColumnStatistics.get(0);

        assertEquals(0, intStatistic.getFirstIndex());
        assertEquals(baseMin, intStatistic.getFirstValue());

        assertEquals(0, intStatistic.getLastIndex());
        assertEquals(baseMin, intStatistic.getLastValue());

        assertEquals(baseMin, intStatistic.getMin());
        assertEquals(baseMin, intStatistic.getMax());

        assertEquals(baseMin, intStatistic.getSum());
        assertEquals(1, intStatistic.getCount());

        var newValue = (byte) (baseMin - random.nextInt(2, 20));

        doAnswer(it -> {
            data[(int) it.getArgument(0)] = newValue;
            return null;
        })
                .when(byteMutableColumn).write(1, newValue);

        aggregatedByteMutableColumn.write(1, newValue);

        chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        intStatistic = chunkedColumnStatistics.get(0);
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
        var byteMutableColumn = mock(ByteMutableColumn.class);

        byte tombstone = 0;
        when(byteMutableColumn.getTombstone())
                .thenReturn(tombstone);

        var value = (byte) random.nextInt(-1000, 1000);
        if (value == 0) {
            value++;
        }
        var data = new byte[]{value};
        when(byteMutableColumn.getData())
                .thenReturn(data);

        when(byteMutableColumn.size())
                .thenReturn(1);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(byteMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var intStatistic = chunkedColumnStatistics.get(0);

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
                .when(byteMutableColumn).tombstone(anyInt());

        aggregatedByteMutableColumn.tombstone(0);

        chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        assertEquals(-1, intStatistic.getFirstIndex());
        assertEquals(tombstone, intStatistic.getFirstValue());

        assertEquals(-1, intStatistic.getLastIndex());
        assertEquals(tombstone, intStatistic.getLastValue());

        assertEquals(Byte.MAX_VALUE, intStatistic.getMin());
        assertEquals(Byte.MIN_VALUE, intStatistic.getMax());

        assertEquals(0, intStatistic.getSum());
        assertEquals(0, intStatistic.getCount());

    }

    @Test
    void testRemoveMiddleValue() {
        var byteMutableColumn = mock(ByteMutableColumn.class);

        byte tombstone = 0;
        when(byteMutableColumn.getTombstone())
                .thenReturn(tombstone);

        var value = (byte) random.nextInt(-100, 100);
        if (value == 0) {
            value++;
        }
        var data = new byte[]{value};
        when(byteMutableColumn.getData())
                .thenReturn(data);

        when(byteMutableColumn.size())
                .thenReturn(1);

        var aggregatedByteMutableColumn = new AggregatedByteMutableColumnImpl(byteMutableColumn, 8);

        var chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        var intStatistic = chunkedColumnStatistics.get(0);

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
                .when(byteMutableColumn).tombstone(anyInt());

        aggregatedByteMutableColumn.tombstone(0);

        chunkedColumnStatistics = aggregatedByteMutableColumn.getChunkedColumnStatistics();
        assertNotNull(chunkedColumnStatistics);

        assertEquals(-1, intStatistic.getFirstIndex());
        assertEquals(tombstone, intStatistic.getFirstValue());

        assertEquals(-1, intStatistic.getLastIndex());
        assertEquals(tombstone, intStatistic.getLastValue());

        assertEquals(Byte.MAX_VALUE, intStatistic.getMin());
        assertEquals(Byte.MIN_VALUE, intStatistic.getMax());

        assertEquals(0, intStatistic.getSum());
        assertEquals(0, intStatistic.getCount());

    }
}