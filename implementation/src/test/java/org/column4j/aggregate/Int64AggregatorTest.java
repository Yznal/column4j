package org.column4j.aggregate;

import org.column4j.column.impl.mutable.primitive.Int64MutableColumnImpl;
import org.column4j.column.mutable.primitive.Int64MutableColumn;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Int64AggregatorTest {
    private final Random random = new Random();
    private final int columnSize = 1000;
    private final int columnsCount = 12;
    private final int maxChunkSize = 23;
    private final long tombstone = 34;

    @Test
    void minMaxTest() {
        var column = new Int64MutableColumnImpl(maxChunkSize, tombstone);
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;
        int from = columnSize / 7;
        int to = 9 * columnSize / 11;

        for (int i = 0; i < columnSize; ++i) {
            if (random.nextBoolean()) {
                continue;
            }
            long value = random.nextLong();
            column.write(i, value);

            if (i >= from && i < to) {
                if (value > max) {
                    max = value;
                }
                if (value < min) {
                    min = value;
                }
            }
        }

        assertEquals(min, Int64Aggregator.min(column, from, to));
        assertEquals(max, Int64Aggregator.max(column, from, to));
    }

    @Test
    void indexOfAnotherTest() {
        final int testedValues = 25;
        var column = new Int64MutableColumnImpl(maxChunkSize, tombstone);
        int from = columnSize / 7;
        int to = 9 * columnSize / 11;

        long defaultValue = random.nextLong();

        while (defaultValue == tombstone) {
            defaultValue = random.nextLong();
        }

        for (int i = 0; i < columnSize; ++i) {
            long value = random.nextLong();
            column.write(i, defaultValue);
            if (random.nextBoolean()) {
                column.write(i, defaultValue);
            }
        }

        int expectedIndexOfAnother = to;
        int expectedLastIndexOfAnother = from;

        for (int i = 0; i < testedValues; i++) {
            int index = random.nextInt(from, to);
            long value = random.nextLong();
            while (value == defaultValue) {
                value = random.nextLong();
            }
            column.write(index, value);

            expectedIndexOfAnother = Math.min(expectedIndexOfAnother, index);
            expectedLastIndexOfAnother = Math.max(expectedLastIndexOfAnother, index);
        }

        assertEquals(expectedIndexOfAnother, Int64Aggregator.indexOfAnother(column, defaultValue, from, to));
        assertEquals(expectedLastIndexOfAnother, Int64Aggregator.lastIndexOfAnother(column, defaultValue, from, to));
    }

    @Test
    void mulTwoColumnsTest() {
        var column1 = new Int64MutableColumnImpl(maxChunkSize, tombstone);
        var column2 = new Int64MutableColumnImpl(maxChunkSize, tombstone);
        var expected = new long[columnSize];

        for (int i = 0; i < columnSize; i++) {
            long val1 = random.nextLong();
            long val2 = random.nextLong();
            column1.write(i, val1);
            column2.write(i, val2);
            expected[i] = val1 * val2;
        }

        var resColumn = Int64Aggregator.mul(column1, column2, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(resColumn.get(i), expected[i]);
        }
    }

    @Test
    void sumTwoColumnsTest() {
        var column1 = new Int64MutableColumnImpl(maxChunkSize, tombstone);
        var column2 = new Int64MutableColumnImpl(maxChunkSize, tombstone);
        var expected = new long[columnSize];

        for (int i = 0; i < columnSize; i++) {
            long val1 = random.nextLong();
            long val2 = random.nextLong();
            column1.write(i, val1);
            column2.write(i, val2);
            expected[i] = val1 + val2;
        }

        var resColumn = Int64Aggregator.sum(column1, column2, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(resColumn.get(i), expected[i]);
        }
    }

    @Test
    void mulManyColumnsTest() {
        var columns = new Int64MutableColumn[columnsCount];
        var expected = new long[columnSize];
        for (int i = 0; i < columnsCount; i++) {
            columns[i] = new Int64MutableColumnImpl(maxChunkSize, tombstone);
            for (int j = 0; j < columnSize; j++) {
                long val = random.nextLong();
                columns[i].write(j, val);
                expected[j] = i == 0 ? val : expected[j] * val;
            }
        }
        Int64MutableColumn result = Int64Aggregator.mul(columns, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(expected[i], result.get(i));
        }
    }

    @Test
    void sumManyColumnsTest() {
        var columns = new Int64MutableColumn[columnsCount];
        var expected = new long[columnSize];
        for (int i = 0; i < columnsCount; i++) {
            columns[i] = new Int64MutableColumnImpl(maxChunkSize, tombstone);
            for (int j = 0; j < columnSize; j++) {
                long val = random.nextLong();
                columns[i].write(j, val);
                expected[j] += val;
            }
        }
        Int64MutableColumn result = Int64Aggregator.sum(columns, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(expected[i], result.get(i));
        }
    }
}
