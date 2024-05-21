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
        var column = new Int64MutableColumnImpl(maxChunkSize, tombstone);
        for (int i = 0; i < columnSize; i++) {
            column.write(i, 2);
        }

        assertEquals(0, Int64Aggregator.indexOfAnother(column, 1, 0, columnSize));
        assertEquals(columnSize - 1, Int64Aggregator.lastIndexOfAnother(column, 1, 0, columnSize));
        assertEquals(-1, Int64Aggregator.indexOfAnother(column, 2, 0, columnSize));
        assertEquals(-1, Int64Aggregator.lastIndexOfAnother(column, 2, 0, columnSize));

        int index1 = columnSize / 4;
        int index2 = 3 * columnSize / 4;
        column.write(index1, 1);
        column.write(index2, 1);

        assertEquals(index1, Int64Aggregator.indexOfAnother(column, 2, 0, columnSize));
        assertEquals(index2, Int64Aggregator.lastIndexOfAnother(column, 2, 0, columnSize));
    }

    @Test
    void indexOfTest() {
        var column = new Int64MutableColumnImpl(maxChunkSize, tombstone);
        for (int i = 0; i < columnSize; i++) {
            column.write(i, 2);
        }

        assertEquals(0, Int64Aggregator.indexOf(column, 2, 0, columnSize));
        assertEquals(columnSize - 1, Int64Aggregator.lastIndexOf(column, 2, 0, columnSize));
        assertEquals(-1, Int64Aggregator.indexOf(column, 1, 0, columnSize));
        assertEquals(-1, Int64Aggregator.lastIndexOf(column, 1, 0, columnSize));

        int index1 = columnSize / 4;
        int index2 = 3 * columnSize / 4;
        column.write(index1, 1);
        column.write(index2, 1);

        assertEquals(index1, Int64Aggregator.indexOf(column, 1, 0, columnSize));
        assertEquals(index2, Int64Aggregator.lastIndexOf(column, 1, 0, columnSize));
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
