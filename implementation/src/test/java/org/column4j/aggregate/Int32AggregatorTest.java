package org.column4j.aggregate;

import org.column4j.column.impl.mutable.primitive.Int32MutableColumnImpl;
import org.column4j.column.mutable.primitive.Int32MutableColumn;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Int32AggregatorTest {
    private final Random random = new Random();
    private final int columnSize = 1000;
    private final int columnsCount = 12;
    private final int maxChunkSize = 23;
    private final int tombstone = 34;

    @Test
    void minMaxTest() {
        var column = new Int32MutableColumnImpl(maxChunkSize, tombstone);
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        int from = columnSize / 7;
        int to = 9 * columnSize / 11;

        for (int i = 0; i < columnSize; ++i) {
            if (random.nextBoolean()) {
                continue;
            }
            int value = random.nextInt();
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

        assertEquals(min, Int32Aggregator.min(column, from, to));
        assertEquals(max, Int32Aggregator.max(column, from, to));
    }

    @Test
    void indexOfAnotherTest() {
        var column = new Int32MutableColumnImpl(maxChunkSize, tombstone);
        for (int i = 0; i < columnSize; i++) {
            column.write(i, 2);
        }

        assertEquals(0, Int32Aggregator.indexOfAnother(column, 1, 0, columnSize));
        assertEquals(columnSize - 1, Int32Aggregator.lastIndexOfAnother(column, 1, 0, columnSize));
        assertEquals(-1, Int32Aggregator.indexOfAnother(column, 2, 0, columnSize));
        assertEquals(-1, Int32Aggregator.lastIndexOfAnother(column, 2, 0, columnSize));

        int index1 = columnSize / 4;
        int index2 = 3 * columnSize / 4;
        column.write(index1, 1);
        column.write(index2, 1);

        assertEquals(index1, Int32Aggregator.indexOfAnother(column, 2, 0, columnSize));
        assertEquals(index2, Int32Aggregator.lastIndexOfAnother(column, 2, 0, columnSize));
    }

    @Test
    void indexOfTest() {
        var column = new Int32MutableColumnImpl(maxChunkSize, tombstone);
        for (int i = 0; i < columnSize; i++) {
            column.write(i, 2);
        }

        assertEquals(0, Int32Aggregator.indexOf(column, 2, 0, columnSize));
        assertEquals(columnSize - 1, Int32Aggregator.lastIndexOf(column, 2, 0, columnSize));
        assertEquals(-1, Int32Aggregator.indexOf(column, 1, 0, columnSize));
        assertEquals(-1, Int32Aggregator.lastIndexOf(column, 1, 0, columnSize));

        int index1 = columnSize / 4;
        int index2 = 3 * columnSize / 4;
        column.write(index1, 1);
        column.write(index2, 1);

        assertEquals(index1, Int32Aggregator.indexOf(column, 1, 0, columnSize));
        assertEquals(index2, Int32Aggregator.lastIndexOf(column, 1, 0, columnSize));
    }

    @Test
    void mulTwoColumnsTest() {
        var column1 = new Int32MutableColumnImpl(maxChunkSize, tombstone);
        var column2 = new Int32MutableColumnImpl(maxChunkSize, tombstone);
        var expected = new int[columnSize];

        for (int i = 0; i < columnSize; i++) {
            int val1 = random.nextInt();
            int val2 = random.nextInt();
            column1.write(i, val1);
            column2.write(i, val2);
            expected[i] = (val1 * val2);
        }

        var resColumn = Int32Aggregator.mul(column1, column2, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(resColumn.get(i), expected[i]);
        }
    }

    @Test
    void sumTwoColumnsTest() {
        var column1 = new Int32MutableColumnImpl(maxChunkSize, tombstone);
        var column2 = new Int32MutableColumnImpl(maxChunkSize, tombstone);
        var expected = new int[columnSize];

        for (int i = 0; i < columnSize; i++) {
            int val1 = random.nextInt();
            int val2 = random.nextInt();
            column1.write(i, val1);
            column2.write(i, val2);
            expected[i] = val1 + val2;
        }

        var resColumn = Int32Aggregator.sum(column1, column2, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(resColumn.get(i), expected[i]);
        }
    }

    @Test
    void mulManyColumnsTest() {
        var columns = new Int32MutableColumn[columnsCount];
        var expected = new int[columnSize];
        for (int i = 0; i < columnsCount; i++) {
            columns[i] = new Int32MutableColumnImpl(maxChunkSize, tombstone);
            for (int j = 0; j < columnSize; j++) {
                int val = random.nextInt();
                columns[i].write(j, val);
                expected[j] = i == 0 ? val : (expected[j] * val);
            }
        }
        Int32MutableColumn result = Int32Aggregator.mul(columns, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(expected[i], result.get(i));
        }
    }

    @Test
    void sumManyColumnsTest() {
        var columns = new Int32MutableColumn[columnsCount];
        var expected = new int[columnSize];
        for (int i = 0; i < columnsCount; i++) {
            columns[i] = new Int32MutableColumnImpl(maxChunkSize, tombstone);
            for (int j = 0; j < columnSize; j++) {
                int val = random.nextInt();
                columns[i].write(j, val);
                expected[j] += val;
            }
        }
        Int32MutableColumn result = Int32Aggregator.sum(columns, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(expected[i], result.get(i));
        }
    }
}
