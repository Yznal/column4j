package org.column4j.aggregate;

import org.column4j.column.impl.mutable.primitive.Int8MutableColumnImpl;
import org.column4j.column.mutable.primitive.Int8MutableColumn;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Int8AggregatorTest {
    private final Random random = new Random();
    private final int columnSize = 1000;
    private final int columnsCount = 12;
    private final int maxChunkSize = 23;
    private final byte tombstone = 34;

    @Test
    void minMaxTest() {
        var column = new Int8MutableColumnImpl(maxChunkSize, tombstone);
        byte max = Byte.MIN_VALUE;
        byte min = Byte.MAX_VALUE;
        int from = columnSize / 7;
        int to = 9 * columnSize / 11;

        for (int i = 0; i < columnSize; ++i) {
            if (random.nextBoolean()) {
                continue;
            }
            byte value = (byte)random.nextInt();
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

        assertEquals(min, Int8Aggregator.min(column, from, to));
        assertEquals(max, Int8Aggregator.max(column, from, to));
    }

    @Test
    void indexOfAnotherTest() {
        var column = new Int8MutableColumnImpl(maxChunkSize, tombstone);
        for (int i = 0; i < columnSize; i++) {
            column.write(i, (byte)2);
        }

        assertEquals(0, Int8Aggregator.indexOfAnother(column, (byte)1, 0, columnSize));
        assertEquals(columnSize - 1, Int8Aggregator.lastIndexOfAnother(column, (byte)1, 0, columnSize));
        assertEquals(-1, Int8Aggregator.indexOfAnother(column, (byte)2, 0, columnSize));
        assertEquals(-1, Int8Aggregator.lastIndexOfAnother(column, (byte)2, 0, columnSize));

        int index1 = columnSize / 4;
        int index2 = 3 * columnSize / 4;
        column.write(index1, (byte)1);
        column.write(index2, (byte)1);

        assertEquals(index1, Int8Aggregator.indexOfAnother(column, (byte)2, 0, columnSize));
        assertEquals(index2, Int8Aggregator.lastIndexOfAnother(column, (byte)2, 0, columnSize));
    }

    @Test
    void indexOfTest() {
        var column = new Int8MutableColumnImpl(maxChunkSize, tombstone);
        for (int i = 0; i < columnSize; i++) {
            column.write(i, (byte)2);
        }

        assertEquals(0, Int8Aggregator.indexOf(column, (byte)2, 0, columnSize));
        assertEquals(columnSize - 1, Int8Aggregator.lastIndexOf(column, (byte)2, 0, columnSize));
        assertEquals(-1, Int8Aggregator.indexOf(column, (byte)1, 0, columnSize));
        assertEquals(-1, Int8Aggregator.lastIndexOf(column, (byte)1, 0, columnSize));

        int index1 = columnSize / 4;
        int index2 = 3 * columnSize / 4;
        column.write(index1, (byte)1);
        column.write(index2, (byte)1);

        assertEquals(index1, Int8Aggregator.indexOf(column, (byte)1, 0, columnSize));
        assertEquals(index2, Int8Aggregator.lastIndexOf(column, (byte)1, 0, columnSize));
    }

    @Test
    void mulTwoColumnsTest() {
        var column1 = new Int8MutableColumnImpl(maxChunkSize, tombstone);
        var column2 = new Int8MutableColumnImpl(maxChunkSize, tombstone);
        var expected = new byte[columnSize];

        for (int i = 0; i < columnSize; i++) {
            byte val1 = (byte)random.nextInt();
            byte val2 = (byte)random.nextInt();
            column1.write(i, val1);
            column2.write(i, val2);
            expected[i] = (byte) (val1 * val2);
        }

        var resColumn = Int8Aggregator.mul(column1, column2, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(resColumn.get(i), expected[i]);
        }
    }

    @Test
    void sumTwoColumnsTest() {
        var column1 = new Int8MutableColumnImpl(maxChunkSize, tombstone);
        var column2 = new Int8MutableColumnImpl(maxChunkSize, tombstone);
        var expected = new byte[columnSize];

        for (int i = 0; i < columnSize; i++) {
            byte val1 = (byte)random.nextInt();
            byte val2 = (byte)random.nextInt();
            column1.write(i, val1);
            column2.write(i, val2);
            expected[i] = (byte) (val1 + val2);
        }

        var resColumn = Int8Aggregator.sum(column1, column2, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(resColumn.get(i), expected[i]);
        }
    }

    @Test
    void mulManyColumnsTest() {
        var columns = new Int8MutableColumn[columnsCount];
        var expected = new byte[columnSize];
        for (int i = 0; i < columnsCount; i++) {
            columns[i] = new Int8MutableColumnImpl(maxChunkSize, tombstone);
            for (int j = 0; j < columnSize; j++) {
                byte val = (byte)random.nextInt();
                columns[i].write(j, val);
                expected[j] = i == 0 ? val : (byte) (expected[j] * val);
            }
        }
        Int8MutableColumn result = Int8Aggregator.mul(columns, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(expected[i], result.get(i));
        }
    }

    @Test
    void sumManyColumnsTest() {
        var columns = new Int8MutableColumn[columnsCount];
        var expected = new byte[columnSize];
        for (int i = 0; i < columnsCount; i++) {
            columns[i] = new Int8MutableColumnImpl(maxChunkSize, tombstone);
            for (int j = 0; j < columnSize; j++) {
                byte val = (byte)random.nextInt();
                columns[i].write(j, val);
                expected[j] += val;
            }
        }
        Int8MutableColumn result = Int8Aggregator.sum(columns, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(expected[i], result.get(i));
        }
    }
}