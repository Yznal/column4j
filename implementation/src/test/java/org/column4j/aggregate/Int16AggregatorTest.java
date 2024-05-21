package org.column4j.aggregate;

import org.column4j.column.impl.mutable.primitive.Int16MutableColumnImpl;
import org.column4j.column.mutable.primitive.Int16MutableColumn;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Int16AggregatorTest {
    private final Random random = new Random();
    private final int columnSize = 1000;
    private final int columnsCount = 12;
    private final int maxChunkSize = 23;
    private final short tombstone = 34;

    @Test
    void minMaxTest() {
        var column = new Int16MutableColumnImpl(maxChunkSize, tombstone);
        short max = Short.MIN_VALUE;
        short min = Short.MAX_VALUE;
        int from = columnSize / 7;
        int to = 9 * columnSize / 11;

        for (int i = 0; i < columnSize; ++i) {
            if (random.nextBoolean()) {
                continue;
            }
            short value = (short)random.nextInt();
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

        assertEquals(min, Int16Aggregator.min(column, from, to));
        assertEquals(max, Int16Aggregator.max(column, from, to));
    }

    @Test
    void indexOfAnotherTest() {
        var column = new Int16MutableColumnImpl(maxChunkSize, tombstone);
        for (int i = 0; i < columnSize; i++) {
            column.write(i, (short)2);
        }

        assertEquals(0, Int16Aggregator.indexOfAnother(column, (short)1, 0, columnSize));
        assertEquals(columnSize - 1, Int16Aggregator.lastIndexOfAnother(column, (short)1, 0, columnSize));
        assertEquals(-1, Int16Aggregator.indexOfAnother(column, (short)2, 0, columnSize));
        assertEquals(-1, Int16Aggregator.lastIndexOfAnother(column, (short)2, 0, columnSize));

        int index1 = columnSize / 4;
        int index2 = 3 * columnSize / 4;
        column.write(index1, (short)1);
        column.write(index2, (short)1);

        assertEquals(index1, Int16Aggregator.indexOfAnother(column, (short)2, 0, columnSize));
        assertEquals(index2, Int16Aggregator.lastIndexOfAnother(column, (short)2, 0, columnSize));
    }

    @Test
    void indexOfTest() {
        var column = new Int16MutableColumnImpl(maxChunkSize, tombstone);
        for (int i = 0; i < columnSize; i++) {
            column.write(i, (short)2);
        }

        assertEquals(0, Int16Aggregator.indexOf(column, (short)2, 0, columnSize));
        assertEquals(columnSize - 1, Int16Aggregator.lastIndexOf(column, (short)2, 0, columnSize));
        assertEquals(-1, Int16Aggregator.indexOf(column, (short)1, 0, columnSize));
        assertEquals(-1, Int16Aggregator.lastIndexOf(column, (short)1, 0, columnSize));

        int index1 = columnSize / 4;
        int index2 = 3 * columnSize / 4;
        column.write(index1, (short)1);
        column.write(index2, (short)1);

        assertEquals(index1, Int16Aggregator.indexOf(column, (short)1, 0, columnSize));
        assertEquals(index2, Int16Aggregator.lastIndexOf(column, (short)1, 0, columnSize));
    }

    @Test
    void mulTwoColumnsTest() {
        var column1 = new Int16MutableColumnImpl(maxChunkSize, tombstone);
        var column2 = new Int16MutableColumnImpl(maxChunkSize, tombstone);
        var expected = new short[columnSize];

        for (int i = 0; i < columnSize; i++) {
            short val1 = (short)random.nextInt();
            short val2 = (short)random.nextInt();
            column1.write(i, val1);
            column2.write(i, val2);
            expected[i] = (short) (val1 * val2);
        }

        var resColumn = Int16Aggregator.mul(column1, column2, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(resColumn.get(i), expected[i]);
        }
    }

    @Test
    void sumTwoColumnsTest() {
        var column1 = new Int16MutableColumnImpl(maxChunkSize, tombstone);
        var column2 = new Int16MutableColumnImpl(maxChunkSize, tombstone);
        var expected = new short[columnSize];

        for (int i = 0; i < columnSize; i++) {
            short val1 = (short)random.nextInt();
            short val2 = (short)random.nextInt();
            column1.write(i, val1);
            column2.write(i, val2);
            expected[i] = (short) (val1 + val2);
        }

        var resColumn = Int16Aggregator.sum(column1, column2, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(resColumn.get(i), expected[i]);
        }
    }

    @Test
    void mulManyColumnsTest() {
        var columns = new Int16MutableColumn[columnsCount];
        var expected = new short[columnSize];
        for (int i = 0; i < columnsCount; i++) {
            columns[i] = new Int16MutableColumnImpl(maxChunkSize, tombstone);
            for (int j = 0; j < columnSize; j++) {
                short val = (short)random.nextInt();
                columns[i].write(j, val);
                expected[j] = i == 0 ? val : (short) (expected[j] * val);
            }
        }
        Int16MutableColumn result = Int16Aggregator.mul(columns, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(expected[i], result.get(i));
        }
    }

    @Test
    void sumManyColumnsTest() {
        var columns = new Int16MutableColumn[columnsCount];
        var expected = new short[columnSize];
        for (int i = 0; i < columnsCount; i++) {
            columns[i] = new Int16MutableColumnImpl(maxChunkSize, tombstone);
            for (int j = 0; j < columnSize; j++) {
                short val = (short)random.nextInt();
                columns[i].write(j, val);
                expected[j] += val;
            }
        }
        Int16MutableColumn result = Int16Aggregator.sum(columns, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(expected[i], result.get(i));
        }
    }
}
