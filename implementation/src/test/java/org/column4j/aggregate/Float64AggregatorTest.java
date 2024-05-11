package org.column4j.aggregate;

import org.column4j.column.impl.mutable.primitive.Float64MutableColumnImpl;
import org.column4j.column.mutable.primitive.Float64MutableColumn;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Float64AggregatorTest {
    private final Random random = new Random();
    private final int columnSize = 1000;
    private final int columnsCount = 12;
    private final int maxChunkSize = 23;
    private final double tombstone = 3.141592653589793;

    @Test
    void minMaxTest() {
        var column = new Float64MutableColumnImpl(maxChunkSize, tombstone);
        double max = Double.MIN_VALUE;
        double min = Double.MAX_VALUE;
        int from = columnSize / 7;
        int to = 9 * columnSize / 11;

        for (int i = 0; i < columnSize; ++i) {
            if (random.nextBoolean()) {
                continue;
            }
            double value = random.nextDouble(Double.MAX_VALUE);
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

        assertEquals(min, Float64Aggregator.min(column, from, to));
        assertEquals(max, Float64Aggregator.max(column, from, to));
    }

    @Test
    void indexOfAnotherTest() {
        final int testedValues = 25;
        var column = new Float64MutableColumnImpl(maxChunkSize, tombstone);
        int from = columnSize / 7;
        int to = 9 * columnSize / 11;

        double defaultValue = random.nextDouble();

        while (defaultValue == tombstone) {
            defaultValue = random.nextDouble();
        }

        for (int i = 0; i < columnSize; ++i) {
            double value = random.nextDouble(Double.MAX_VALUE);
            column.write(i, defaultValue);
            if (random.nextBoolean()) {
                column.write(i, defaultValue);
            }
        }

        int expectedIndexOfAnother = to;
        int expectedLastIndexOfAnother = from;

        for (int i = 0; i < testedValues; i++) {
            int index = random.nextInt(from, to);
            double value = random.nextDouble();
            while (value == defaultValue) {
                value = random.nextDouble();
            }
            column.write(index, value);

            expectedIndexOfAnother = Math.min(expectedIndexOfAnother, index);
            expectedLastIndexOfAnother = Math.max(expectedLastIndexOfAnother, index);
        }

        assertEquals(expectedIndexOfAnother, Float64Aggregator.indexOfAnother(column, defaultValue, from, to));
        assertEquals(expectedLastIndexOfAnother, Float64Aggregator.lastIndexOfAnother(column, defaultValue, from, to));
    }

    @Test
    void mulTwoColumnsTest() {
        var column1 = new Float64MutableColumnImpl(maxChunkSize, tombstone);
        var column2 = new Float64MutableColumnImpl(maxChunkSize, tombstone);
        var expected = new double[columnSize];

        for (int i = 0; i < columnSize; i++) {
            double val1 = random.nextDouble();
            double val2 = random.nextDouble();
            column1.write(i, val1);
            column2.write(i, val2);
            expected[i] = val1 * val2;
        }

        var resColumn = Float64Aggregator.mul(column1, column2, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(resColumn.get(i), expected[i]);
        }
    }

    @Test
    void sumTwoColumnsTest() {
        var column1 = new Float64MutableColumnImpl(maxChunkSize, tombstone);
        var column2 = new Float64MutableColumnImpl(maxChunkSize, tombstone);
        var expected = new double[columnSize];

        for (int i = 0; i < columnSize; i++) {
            double val1 = random.nextDouble();
            double val2 = random.nextDouble();
            column1.write(i, val1);
            column2.write(i, val2);
            expected[i] = val1 + val2;
        }

        var resColumn = Float64Aggregator.sum(column1, column2, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(resColumn.get(i), expected[i]);
        }
    }

    @Test
    void mulManyColumnsTest() {
        var columns = new Float64MutableColumn[columnsCount];
        var expected = new double[columnSize];
        for (int i = 0; i < columnsCount; i++) {
            columns[i] = new Float64MutableColumnImpl(maxChunkSize, tombstone);
            for (int j = 0; j < columnSize; j++) {
                double val = random.nextDouble();
                columns[i].write(j, val);
                expected[j] = i == 0 ? val : expected[j] * val;
            }
        }
        Float64MutableColumn result = Float64Aggregator.mul(columns, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(expected[i], result.get(i));
        }
    }

    @Test
    void sumManyColumnsTest() {
        var columns = new Float64MutableColumn[columnsCount];
        var expected = new double[columnSize];
        for (int i = 0; i < columnsCount; i++) {
            columns[i] = new Float64MutableColumnImpl(maxChunkSize, tombstone);
            for (int j = 0; j < columnSize; j++) {
                double val = random.nextDouble();
                columns[i].write(j, val);
                expected[j] += val;
            }
        }
        Float64MutableColumn result = Float64Aggregator.sum(columns, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(expected[i], result.get(i));
        }
    }
}
