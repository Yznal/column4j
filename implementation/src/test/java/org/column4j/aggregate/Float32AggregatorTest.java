package org.column4j.aggregate;

import org.column4j.column.mutable.primitive.Float32MutableColumn;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.column4j.column.impl.mutable.primitive.Float32MutableColumnImpl;

class Float32AggregatorTest {
    private final Random random = new Random();
    private final int columnSize = 1000;
    private final int columnsCount = 12;
    private final int maxChunkSize = 23;
    private final float tombstone = (float)3.141592653589793;

    @Test
    void minMaxTest() {
        var column = new Float32MutableColumnImpl(maxChunkSize, tombstone);
        float max = Float.MIN_VALUE;
        float min = Float.MAX_VALUE;
        int from = columnSize / 7;
        int to = 9 * columnSize / 11;

        for (int i = 0; i < columnSize; ++i) {
            if (random.nextBoolean()) {
                continue;
            }
            float value = random.nextFloat(Float.MAX_VALUE);
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

        assertEquals(min, Float32Aggregator.min(column, from, to));
        assertEquals(max, Float32Aggregator.max(column, from, to));
    }

    @Test
    void indexOfAnotherTest() {
        final int testedValues = 25;
        var column = new Float32MutableColumnImpl(maxChunkSize, tombstone);
        int from = columnSize / 7;
        int to = 9 * columnSize / 11;

        float defaultValue = random.nextFloat();

        while (defaultValue == tombstone) {
            defaultValue = random.nextFloat();
        }

        for (int i = 0; i < columnSize; ++i) {
            float value = random.nextFloat(Float.MAX_VALUE);
            column.write(i, defaultValue);
            if (random.nextBoolean()) {
                column.write(i, defaultValue);
            }
        }

        int expectedIndexOfAnother = to;
        int expectedLastIndexOfAnother = from;

        for (int i = 0; i < testedValues; i++) {
            int index = random.nextInt(from, to);
            float value = random.nextFloat();
            while (value == defaultValue) {
                value = random.nextFloat();
            }
            column.write(index, value);

            expectedIndexOfAnother = Math.min(expectedIndexOfAnother, index);
            expectedLastIndexOfAnother = Math.max(expectedLastIndexOfAnother, index);
        }

        assertEquals(expectedIndexOfAnother, Float32Aggregator.indexOfAnother(column, defaultValue, from, to));
        assertEquals(expectedLastIndexOfAnother, Float32Aggregator.lastIndexOfAnother(column, defaultValue, from, to));
    }

    @Test
    void mulTwoColumnsTest() {
        var column1 = new Float32MutableColumnImpl(maxChunkSize, tombstone);
        var column2 = new Float32MutableColumnImpl(maxChunkSize, tombstone);
        var expected = new float[columnSize];

        for (int i = 0; i < columnSize; i++) {
            float val1 = random.nextFloat();
            float val2 = random.nextFloat();
            column1.write(i, val1);
            column2.write(i, val2);
            expected[i] = val1 * val2;
        }

        var resColumn = Float32Aggregator.mul(column1, column2, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(resColumn.get(i), expected[i]);
        }
    }

    @Test
    void sumTwoColumnsTest() {
        var column1 = new Float32MutableColumnImpl(maxChunkSize, tombstone);
        var column2 = new Float32MutableColumnImpl(maxChunkSize, tombstone);
        var expected = new float[columnSize];

        for (int i = 0; i < columnSize; i++) {
            float val1 = random.nextFloat();
            float val2 = random.nextFloat();
            column1.write(i, val1);
            column2.write(i, val2);
            expected[i] = val1 + val2;
        }

        var resColumn = Float32Aggregator.sum(column1, column2, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(resColumn.get(i), expected[i]);
        }
    }

    @Test
    void mulManyColumnsTest() {
        var columns = new Float32MutableColumn[columnsCount];
        var expected = new float[columnSize];
        for (int i = 0; i < columnsCount; i++) {
            columns[i] = new Float32MutableColumnImpl(maxChunkSize, tombstone);
            for (int j = 0; j < columnSize; j++) {
                float val = random.nextFloat();
                columns[i].write(j, val);
                expected[j] = i == 0 ? val : expected[j] * val;
            }
        }
        Float32MutableColumn result = Float32Aggregator.mul(columns, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(expected[i], result.get(i));
        }
    }

    @Test
    void sumManyColumnsTest() {
        var columns = new Float32MutableColumn[columnsCount];
        var expected = new float[columnSize];
        for (int i = 0; i < columnsCount; i++) {
            columns[i] = new Float32MutableColumnImpl(maxChunkSize, tombstone);
            for (int j = 0; j < columnSize; j++) {
                float val = random.nextFloat();
                columns[i].write(j, val);
                expected[j] += val;
            }
        }
        Float32MutableColumn result = Float32Aggregator.sum(columns, columnSize, maxChunkSize);

        for (int i = 0; i < columnSize; i++) {
            assertEquals(expected[i], result.get(i));
        }
    }
}
