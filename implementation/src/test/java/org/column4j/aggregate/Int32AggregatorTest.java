package org.column4j.aggregate;

import org.column4j.column.mutable.primitive.Int32MutableColumn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.column4j.column.impl.mutable.primitive.Int32MutableColumnImpl;
import org.column4j.aggregate.Int32Aggregator;

class Int32AggregatorTest {
    private final Random random = new Random();
    private final int columnSize = 1000;
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
        final int testedValues = 25;
        var column = new Int32MutableColumnImpl(maxChunkSize, tombstone);
        int from = columnSize / 7;
        int to = 9 * columnSize / 11;

        int defaultValue = random.nextInt();

        while (defaultValue == tombstone) {
            defaultValue = random.nextInt();
        }

        for (int i = 0; i < columnSize; ++i) {
            int value = random.nextInt();
            column.write(i, defaultValue);
            if (random.nextBoolean()) {
                column.write(i, defaultValue);
            }
        }

        int expectedIndexOfAnother = to;
        int expectedLastIndexOfAnother = from;

        for (int i = 0; i < testedValues; i++) {
            int index = random.nextInt(from, to);
            int value = random.nextInt();
            while (value == defaultValue) {
                value = random.nextInt();
            }
            column.write(index, value);

            expectedIndexOfAnother = Math.min(expectedIndexOfAnother, index);
            expectedLastIndexOfAnother = Math.max(expectedLastIndexOfAnother, index);
        }

        assertEquals(expectedIndexOfAnother, Int32Aggregator.indexOfAnother(column, defaultValue, from, to));
        assertEquals(expectedLastIndexOfAnother, Int32Aggregator.lastIndexOfAnother(column, defaultValue, from, to));
    }

    @Test
    void sumTest() {
    }
}
