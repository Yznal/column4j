package org.column4j.aggregate;

import org.column4j.column.mutable.primitive.Int64MutableColumn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.column4j.column.impl.mutable.primitive.Int64MutableColumnImpl;
import org.column4j.aggregate.Int64Aggregator;

class Int64AggregatorTest {
    private final Random random = new Random();
    private final int columnSize = 1000;
    private final int maxChunkSize = 23;
    private final int tombstone = 34;

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
            long value = random.nextLong(Long.MAX_VALUE);
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
            long value = random.nextLong(Long.MAX_VALUE);
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
    void sumTest() {
    }
}
