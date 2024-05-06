package org.column4j.aggregate;

import org.column4j.column.mutable.primitive.Int8MutableColumn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.column4j.column.impl.mutable.primitive.Int8MutableColumnImpl;
import org.column4j.aggregate.Int8Aggregator;

class Int8AggregatorTest {
    private final Random random = new Random();
    private final int columnSize = 1000;
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
        final int testedValues = 25;
        var column = new Int8MutableColumnImpl(maxChunkSize, tombstone);
        int from = columnSize / 7;
        int to = 9 * columnSize / 11;

        byte defaultValue = (byte)random.nextInt();

        while (defaultValue == tombstone) {
            defaultValue = (byte)random.nextInt();
        }

        for (int i = 0; i < columnSize; ++i) {
            byte value = (byte)random.nextInt();
            column.write(i, defaultValue);
            if (random.nextBoolean()) {
                column.write(i, defaultValue);
            }
        }

        int expectedIndexOfAnother = to;
        int expectedLastIndexOfAnother = from;

        for (int i = 0; i < testedValues; i++) {
            int index = random.nextInt(from, to);
            byte value = (byte)random.nextInt();
            while (value == defaultValue) {
                value = (byte)random.nextInt();
            }
            column.write(index, value);

            expectedIndexOfAnother = Math.min(expectedIndexOfAnother, index);
            expectedLastIndexOfAnother = Math.max(expectedLastIndexOfAnother, index);
        }

        assertEquals(expectedIndexOfAnother, Int8Aggregator.indexOfAnother(column, defaultValue, from, to));
        assertEquals(expectedLastIndexOfAnother, Int8Aggregator.lastIndexOfAnother(column, defaultValue, from, to));
    }

    @Test
    void sumTest() {
    }
}
