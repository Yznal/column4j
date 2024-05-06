package org.column4j.aggregate;

import org.column4j.column.mutable.primitive.Int16MutableColumn;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.column4j.column.impl.mutable.primitive.Int16MutableColumnImpl;
import org.column4j.aggregate.Int16Aggregator;

class Int16AggregatorTest {
    private final Random random = new Random();
    private final int columnSize = 1000;
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
        final int testedValues = 25;
        var column = new Int16MutableColumnImpl(maxChunkSize, tombstone);
        int from = columnSize / 7;
        int to = 9 * columnSize / 11;

        short defaultValue = (short)random.nextInt();

        while (defaultValue == tombstone) {
            defaultValue = (short)random.nextInt();
        }

        for (int i = 0; i < columnSize; ++i) {
            short value = (short)random.nextInt();
            column.write(i, defaultValue);
            if (random.nextBoolean()) {
                column.write(i, defaultValue);
            }
        }

        int expectedIndexOfAnother = to;
        int expectedLastIndexOfAnother = from;

        for (int i = 0; i < testedValues; i++) {
            int index = random.nextInt(from, to);
            short value = (short)random.nextInt();
            while (value == defaultValue) {
                value = (short)random.nextInt();
            }
            column.write(index, value);

            expectedIndexOfAnother = Math.min(expectedIndexOfAnother, index);
            expectedLastIndexOfAnother = Math.max(expectedLastIndexOfAnother, index);
        }

        assertEquals(expectedIndexOfAnother, Int16Aggregator.indexOfAnother(column, defaultValue, from, to));
        assertEquals(expectedLastIndexOfAnother, Int16Aggregator.lastIndexOfAnother(column, defaultValue, from, to));
    }

    @Test
    void sumTest() {
    }
}
