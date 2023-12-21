package org.column4j.column.impl.mutable.primitive;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class Int8MutableColumnImplTest {
    private final Random random = new Random();

    public static Stream<Arguments> userCases() {
        return Stream.of(
                Arguments.of(8, 5, 1),
                Arguments.of(8, 9, 2),
                Arguments.of(8, 16, 2),
                Arguments.of(127, 16, 1),
                Arguments.of(127, 1270, 10)
        );
    }

    @ParameterizedTest
    @MethodSource("userCases")
    void testUserCase(int maxChunkSize, int amountOfWrite, int exceptedChunkCount) {
        var tombstone = (byte) random.nextInt(-8, 8);
        var column = new Int8MutableColumnImpl(maxChunkSize, tombstone);

        int expectedFilled = 0;
        for (int i = 0; i < amountOfWrite; i++) {
            var value = random.nextLong(-8, 8);
            column.write(i, (byte) value);
            if (value != tombstone) {
                expectedFilled++;
            }
        }

        var chunks = column.getChunks();
        assertEquals(exceptedChunkCount, chunks.size());

        var actualFilled = 0;
        for (var chunk : chunks) {
            var statistic = chunk.getStatistic();
            actualFilled += statistic.getCount();
        }

        assertEquals(expectedFilled, actualFilled);
    }

}