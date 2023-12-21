package org.column4j.index.v3.chunk.primitive.impl.sorted;

import org.column4j.index.v3.chunk.primitive.Int32ChunkIndex;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class SortedChunkIndexTest {


    private static final Random rnd = new Random(10);

    @Test
    void testSelection() {
        int testSegmentSize = 128;
        int testValueRepeated = 51324;
        int nonexistent = 51643;

        int testValue = 51261;
        int[] testArr = IntStream.concat(
                IntStream.of( testValue, testValueRepeated, testValueRepeated, testValueRepeated, testValueRepeated),
                IntStream.range(0, 2048).map(this::nextInt)
        ).toArray();

        assertEquals(2053, testArr.length);

        Int32ChunkIndex index = SortedInt32ChunkIndex.fromChunk(testArr, testSegmentSize);

        assertFalse(index.contains(nonexistent));
        assertNull(index.lookupValues(nonexistent));

        assertTrue(index.contains(testValue));
        int[] res = index.lookupValues(testValue);
        assertEquals(1, res.length);
        int foundValue = testArr[res[0]];
        assertEquals(testValue, foundValue);

        assertTrue(index.contains(testValueRepeated));
        int[] repRes = index.lookupValues(testValueRepeated);
        assertEquals(4, repRes.length);
        for (int i : repRes) {
            assertEquals(testValueRepeated, testArr[i]);
        }
    }




    private int nextInt(int in) {
        return in < 1024 ? rnd.nextInt(50000) : 52000 + rnd.nextInt(50000);
    }
}

