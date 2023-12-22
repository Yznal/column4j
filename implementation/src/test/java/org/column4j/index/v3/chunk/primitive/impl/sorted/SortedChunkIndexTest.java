package org.column4j.index.v3.chunk.primitive.impl.sorted;

import org.column4j.index.v3.chunk.primitive.Int32ChunkIndex;
import org.column4j.index.v3.chunk.primitive.StringChunkIndex;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.UUID;
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

    @Test
    void testStringIndex() {
        int chunkIndexSegmentSize = 128;
        String[] testChunkData = IntStream.rangeClosed(0, 1000)
                .mapToObj(_ -> UUID.randomUUID().toString())
                .toArray(String[]::new);

        String testString = "test";
        String testRepeatedString = "repeated";
        String notPresentString = "i am not";
        int randomIndex = rnd.nextInt(950);
        testChunkData[randomIndex] = testString;
        for (int i = randomIndex + 120; i < randomIndex + 124; i++) {
            testChunkData[i % 1001] = testRepeatedString;
        }
        StringChunkIndex index = SortedStringChunkIndex.fromChunk(testChunkData, chunkIndexSegmentSize);

        assertEquals(1001, testChunkData.length);
        assertTrue(index.contains(testString));
        assertTrue(index.contains(testRepeatedString));
        assertFalse(index.contains(notPresentString));
        for (String s : testChunkData) {
            assertTrue(index.contains(s));
        }

        int[] singleRes = index.lookupValues(testString);
        assertNotNull(singleRes);
        assertEquals(1, singleRes.length);
        assertEquals(testString, testChunkData[singleRes[0]]);

        int[] repeatedRes = index.lookupValues(testRepeatedString);
        assertNotNull(repeatedRes);
        assertEquals(4, repeatedRes.length);
        for (int idx : repeatedRes) {
            assertEquals(testRepeatedString, testChunkData[idx]);
        }
        for (String s : testChunkData) {
            int[] found = index.lookupValues(s);
            assertNotNull(found);
            for (int idx : found) {
                assertEquals(s, testChunkData[idx]);
            }
        }
    }




    private int nextInt(int in) {
        return in < 1024 ? rnd.nextInt(50000) : 52000 + rnd.nextInt(50000);
    }
}

