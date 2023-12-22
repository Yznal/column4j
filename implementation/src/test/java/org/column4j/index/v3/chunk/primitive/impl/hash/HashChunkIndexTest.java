package org.column4j.index.v3.chunk.primitive.impl.hash;

import org.column4j.index.v3.chunk.primitive.StringChunkIndex;
import org.column4j.index.v3.chunk.primitive.impl.sorted.SortedStringChunkIndex;
import org.column4j.index.v3.chunk.primitive.mutable.MutableInt32ChunkIndex;
import org.column4j.index.v3.chunk.primitive.mutable.MutableStringChunkIndex;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class HashChunkIndexTest {

    private static final Random rnd = new Random(10);

    @Test
    void testSelection() {
        int testValueRepeated = 51324;
        int nonexistent = 51643;


        int testValue = 51261;
        int[] testArr = IntStream.concat(
                IntStream.of( testValue, testValueRepeated, testValueRepeated, testValueRepeated, testValueRepeated),
                IntStream.range(0, 2048).map(this::nextInt)
        ).toArray();

        assertEquals(2053, testArr.length);

        MutableInt32ChunkIndex index = new HashInt32ChunkIndex();

        for (int i = 0; i < testArr.length; i++) {
            index.insertRecord(i, testArr[i]);
        }

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
        MutableStringChunkIndex index = new HashStringChunkIndex();
        for (int i = 0; i < testChunkData.length; i++) {
            index.insertRecord(i, testChunkData[i]);
        }

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

    @Test
    void testUpdate() {

        int testValueRepeated = 51324;
        int nonexistent = 201342;

        int testValue = 51261;
        int[] testArr = IntStream.concat(
                IntStream.of( testValue, testValueRepeated, testValueRepeated, testValueRepeated, testValueRepeated),
                IntStream.range(0, 2048).map(this::nextInt)
        ).toArray();

        assertEquals(2053, testArr.length);

        MutableInt32ChunkIndex index = new HashInt32ChunkIndex();

        for (int i = 0; i < testArr.length; i++) {
            index.insertRecord(i, testArr[i]);
        }

        assertFalse(index.contains(nonexistent));

        testArr[2050] = nonexistent;
        index.insertRecord(2050, nonexistent);

        assertTrue(index.contains(nonexistent));
        int[] res = index.lookupValues(nonexistent);
        assertEquals(1, res.length);
        assertEquals(nonexistent, testArr[res[0]]);
    }



    private int nextInt(int in) {
        return in < 1024 ? rnd.nextInt(50000) : 52000 + rnd.nextInt(50000);
    }

}