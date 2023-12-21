package org.column4j.index.v3.chunk.primitive.impl.hash;

import org.column4j.index.v3.chunk.primitive.Int32ChunkIndex;
import org.column4j.index.v3.chunk.primitive.mutable.MutableInt32ChunkIndex;
import org.junit.jupiter.api.Test;

import java.util.Random;
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