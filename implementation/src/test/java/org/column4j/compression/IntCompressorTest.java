package org.column4j.compression;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IntCompressorTest {

    @Test
    void testOneByteCompressor() {
        int[] original = {34, 75, 39, 44, 126, 230, 244};
        testIntCompressor(1, original);
    }

    @Test
    void testTwoByteCompressor() {
        int[] original = { 47380, 50552, 33571, 49706, 0, 38433, 29104, 263, 36452, 34597 };
        testIntCompressor(2, original);
    }

    @Test
    void testThreeByteCompressor() {
        int[] original = { 11143456, 15333231, 4353426, 4120979, 6678605, 3187558, 15878654, 9480521, 11689069, 5070716 };
        testIntCompressor(3, original);
    }

    private void testIntCompressor(int b, int[] original) {
        var compressor = new IntCompressor(b);
        byte[] compressed = compressor.compressInts(original);
        int[] decompressed = compressor.decompressInts(compressed);
        assertArrayEquals(original, decompressed);
        // check vector correctness
        byte[] simdCompressed = compressor.compressIntsV(original);
        assertArrayEquals(compressed, simdCompressed);
    }

}