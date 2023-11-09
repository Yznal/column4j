package org.column4j.compression;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IntCompressorTest {

    @Test
    void testOneByteCompressor() {
        int[] original = { 80, 25, 140, 185, 73, 21, 109, 106, 7, 57, 229, 162, 161, 66, 192, 181, 238, 167, 93, 187 };
        testIntCompressor(1, original);
    }

    @Test
    void testTwoByteCompressor() {
        int[] original = { 54328, 28998, 50219, 917, 60047, 34015, 31368, 28807, 55167, 18653, 51605, 1100, 197, 39377,
                7127, 43982, 11490, 9038, 38608, 60495 };
        testIntCompressor(2, original);
    }

    @Test
    void testThreeByteCompressor() {
        int[] original = { 9460365, 7121158, 13231579, 10270892, 9889100, 9369668, 7649441, 16388751, 8150379, 611354,
                10938047, 5961797, 5103938, 7861492, 1002005, 13436969, 8982004, 16013445, 8051441, 8808590 };
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
        // check vector2 correctness
        byte[] simdCompressed2 = compressor.compressIntsV2(original);
        assertArrayEquals(compressed, simdCompressed2);
    }

}