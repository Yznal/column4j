package org.column4j.compression;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

public class IntCompressor {

    private static final long[] INT_COMPRESS_MASKS = {
            0x1111111111111111L,    // 10001000100...
            0x3333333333333333L,    // 11001100110...
            0x7777777777777777L,    // 11101110111...
            0xFFFFFFFFFFFFFFFFL     // 11111111111... noop
    };
    private static final int[][] INTS_OUT_MAPS = {
            new int[] {0, -1, -1, -1, 1, -1, -1, -1, 2, -1 , -1, -1, 3, -1, -1, -1},
            new int[] {0, 1, -1, -1, 2, 3, -1, -1, 4, 5, -1, -1, 6, 7, -1, -1},
            new int[] {0, 1, 2, -1, 3, 4, 5, -1, 6, 7, 8, -1, 9, 10, 11, -1},
            new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
    };

    private static final long[] LONG_COMPRESS_MASKS = {
            0x1010101010101010L,    // 10000000100...
            0x3030303030303030L,    // 11000000110...
            0x7077070707070707L,    // 11100000111...
            0xF0F0F0F0F0F0F0F0L     // 11110000111...
    };

    private final int byteSize;
    private final VectorMask<Byte> cmpIntMask;
    private final VectorMask<Byte> outIntMask;

    VectorSpecies<Byte> bSpecies = ByteVector.SPECIES_PREFERRED;
    VectorSpecies<Integer> iSpecies = IntVector.SPECIES_PREFERRED;

    public IntCompressor(int byteSize) {
        if (byteSize < 1) {
            throw new IllegalArgumentException("Byte size should be positive");
        }
        this.byteSize = byteSize;
        this.cmpIntMask = VectorMask.fromLong(bSpecies, INT_COMPRESS_MASKS[byteSize - 1]);
        this.outIntMask = VectorMask.fromLong(bSpecies, (1L << byteSize * iSpecies.length()) - 1);
    }

    public byte[] compressInts(int[] original) {
        validateIntParams();

        byte[] compressed = new byte[byteSize * original.length];
        for (int i = 0; i < original.length; i++) {
            for (int b = 0; b < byteSize; b++) {
                compressed[i * byteSize + b] = (byte) (original[i] >> b * Byte.SIZE);
            }
        }
        return compressed;
    }

    public byte[] compressIntsV(int[] original) {
        validateIntParams();

        byte[] compressed = new byte[byteSize * original.length];
        int offset = 0;
        int bytesOffset = 0;
        for ( ;
              offset < iSpecies.loopBound(original.length);
              offset += iSpecies.length(), bytesOffset += byteSize * iSpecies.length()
        ) {
            ByteVector v = IntVector.fromArray(iSpecies, original, offset).reinterpretAsBytes();
            v.compress(cmpIntMask).intoArray(compressed, bytesOffset, outIntMask);
        }
        for (; offset < original.length; offset++) { // remainder
            for (int b = 0; b < byteSize; b++) {
                compressed[offset * byteSize + b] = (byte) (original[offset] >> b * Byte.SIZE);
            }
        }
        return compressed;
    }

    public byte[] compressIntsV2(int[] original) {
        validateIntParams();

        byte[] compressed = new byte[byteSize * original.length];
        int offset = 0;
        int bytesOffset = 0;
        for ( ;
              offset < iSpecies.loopBound(original.length);
              offset += iSpecies.length(), bytesOffset += byteSize * iSpecies.length()
        ) {
            ByteVector v = IntVector.fromArray(iSpecies, original, offset).reinterpretAsBytes();
            v.intoArray(compressed, bytesOffset, INTS_OUT_MAPS[byteSize-1], 0,  cmpIntMask);
        }
        for (; offset < original.length; offset++) { // remainder
            for (int b = 0; b < byteSize; b++) {
                compressed[offset * byteSize + b] = (byte) (original[offset] >> b * Byte.SIZE);
            }
        }
        return compressed;
    }

    public byte[] compressLongs(long[] original) {
        validateLongParams();

        byte[] compressed = new byte[byteSize * original.length];
        for (int i = 0; i < original.length; i++) {
            for (int b = 0; b < byteSize; b++) {
                compressed[i * byteSize + b] = (byte) (original[i] >> b * Byte.SIZE);
            }
        }
        return compressed;
    }

    public int[] decompressInts(byte[] bytes) {
        validateIntParams();
        if (bytes.length % byteSize != 0) {
            throw new IllegalArgumentException("Bytes not aligned for %d bytes compression".formatted(byteSize));
        }

        int[] decompressed = new int[bytes.length / byteSize];
        for (int i = 0; i < decompressed.length; i++) {
            decompressed[i] = getIntAt(bytes, i);
        }
        return decompressed;
    }

    public long[] decompressLongs(byte[] bytes) {
        validateLongParams();
        if (bytes.length % byteSize != 0) {
            throw new IllegalArgumentException("Bytes not aligned for %d bytes compression".formatted(byteSize));
        }

        long[] decompressed = new long[bytes.length / byteSize];
        for (int i = 0; i < decompressed.length; i++) {
            decompressed[i] = getLongAt(bytes, i);
        }
        return decompressed;
    }

    public int getIntAt(byte[] arr, int idx) {
        int res = 0;
        for (int b = byteSize - 1; b >= 0; b--) {
            res |= Byte.toUnsignedInt(arr[idx * byteSize + b]) << (b * Byte.SIZE);
        }
        return res;
    }

    public long getLongAt(byte[] arr, int idx) {
        long res = 0;
        for (int b = byteSize - 1; b >= 0; b--) {
            res |= Byte.toUnsignedLong(arr[idx * byteSize + b]) <<  (b * Byte.SIZE);
        }
        return res;
    }

    private void validateIntParams() {
        if (byteSize > Integer.BYTES) {
            throw new IllegalStateException("Trying to (de)compress int to %d bytes".formatted(byteSize));
        }
    }

    private void validateLongParams() {
        if (byteSize > Long.BYTES) {
            throw new IllegalStateException("Trying to (de)compress long to %d bytes".formatted(byteSize));
        }
    }

}
