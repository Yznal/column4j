package org.column4j.compression;

import jdk.incubator.vector.*;

public class IntCompressor {

    static VectorSpecies<Byte> bSpecies = ByteVector.SPECIES_PREFERRED;
    static VectorSpecies<Integer> iSpecies = IntVector.SPECIES_PREFERRED;

    private static final long[] INT_COMPRESS_MASKS = {
            0x1111111111111111L,    // 10001000100...
            0x3333333333333333L,    // 11001100110...
            0x7777777777777777L,    // 11101110111...
            0xFFFFFFFFFFFFFFFFL     // 11111111111... noop
    };

    private final int byteSize;
    private final VectorMask<Byte> compressionIntMask;
    private final VectorMask<Byte> outBytesIntMask;

    public IntCompressor(int byteSize) {
        if (byteSize < 1) {
            throw new IllegalArgumentException("Byte size should be positive");
        }
        this.byteSize = byteSize;
        this.compressionIntMask = VectorMask.fromLong(bSpecies, INT_COMPRESS_MASKS[byteSize - 1]);
        this.outBytesIntMask = VectorMask.fromLong(bSpecies, (1L << byteSize * iSpecies.length()) - 1);
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
        int cmpByteSize = byteSize * iSpecies.length();
        for ( ;
              offset < iSpecies.loopBound(original.length);
              offset += iSpecies.length(), bytesOffset += cmpByteSize
        ) {
            ByteVector v = IntVector.fromArray(iSpecies, original, offset).reinterpretAsBytes();
            v.compress(compressionIntMask).intoArray(compressed, bytesOffset, outBytesIntMask);
        }
        for (; offset < original.length; offset++) { // remainder
            for (int b = 0; b < byteSize; b++) {
                compressed[offset * byteSize + b] = (byte) (original[offset] >> b * Byte.SIZE);
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

    public int[] decompressIntsV(byte[] bytes) {
        validateIntParams();
        if (bytes.length % byteSize != 0) {
            throw new IllegalArgumentException("Bytes not aligned for %d bytes compression".formatted(byteSize));
        }


        int[] decompressed = new int[bytes.length / byteSize];
        int processedByteSize = byteSize * iSpecies.length();
        int byteBound = bytes.length - bSpecies.length();
        int byteOffset = 0;
        int offset = 0;
        for ( ;
              byteOffset < byteBound;
              byteOffset += processedByteSize, offset += iSpecies.length()
        ) {
            IntVector v = ByteVector.fromArray(bSpecies, bytes, byteOffset).expand(compressionIntMask).reinterpretAsInts();
            v.intoArray(decompressed, offset);
        }
        for ( ; offset < decompressed.length; offset++) {
            decompressed[offset] = getIntAt(bytes, offset);
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

    private void validateIntParams() {
        if (byteSize > Integer.BYTES) {
            throw new IllegalStateException("Trying to (de)compress int to %d bytes".formatted(byteSize));
        }
    }

}
