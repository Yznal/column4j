package org.column4j.compression;

import jdk.incubator.vector.*;

public class LongCompressor {

    static VectorSpecies<Byte> bSpecies = ByteVector.SPECIES_PREFERRED;
    static VectorSpecies<Long> lSpecies = LongVector.SPECIES_PREFERRED;

    private static final long[] LONG_COMPRESS_MASKS = {
            0x1010101010101010L,    // 10000000100...
            0x3030303030303030L,    // 11000000110...
            0x7077070707070707L,    // 11100000111...
            0xF0F0F0F0F0F0F0F0L,    // 11110000111...
            0xF1F1F1F1F1F1F1F1L,    // 11111000111...
            0xF3F3F3F3F3F3F3F3L,    // 11111100111...
            0xF7F7F7F7F7F7F7F7L,    // 11111110111...
            0xFFFFFFFFFFFFFFFFL     // 11111111111... noop
    };

    private final int byteSize;
    private final VectorMask<Byte> compressionLongMask;
    private final VectorMask<Byte> outBytesLongMask;

    public LongCompressor(int byteSize) {
        if (byteSize < 1) {
            throw new IllegalArgumentException("Byte size should be positive");
        }
        this.byteSize = byteSize;
        this.compressionLongMask = VectorMask.fromLong(bSpecies, LONG_COMPRESS_MASKS[byteSize - 1]);
        this.outBytesLongMask = VectorMask.fromLong(bSpecies, (1L << byteSize * lSpecies.length()) - 1);
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

    public byte[] compressLongsV(long[] original) {
        validateLongParams();

        byte[] compressed = new byte[byteSize * original.length];
        int offset = 0;
        int bytesOffset = 0;
        int cmpByteSize = byteSize * lSpecies.length();
        for ( ;
              offset < lSpecies.loopBound(original.length);
              offset += lSpecies.length(), bytesOffset += cmpByteSize
        ) {
            ByteVector v = LongVector.fromArray(lSpecies, original, offset).reinterpretAsBytes();
            v.compress(compressionLongMask).intoArray(compressed, bytesOffset, outBytesLongMask);
        }
        for (; offset < original.length; offset++) { // remainder
            for (int b = 0; b < byteSize; b++) {
                compressed[offset * byteSize + b] = (byte) (original[offset] >> b * Byte.SIZE);
            }
        }
        return compressed;
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

    public long[] decompressLongsV(byte[] bytes) {
        validateLongParams();
        if (bytes.length % byteSize != 0) {
            throw new IllegalArgumentException("Bytes not aligned for %d bytes compression".formatted(byteSize));
        }


        long[] decompressed = new long[bytes.length / byteSize];
        int processedByteSize = byteSize * lSpecies.length();
        int byteBound = bytes.length - bSpecies.length();
        int byteOffset = 0;
        int offset = 0;
        for ( ;
              byteOffset < byteBound;
              byteOffset += processedByteSize, offset += lSpecies.length()
        ) {
            LongVector v = ByteVector.fromArray(bSpecies, bytes, byteOffset).expand(compressionLongMask).reinterpretAsLongs();
            v.intoArray(decompressed, offset);
        }
        for ( ; offset < decompressed.length; offset++) {
            decompressed[offset] = getLongAt(bytes, offset);
        }
        return decompressed;
    }

    public long getLongAt(byte[] arr, int idx) {
        long res = 0;
        for (int b = byteSize - 1; b >= 0; b--) {
            res |= Byte.toUnsignedLong(arr[idx * byteSize + b]) <<  (b * Byte.SIZE);
        }
        return res;
    }

    private void validateLongParams() {
        if (byteSize > Long.BYTES) {
            throw new IllegalStateException("Trying to (de)compress long to %d bytes".formatted(byteSize));
        }
    }
}
