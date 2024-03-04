package org.column4j.index.v2.blocks.bitset;

import org.column4j.index.v2.blocks.Block;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.BitSet;

/**
 * Implementation of read-only block that uses {@link BitSet}
 */
@ThreadSafe
public class RoBitSetBlock implements Block<BitSet> {

    private final int[] codes;
    private final int[] offsets;
    private final LongBuffer bitsets; // bitset[i] = bitsets[offset[i]:offset[i+1]]

    public RoBitSetBlock(MutableIntObjectMap<BitSet> values) {
        codes = new int[values.size()];
        int idx = 0;
        offsets = new int[codes.length + 1];

        long totalSize = 0;
        for (var bs : values.keyValuesView()) {
            totalSize += bs.getTwo().size();
            codes[idx++] = bs.getOne();
        }
        totalSize /= 64;
        bitsets = LongBuffer.allocate((int) totalSize);

        Arrays.sort(codes);
        int offset = 0;
        for (int i = 0; i < codes.length; i++) {
            offsets[i] = offset;
            long[] bitSetLongs = values.get(codes[i]).toLongArray();
            bitsets.put(offset, bitSetLongs, 0, bitSetLongs.length);
            offset += bitSetLongs.length;
        }
        offsets[codes.length] = offset;
    }

    @Nullable
    @Override
    public BitSet lookup(int idx) {
        int offsetPos = Arrays.binarySearch(codes, idx);
        if (offsetPos < 0) {
            return null;
        }
        int offsetIndex = offsets[offsetPos];
        int endIndex = offsets[offsetPos + 1];
        return BitSet.valueOf(bitsets.slice(offsetIndex, endIndex - offsetIndex));
    }

    @Override
    public int size() {
        return codes.length;
    }

    @Override
    public int minKey() {
        return codes[0];
    }

    @Override
    public int maxKey() {
        return codes[codes.length - 1];
    }
}
