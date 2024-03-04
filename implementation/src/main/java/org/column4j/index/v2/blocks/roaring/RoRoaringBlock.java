package org.column4j.index.v2.blocks.roaring;

import org.column4j.index.v2.blocks.Block;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

@ParametersAreNonnullByDefault
public class RoRoaringBlock implements Block<RoaringBitmap> {

    int[] codes;

    int[] offsets;
    ByteBuffer bitsets; // bitset[i] = bitsets[offset[i]:offset[i+1]]

    public RoRoaringBlock(MutableIntObjectMap<RoaringBitmap> values) {
        codes = new int[values.size()];
        int idx = 0;
        offsets = new int[codes.length + 1];

        long totalSize = 0;
        for (var rbm : values.keyValuesView()) {

            totalSize += rbm.getTwo().serializedSizeInBytes();
            codes[idx++] = rbm.getOne();
        }
        bitsets = ByteBuffer.allocate((int) totalSize);

        Arrays.sort(codes);
        int offset = 0;
        for (int i = 0; i < codes.length; i++) {
            offsets[i] = offset;
            values.get(codes[i]).serialize(bitsets);

            offset = bitsets.position();
        }
        offsets[codes.length] = offset;
    }

    @Nullable
    @Override
    public RoaringBitmap lookup(int idx) {
        int offsetPos = Arrays.binarySearch(codes, idx);
        if (offsetPos < 0) {
            return null;
        }
        int offsetIndex = offsets[offsetPos];
        int endIndex = offsets[offsetPos + 1];
        var rbm = new RoaringBitmap();
        try {
            rbm.deserialize(bitsets.slice(offsetIndex, endIndex - offsetIndex));
        } catch (IOException ioEx) {
            throw new RuntimeException(ioEx);
        }
        return rbm;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public int minKey() {
        return 0;
    }

    @Override
    public int maxKey() {
        return 0;
    }
}
