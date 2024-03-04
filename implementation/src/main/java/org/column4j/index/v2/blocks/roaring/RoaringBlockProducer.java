package org.column4j.index.v2.blocks.roaring;

import org.column4j.index.v2.blocks.Block;
import org.column4j.index.v2.blocks.BlockProducer;
import org.column4j.index.v2.blocks.MutableBlock;
import org.roaringbitmap.RoaringBitmap;

public class RoaringBlockProducer implements BlockProducer<RoaringBitmap> {
    @Override
    public MutableBlock<RoaringBitmap> createMutable() {
        return new RwRoaringBlock();
    }

    @Override
    public Block<RoaringBitmap> finalize(MutableBlock<RoaringBitmap> source) {
        return new RoRoaringBlock(source.getData());
    }
}
