package org.column4j.index.v2.skiplist;

import org.column4j.index.v2.blocks.Block;
import org.column4j.index.v2.blocks.MutableBlock;
import org.column4j.index.v2.blocks.roaring.RoaringBlockProducer;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public class RoaringSkipList extends SkipListIndexColumn<RoaringBitmap> {
    public RoaringSkipList() {
        super(new RoaringBlockProducer());
    }

    @Override
    protected RoaringBitmap identityContainer() {
        return new RoaringBitmap();
    }

    @Override
    protected void lookupMutable(int key, MutableBlock<RoaringBitmap> block, RoaringBitmap bs) {
        RoaringBitmap res = block.lookup(key);
        if (res != null) {
            bs.or(res);
        }
    }

    @Override
    protected void lookupFinalized(int key, Block<RoaringBitmap> block, RoaringBitmap bs) {
        if (key >= block.minKey() && key <= block.maxKey()) {
            RoaringBitmap res = block.lookup(key);
            if (res != null) {
                bs.or(res);
            }
        }
    }
}
