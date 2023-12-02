package org.column4j.index.v2.skiplist;

import org.column4j.index.v2.blocks.Block;
import org.column4j.index.v2.blocks.BlockProducer;
import org.column4j.index.v2.blocks.MutableBlock;
import org.column4j.index.v2.blocks.bitset.BitSetBlockProducer;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.BitSet;

@ParametersAreNonnullByDefault
public class BitSetSkipIndex extends SkipListIndexColumn<BitSet> {

    public BitSetSkipIndex() {
        super(new BitSetBlockProducer());
    }

    @Override
    protected BitSet identityContainer() {
        return new BitSet();
    }

    @Override
    protected void lookupMutable(int key, MutableBlock<BitSet> block, BitSet bs) {
        BitSet res = block.lookup(key);
        if (res != null) {
            bs.or(res);
        }
    }

    @Override
    protected void lookupFinalized(int key, Block<BitSet> block, BitSet bs) {
        if (key >= block.minKey() && key <= block.maxKey()) {
            BitSet res = block.lookup(key);
            if (res != null) {
                bs.or(res);
            }
        }
    }
}
