package org.column4j.index.v2.blocks.bitset;

import org.column4j.index.v2.blocks.Block;
import org.column4j.index.v2.blocks.BlockProducer;
import org.column4j.index.v2.blocks.MutableBlock;

import java.util.BitSet;

public class BitSetBlockProducer implements BlockProducer<BitSet> {



    @Override
    public MutableBlock<BitSet> createMutable() {
        return new RwBitSetBlock();
    }

    @Override
    public Block<BitSet> finalize(MutableBlock<BitSet> source) {
        return new RoBitSetBlock(source.getData());
    }
}
