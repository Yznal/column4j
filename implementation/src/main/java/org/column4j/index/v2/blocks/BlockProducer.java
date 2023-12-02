package org.column4j.index.v2.blocks;

public interface BlockProducer<BlockVal> {

    MutableBlock<BlockVal> createMutable();

    Block<BlockVal> finalize(MutableBlock<BlockVal> source);
}
