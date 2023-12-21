package org.column4j.index.v2.skiplist;

import org.column4j.index.v2.IndexColumn;
import org.column4j.index.v2.blocks.Block;
import org.column4j.index.v2.blocks.BlockProducer;
import org.column4j.index.v2.blocks.MutableBlock;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Column of string-to-bitset values based on min-max skip index
 */
@ParametersAreNonnullByDefault
public abstract class SkipListIndexColumn<ValType> implements IndexColumn<ValType> {

    private static final int NO_VALUE = -1;

    private final MutableObjectIntMap<CharSequence> encoding = new ObjectIntHashMap<>();
    private final AtomicInteger codeSource = new AtomicInteger(0);

    protected List<Block<ValType>> finalizedBlocks = new CopyOnWriteArrayList<>();
    protected volatile MutableBlock<ValType> prepareFinalize;
    protected volatile MutableBlock<ValType> activeBlock;

    private final Object encodingMonitor = new Object();
    private final BlockProducer<ValType> blockProducer;

    public SkipListIndexColumn(BlockProducer<ValType> blockProducer) {
        this.blockProducer = blockProducer;
        this.prepareFinalize = blockProducer.createMutable();
        this.activeBlock = blockProducer.createMutable();
    }

    @Nullable
    @Override
    public ValType lookupIndex(CharSequence key) {
        int keyCode = getEncoding(key);
        if (keyCode == NO_VALUE) {
            return null;
        }
        // get "snapshot" links
        MutableBlock<ValType> activeSnap = activeBlock;
        MutableBlock<ValType> prepareSnap = prepareFinalize;
        List<Block<ValType>> immutableSnap = finalizedBlocks;
        ValType result = identityContainer();

        lookupMutable(keyCode, activeSnap, result);
        lookupMutable(keyCode, prepareSnap, result);
        for (var block : immutableSnap) {
            lookupFinalized(keyCode, block, result);
        }

        return result;
    }

    @Override
    public void store(CharSequence key, int value) {
        int encoding = addEncoding(key);
        activeBlock.addRecord(encoding, value);
        rotateIfNeeded();
    }

    @Override
    public int cardinality() {
        return codeSource.get();
    }

    protected void rotateIfNeeded() {
        if (activeBlock.finalizable()) {
            prepareFinalize = activeBlock;
            activeBlock = blockProducer.createMutable();

            Block<ValType> immutable = blockProducer.finalize(prepareFinalize);
            finalizedBlocks.add(immutable);
        }
    }

    private int addEncoding(CharSequence key) {
        synchronized (encodingMonitor) {
            return encoding.getIfAbsentPut(key, codeSource::getAndIncrement);
        }
    }

    private int getEncoding(CharSequence key) {
        synchronized(encodingMonitor) {
            return encoding.getIfAbsent(key, NO_VALUE);
        }
    }


    protected abstract ValType identityContainer();

    protected abstract void lookupMutable(int key, MutableBlock<ValType> block, ValType bs);

    protected abstract void lookupFinalized(int key, Block<ValType> block, ValType bs);


}
