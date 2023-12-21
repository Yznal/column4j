package org.column4j.index.v2.blocks;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;

public interface MutableBlock<ValType> extends Block<ValType> {

    void addRecord(int key, int value);

    boolean finalizable();

    MutableIntObjectMap<ValType> getData();

}
