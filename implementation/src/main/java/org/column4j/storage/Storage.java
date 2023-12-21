package org.column4j.storage;

import org.column4j.column.ColumnType;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.index.Dimension;

import java.util.Collection;
import java.util.List;

public interface Storage {


    List<MutableColumn<?, ?>> findColumns(Collection<Dimension> query);


    MutableColumn<?, ?> createColumn(Collection<Dimension> dimensions, CharSequence name, ColumnType type);


}
