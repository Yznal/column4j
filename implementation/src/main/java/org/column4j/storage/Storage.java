package org.column4j.storage;

import org.column4j.column.ColumnType;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.index.Dimension;

import java.util.Collection;
import java.util.List;

/**
 * Sample variant of assembled storage
 */
public interface Storage {

    /**
     * Find data columns corresponding to dimensions
     * @param query dimensions
     * @return list of found columns
     */
    List<MutableColumn<?, ?>> findColumns(Collection<Dimension> query);

    /**
     * Create new column entry for given dimensions
     * @param dimensions tuple of dimension values
     * @param name column name
     * @param type column type
     * @return created column, throws exception otherwise
     */
    MutableColumn<?, ?> createColumn(Collection<Dimension> dimensions, CharSequence name, ColumnType type);


}
