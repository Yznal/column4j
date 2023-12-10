package org.column4j.index.temporal.column;


import javax.annotation.Nullable;

/**
 * Stores pointers from dimension to column ids containing it
 */
public interface DimensionColumn {


    /**
     * Adds record about specific chunk into index
     * @param dimValue column dimension value
     * @param colIndex column id
     */
    void storeColRecord(CharSequence dimValue, int colIndex);

    /**
     * Searches for columns ids that contain given dimension value
     * @param dimValue dimension
     * @return search result or null
     */
    @Nullable
    int[] lookup(CharSequence dimValue);



}
