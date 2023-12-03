package org.column4j.mutable.primitive;

import org.column4j.mutable.MutableColumn;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface FloatMutableColumn extends MutableColumn<float[]> {
    /**
     * Write {@code value} into column at {@code position}
     *
     * @param position position in column
     * @param value value to write
     */
    void write(int position, float value);

    /**
     * Get column value what must be interpreter as empty value.
     * @return tombstoneHolder value
     */
    float getTombstone();
}
