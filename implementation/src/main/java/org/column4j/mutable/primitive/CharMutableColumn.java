package org.column4j.mutable.primitive;

import org.column4j.mutable.MutableColumn;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface CharMutableColumn extends MutableColumn<char[]> {
    /**
     * Write {@code value} into column at {@code position}
     *
     * @param position position in column
     * @param value value to write
     */
    void write(int position, char value);
}
