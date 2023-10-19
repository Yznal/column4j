package org.column4j.mutable;

import org.column4j.Column;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface MutableColumn<T> extends Column<T> {
    /**
     * Write tombstone into column at {@code position}
     *
     * @param position position in column
     */
    void tombstone(int position);
}
