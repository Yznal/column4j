package org.column4j.mutable;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface GenericMutableColumn<T> extends MutableColumn<T[]> {
    /**
     * Write {@code value} into column at {@code position}
     *
     * @param position position in column
     * @param value value to write
     */
    void write(int position, T value);
}
