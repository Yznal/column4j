package org.column4j.mutable;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface StringMutableColumn extends MutableColumn<String[]> {
    /**
     * Write {@code value} into column at {@code position}
     *
     * @param position position in column
     * @param value value to write
     */
    void write(int position, String value);

    /**
     * Get column value what must be interpreter as empty value.
     * @return tombstoneHolder value
     */
    String getTombstone();
}
