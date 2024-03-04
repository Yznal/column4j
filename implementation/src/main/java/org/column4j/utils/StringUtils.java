package org.column4j.utils;

import java.util.Objects;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public final class StringUtils {

    private StringUtils() {

    }

    /**
     * Find index of first element what not equals to passed value
     *
     * @param data  source String array
     * @param value value for filter
     * @param from  left bound (inclusive)
     * @param to    right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int indexOfAnother(String[] data, String value, int from, int to) {
        for (; from < to; from++) {
            if (!Objects.equals(value, data[from])) {
                return from;
            }
        }
        return -1;
    }

    /**
     * Find index of last element what not equals to passed value
     *
     * @param data  source String array
     * @param value value for filter
     * @param from  left bound (inclusive)
     * @param to    right bound (exclusive)
     * @return index if element found or -1 otherwise
     */
    public static int lastIndexOfAnother(String[] data, String value, int from, int to) {
        if (to == data.length) {
            to--;
        }
        for (; to >= from; to--) {
            if (!Objects.equals(value, data[to])) {
                return to;
            }
        }
        return -1;
    }
}
