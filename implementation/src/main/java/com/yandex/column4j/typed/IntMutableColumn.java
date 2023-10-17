package com.yandex.column4j.typed;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface IntMutableColumn extends MutableColumn<int[]> {
    /**
     * Записать в колонку на позицию {@code pos} значение {@code value}
     *
     * @param pos позиция в колонке
     * @param value значение для записи
     */
    void write(int pos, int value);
}
