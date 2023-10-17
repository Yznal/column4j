package com.yandex.column4j.typed;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface StringMutableColumn extends MutableColumn<String[]> {
    /**
     * Записать в колонку на позицию {@code pos} значение {@code value}
     *
     * @param pos позиция в колонке
     * @param value значение для записи
     */
    void write(int pos, String value);
}
