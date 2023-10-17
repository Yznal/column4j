package com.yandex.column4j.base;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface MutableColumn<T> extends Column<T> {
    /**
     * Записать в колонку на позицию {@code pos} значение {@code value}
     *
     * @param pos позиция в колонке
     * @param value значение для записи
     */
    void write(int pos, double value);

    /**
     * Записать в колонку на позицию {@code pos} значение {@code value}
     *
     * @param pos позиция в колонке
     * @param value значение для записи
     */
    void write(int pos, float value);

    /**
     * Записать в колонку на позицию {@code pos} значение {@code value}
     *
     * @param pos позиция в колонке
     * @param value значение для записи
     */
    void write(int pos, long value);

    /**
     * Записать в колонку на позицию {@code pos} значение {@code value}
     *
     * @param pos позиция в колонке
     * @param value значение для записи
     */
    void write(int pos, int value);

    /**
     * Записать в колонку на позицию {@code pos} значение {@code value}
     *
     * @param pos позиция в колонке
     * @param value значение для записи
     */
    void write(int pos, short value);

    /**
     * Записать в колонку на позицию {@code pos} значение {@code value}
     *
     * @param pos позиция в колонке
     * @param value значение для записи
     */
    void write(int pos, byte value);

    /**
     * Записать в колонку на позицию {@code pos} значение {@code value}
     *
     * @param pos позиция в колонке
     * @param value значение для записи
     */
    void write(int pos, boolean value);

    /**
     * Записать в колонку на позицию {@code pos} значение {@code value}
     *
     * @param pos позиция в колонке
     * @param value значение для записи
     */
    void write(int pos, CharSequence value);

    /**
     * Записать в колонку на позицию {@code pos} пустое значение
     *
     * @param pos позиция в колонке
     */
    void tombstone(int pos);


}
