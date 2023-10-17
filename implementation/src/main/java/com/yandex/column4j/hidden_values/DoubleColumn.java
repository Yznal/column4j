package com.yandex.column4j.hidden_values;

import com.yandex.column4j.base.Column;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface DoubleColumn extends Column<double[]> {
    /**
     * Получить значение из позиции {@code pos}
     *
     * @param pos позиция в колонке
     */
    double get(int pos);

    /**
     * Метод для проверки наличия значение по указанной позиции {@code pos}
     *
     * @param pos позиция в колонке
     * @return {@code true} - значение по данной позиции существует, {@code false} - иначе
     */
    boolean isPresent(int pos);
}
