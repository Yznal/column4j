package org.column4j.hidden_values;

import org.column4j.base.Column;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface IntColumn extends Column<int[]> {
    /**
     * Получить значение из позиции {@code pos}
     *
     * @param pos позиция в колонке
     */
    int get(int pos);

    /**
     * Метод для проверки наличия значение по указанной позиции {@code pos}
     *
     * @param pos позиция в колонке
     * @return {@code true} - значение по данной позиции существует, {@code false} - иначе
     */
    boolean isPresent(int pos);
}
