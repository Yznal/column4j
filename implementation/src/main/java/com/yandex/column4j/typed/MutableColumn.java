package com.yandex.column4j.typed;

import com.yandex.column4j.base.Column;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface MutableColumn<T> extends Column<T> {
    /**
     * Записать в колонку на позицию {@code pos} пустое значение
     *
     * @param pos позиция в колонке
     */
    void tombstone(int pos);
}
