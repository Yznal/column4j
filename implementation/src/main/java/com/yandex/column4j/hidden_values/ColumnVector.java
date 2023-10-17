package com.yandex.column4j.hidden_values;

import com.yandex.column4j.base.Column;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface ColumnVector<T> extends Column<T> {

    /**
     * Сложить текущий вектор и полученный и вернуть результат в виде нового вектора
     * @param vector вектор для прибавления к текущему
     * @return результат сложения
     */
    ColumnVector<T> add(ColumnVector<T> vector);

}
