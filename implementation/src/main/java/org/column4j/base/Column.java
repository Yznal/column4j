package org.column4j.base;

/**
 * Интерфейс для получения данных по колонке
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface Column<T> {
    /**
     * Метод возвращает индекс первой не пустой строки в колонке,
     * либо {@code -1} если колонка не содержит данных.
     *
     * @return индекс первой строки в колонке, либо {@code -1}
     */
    int firstRowIndex();

    /**
     * @return количество уникальных значений в колонке
     */
    int capacity();

    /**
     *
     * @return количество записей в колонке
     */
    int size();

    /**
     * @return тип данных в колонке
     */
    ColumnType type();

}
