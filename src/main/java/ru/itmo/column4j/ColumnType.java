package ru.itmo.column4j;

/**
 * Тип данных в колонке
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface ColumnType<T> {

    /**
     * @return Java тип колонки
     */
    Class<T> getType();

    /**
     * Преобразовать переданный объект к типу колонки.
     * Если преобразование невозможно, то необходимо сгенерировать {@link IllegalArgumentException}
     *
     * @param value значение колонки
     * @return преобразованное значение
     */
    T cast(Object value);
}
