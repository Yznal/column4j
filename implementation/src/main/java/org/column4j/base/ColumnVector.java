package org.column4j.base;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface ColumnVector<T> extends Column<T> {

    /**
     * @return получить данные колонки
     */
    T data();

}
