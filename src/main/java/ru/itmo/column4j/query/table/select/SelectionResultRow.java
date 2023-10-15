package ru.itmo.column4j.query.table.select;

import java.util.Optional;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface SelectionResultRow {

    /**
     * Получить значение строки из результатов выбора
     *
     * @param name название колонки
     * @return данные колонки или {@link Optional#empty()}, если колонка не присутствует в результатах выборки
     * @param <T> тип данных
     */
    <T> Optional<T> get(String name);

}
