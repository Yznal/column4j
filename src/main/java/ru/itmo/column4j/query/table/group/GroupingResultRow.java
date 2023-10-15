package ru.itmo.column4j.query.table.group;

import java.util.Optional;

/**
 * Строка результата группировки
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface GroupingResultRow {

    /**
     * Получить значение строки из строки группы результата группировки
     *
     * @param name название колонки
     * @return данные колонки или {@link Optional#empty()}, если колонка не присутствует в результатах выборки
     * @param <T> тип данных
     */
    <T> Optional<T> get(String name);
}
