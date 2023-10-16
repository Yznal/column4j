package com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface Aggregator<T extends AggregatedValue<?>> {

    /**
     * Инициализация агрегирующей функции
     *
     * @return начальное значение агрегатора
     */
    T initialize();

    /**
     * Функция агрегации значения строки
     *
     * @param aggregatedValue агрегированное значение
     * @param value значение для агрегации
     * @return результирующее агрегатное состояние
     */
    T aggregate(T aggregatedValue, Object value);

    /**
     * Объединить два результата агрегации данных
     *
     * @param left данные для объединения
     * @param right данные для объединения
     * @return результат объединения
     */
    T merge(T left, T right);
}
