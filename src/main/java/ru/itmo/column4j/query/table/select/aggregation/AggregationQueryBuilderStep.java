package ru.itmo.column4j.query.table.select.aggregation;

/**
 * Построитель агрегирующего запроса
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface AggregationQueryBuilderStep<T> {

    /**
     * Добавить запрос "количества значений" в колонке
     *
     * @param column название колонки
     * @param alias наименование колонки в результате выборки
     * @return ссылка на {@link T}
     */
    T count(String column, String alias);

    /**
     * Добавить запрос "среднего значения" значений в колонке.<br/>
     * Результат вычисления возвращается типа {@link java.math.BigDecimal}.<br/>
     * Используется точность по умолчанию - {@code 2}
     *
     * @param column название колонки
     * @param alias наименование колонки в результате выборки
     * @return ссылка на {@link T}
     */
    default T average(String column, String alias) {
        return average(column, alias, 2);
    }

    /**
     * Добавить запрос "среднего значения" значений в колонке.<br/>
     * Результат вычисления возвращается типа {@link java.math.BigDecimal}.<br/>
     *
     * @param column название колонки
     * @param alias наименование колонки в результате выборки
     * @param accuracy точность результата вычисления среднего
     * @return ссылка на {@link T}
     */
    T average(String column, String alias, int accuracy);

    /**
     * Добавить запрос "медианы значения" значений в колонке
     *
     * @param column название колонки
     * @param alias наименование колонки в результате выборки
     * @return ссылка на {@link T}
     */
    default T mean(String column, String alias) {
        return percentile(column, alias, 50);
    }

    /**
     * Добавить запрос "персентиля" значений в колонке
     *
     * @param column название колонки
     * @param alias наименование колонки в результате выборки
     * @param percentile значение персентилья
     * @return ссылка на {@link T}
     */
    T percentile(String column, String alias, int percentile);

    /**
     * Добавить запрос "максимального значения" в колонке
     *
     * @param column название колонки
     * @param alias наименование колонки в результате выборки
     * @return ссылка на {@link T}
     */
    T max(String column, String alias);

    /**
     * Добавить запрос "минимального значения" в колонке
     *
     * @param column название колонки
     * @param alias наименование колонки в результате выборки
     * @return ссылка на {@link T}
     */
    T min(String column, String alias);

    /**
     * Добавить запрос "суммы значений" в колонке
     *
     * @param column название колонки
     * @param alias наименование колонки в результате выборки
     * @return ссылка на {@link T}
     */
    T sum(String column, String alias);
}
