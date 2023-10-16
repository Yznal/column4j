package ru.itmo.column4j.query.table.select.criteria;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface CriteriaQueryBuilderStep<P> {

    /**
     * Добавить критерий "значение в колонке строго равно" в запрос
     *
     * @param column название колонки
     * @param value значение для выборки
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    <T> P eq(String column, T value);

    /**
     * Добавить критерий "значение в колонке строго не равно" в запрос
     *
     * @param column название колонки
     * @param value значение для выборки
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    <T> P notEquals(String column, T value);

    /**
     * Добавить критерий "значение в колонке существует" в запрос
     *
     * @param column название колонки
     * @return ссылку на этот {@link P}
     */
    P exists(String column);

    /**
     * Добавить критерий "значение в колонке не существует" в запрос
     *
     * @param column название колонки
     * @return ссылку на этот {@link P}
     */
    P notExists(String column);

    /**
     * Добавить критерий "значение в колонке строго больше" в запрос
     *
     * @param column название колонки
     * @param value значение для выборки
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    <T extends Comparable<T>> P greater(String column, T value);

    /**
     * Добавить критерий "значение в колонке не строго больше" в запрос
     *
     * @param column название колонки
     * @param value значение для выборки
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    <T extends Comparable<T>> P greaterOrEquals(String column, T value);

    /**
     * Добавить критерий "значение в колонке строго меньше" в запрос
     *
     * @param column название колонки
     * @param value значение для выборки
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    <T extends Comparable<T>> P less(String column, T value);

    /**
     * Добавить критерий "значение в колонке не строго меньше" в запрос
     *
     * @param column название колонки
     * @param value значение для выборки
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    <T extends Comparable<T>> P lessOrEquals(String column, T value);

    /**
     * Добавить критерий "значение в колонке находится в интервале" в запрос
     *
     * @param column название колонки
     * @param from левая граница интервала
     * @param to правая граница интервала
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    default <T extends Comparable<T>> P between(String column, T from, T to) {
        return between(column, from, false, to, false);
    }

    /**
     * Добавить критерий "значение в колонке находится в интервале с настраиваемыми границами" в запрос
     *
     * @param column название колонки
     * @param from левая граница интервала
     * @param inclusiveFrom включается ли левая граница в фильтрацию
     * @param to правая граница интервала
     * @param inclusiveTo включается ли правая граница в фильтрацию
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    <T extends Comparable<T>> P between(String column, T from, boolean inclusiveFrom, T to, boolean inclusiveTo);

}
