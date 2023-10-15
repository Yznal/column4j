package ru.itmo.column4j.query.table.select.criteria;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface CriteriaQueryBuilderStep<P> {

    /**
     * Добавить критерий "значение в колонке строго равно" в запрос
     *
     * @param name название колонки
     * @param value значение для выборки
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    <T> P eq(String name, T value);

    /**
     * Добавить критерий "значение в колонке строго больше" в запрос
     *
     * @param name название колонки
     * @param value значение для выборки
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    <T> P greater(String name, T value);

    /**
     * Добавить критерий "значение в колонке не строго больше" в запрос
     *
     * @param name название колонки
     * @param value значение для выборки
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    <T> P greaterOrEquals(String name, T value);

    /**
     * Добавить критерий "значение в колонке строго меньше" в запрос
     *
     * @param name название колонки
     * @param value значение для выборки
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    <T> P less(String name, T value);

    /**
     * Добавить критерий "значение в колонке не строго меньше" в запрос
     *
     * @param name название колонки
     * @param value значение для выборки
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    <T> P lessOrEquals(String name, T value);

    /**
     * Добавить критерий "значение в колонке находится в интервале" в запрос
     *
     * @param name название колонки
     * @param from левая граница интервала
     * @param to правая граница интервала
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    default <T> P between(String name, T from, T to) {
        return between(name, from, false, to, false);
    }

    /**
     * Добавить критерий "значение в колонке находится в интервале с настраиваемыми границами" в запрос
     *
     * @param name название колонки
     * @param from левая граница интервала
     * @param inclusiveFrom включается ли левая граница в фильтрацию
     * @param to правая граница интервала
     * @param inclusiveTo включается ли правая граница в фильтрацию
     * @return ссылку на этот {@link P}
     * @param <T> тип значения
     */
    <T> P between(String name, T from, boolean inclusiveFrom, T to, boolean inclusiveTo);

}
