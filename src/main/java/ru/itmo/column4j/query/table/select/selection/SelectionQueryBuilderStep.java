package ru.itmo.column4j.query.table.select.selection;

/**
 * Построитель запроса простой выборки
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface SelectionQueryBuilderStep<P> {

    /**
     * Добавить запрос колонки в результирующую выборку без агрегации
     *
     * @param column название колонки
     * @return ссылка на {@link P}
     */
    default P column(String column) {
        return column(column, column);
    }

    /**
     * Добавить запрос колонки в результирующую выборку без агрегации с другим названием столбца
     *
     * @param column название колонки
     * @param alias наименование колонки в результате выборки
     * @return ссылка на {@link P}
     */
    P column(String column, String alias);

}
