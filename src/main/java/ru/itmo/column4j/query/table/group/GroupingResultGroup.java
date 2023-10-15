package ru.itmo.column4j.query.table.group;

import java.util.Iterator;

/**
 * Группа результата группировки
 *
 * @author sibmaks
 * @since 0.0.1
 */
public interface GroupingResultGroup {
    /**
     * @return название колонки по которой выполнена группировка
     */
    String getGroupColumnName();

    /**
     * @return значение колонки по которой выполнена группировка
     */
    Object getGroupColumnValue();

    /**
     * Если текущий результат является группой, то возвращается {@code null}
     *
     * @return множество значений сгруппированной выборки
     */
    Iterator<GroupingResultRow> getValues();

    /**
     * @return итетрирование по группам результата
     */
    Iterator<GroupingResultGroup> getGroups();
}
