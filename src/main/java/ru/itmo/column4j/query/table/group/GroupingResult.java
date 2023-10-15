package ru.itmo.column4j.query.table.group;

import java.util.Iterator;
import java.util.Set;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface GroupingResult {
    /**
     * @return множество колонок из результирующей выборки
     */
    Set<String> getColumns();

    /**
     * @return множество колонок по которым выполнялась группировка
     */
    Set<String> getGroupingColumns();

    /**
     *
     * @return итетрирование по группам результата
     */
    Iterator<GroupingResultGroup> getGroups();
}
