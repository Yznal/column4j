package ru.itmo.column4j.query.table.select;

import java.util.Iterator;
import java.util.Set;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface SelectionResult {
    /**
     * @return множество колонок из результирующей выборки
     */
    Set<String> getColumns();

    /**
     * @return итетрирование по строкам результата
     */
    Iterator<SelectionResultRow> getRows();

}
