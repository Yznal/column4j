package org.column4j.table.query.select;

import java.util.Iterator;
import java.util.Set;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface SelectionResult {
    /**
     * @return set of result set columns
     */
    Set<String> getColumns();

    /**
     * @return iterate result set rows
     */
    Iterator<SelectionResultRow> getRows();

}
