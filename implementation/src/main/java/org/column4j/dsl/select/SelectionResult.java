package org.column4j.dsl.select;

import java.util.Collection;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface SelectionResult {
    /**
     * @return collection of result columns aliases
     */
    Collection<String> getColumns();

    /**
     * @return iterable result set rows
     */
    Iterable<SelectionResultRow> getRows();

}
