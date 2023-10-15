package ru.itmo.column4j.query.table.select.selection;

import ru.itmo.column4j.query.table.select.CommonSelectBuilder;
import ru.itmo.column4j.query.table.select.SelectQuery;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface SelectionQueryBuilder extends SelectionQueryBuilderStep<SelectionQueryBuilder>,
        CommonSelectBuilder<SelectQuery> {

}
