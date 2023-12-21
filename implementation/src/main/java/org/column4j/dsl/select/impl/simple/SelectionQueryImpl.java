package org.column4j.dsl.select.impl.simple;

import org.column4j.dsl.select.SelectionResult;
import org.column4j.dsl.select.criteria.CriteriaQuery;
import org.column4j.dsl.select.criteria.CriteriaQueryStep;
import org.column4j.dsl.select.selection.SelectionQuery;
import org.column4j.table.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.IntPredicate;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class SelectionQueryImpl implements SelectionQuery, CriteriaQuery<SelectionResult> {
    private final Table table;
    private final List<AliesColumn> columns = new ArrayList<>();
    private IntPredicate conditions = value -> true;

    public SelectionQueryImpl(Table table) {
        this.table = table;
    }

    @Override
    public CriteriaQueryStep<CriteriaQuery<SelectionResult>> where() {
        return this;
    }

    @Override
    public SelectionQuery column(String column, String alias) {
        int columnIndex = table.getColumnIndex(column);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column '%s' doesn't exists!".formatted(column));
        }
        for (var aliesColumn : columns) {
            if (aliesColumn.alias().equals(alias)) {
                if (aliesColumn.index() != columnIndex) {
                    throw new IllegalArgumentException("Alias '%s' already in query".formatted(alias));
                }
                break;
            }
        }
        columns.add(new AliesColumn(columnIndex, alias));
        return this;
    }

    @Override
    public CriteriaQuery<SelectionResult> eq(String name, byte value) {
        conditions = conditions.and(cursor -> table.getInt8(name, cursor) == value);
        return this;
    }

    @Override
    public CriteriaQuery<SelectionResult> eq(String name, short value) {
        conditions = conditions.and(cursor -> table.getInt16(name, cursor) == value);
        return this;
    }

    @Override
    public CriteriaQuery<SelectionResult> eq(String name, int value) {
        conditions = conditions.and(cursor -> table.getInt32(name, cursor) == value);
        return this;
    }

    @Override
    public CriteriaQuery<SelectionResult> eq(String name, long value) {
        conditions = conditions.and(cursor -> table.getInt64(name, cursor) == value);
        return this;
    }

    @Override
    public CriteriaQuery<SelectionResult> eq(String name, float value) {
        conditions = conditions.and(cursor -> table.getFloat32(name, cursor) == value);
        return this;
    }

    @Override
    public CriteriaQuery<SelectionResult> eq(String name, double value) {
        conditions = conditions.and(cursor -> table.getFloat64(name, cursor) == value);
        return this;
    }

    @Override
    public CriteriaQuery<SelectionResult> eq(String name, String value) {
        conditions = conditions.and(cursor -> Objects.equals(table.getString(name, cursor), value));
        return this;
    }

    @Override
    public CriteriaQuery<SelectionResult> contains(String name, String value) {
        conditions = conditions.and(cursor -> isStringContains(name, cursor, value));
        return this;
    }

    private boolean isStringContains(String name, int cursor, String value) {
        var cellValue = table.getString(name, cursor);
        return Optional.ofNullable(cellValue)
                .filter(it -> it.contains(value))
                .isPresent();
    }

    @Override
    public CriteriaQuery<SelectionResult> between(String name, byte from, boolean inclusiveFrom, byte to, boolean inclusiveTo) {
        conditions = conditions.and(cursor -> {
            var value = table.getInt8(name, cursor);
            if (inclusiveFrom) {
                if (value < from) {
                    return false;
                }
            } else if (value <= from) {
                return false;
            }
            if (inclusiveTo) {
                return value <= to;
            } else {
                return value < to;
            }
        });
        return this;
    }

    @Override
    public CriteriaQuery<SelectionResult> between(String name, short from, boolean inclusiveFrom, short to, boolean inclusiveTo) {
        conditions = conditions.and(cursor -> {
            var value = table.getInt16(name, cursor);
            if (inclusiveFrom) {
                if (value < from) {
                    return false;
                }
            } else if (value <= from) {
                return false;
            }
            if (inclusiveTo) {
                return value <= to;
            } else {
                return value < to;
            }
        });
        return this;
    }

    @Override
    public CriteriaQuery<SelectionResult> between(String name, int from, boolean inclusiveFrom, int to, boolean inclusiveTo) {
        conditions = conditions.and(cursor -> {
            var value = table.getInt32(name, cursor);
            if (inclusiveFrom) {
                if (value < from) {
                    return false;
                }
            } else if (value <= from) {
                return false;
            }
            if (inclusiveTo) {
                return value <= to;
            } else {
                return value < to;
            }
        });
        return this;
    }

    @Override
    public CriteriaQuery<SelectionResult> between(String name, long from, boolean inclusiveFrom, long to, boolean inclusiveTo) {
        conditions = conditions.and(cursor -> {
            var value = table.getInt64(name, cursor);
            if (inclusiveFrom) {
                if (value < from) {
                    return false;
                }
            } else if (value <= from) {
                return false;
            }
            if (inclusiveTo) {
                return value <= to;
            } else {
                return value < to;
            }
        });
        return this;
    }

    @Override
    public CriteriaQuery<SelectionResult> between(String name, float from, boolean inclusiveFrom, float to, boolean inclusiveTo) {
        conditions = conditions.and(cursor -> {
            var value = table.getFloat32(name, cursor);
            if (inclusiveFrom) {
                if (value < from) {
                    return false;
                }
            } else if (value <= from) {
                return false;
            }
            if (inclusiveTo) {
                return value <= to;
            } else {
                return value < to;
            }
        });
        return this;
    }

    @Override
    public CriteriaQuery<SelectionResult> between(String name, double from, boolean inclusiveFrom, double to, boolean inclusiveTo) {
        conditions = conditions.and(cursor -> {
            var value = table.getFloat64(name, cursor);
            if (inclusiveFrom) {
                if (value < from) {
                    return false;
                }
            } else if (value <= from) {
                return false;
            }
            if (inclusiveTo) {
                return value <= to;
            } else {
                return value < to;
            }
        });
        return this;
    }

    @Override
    public SelectionResult execute() {
        return new SelectionResultImpl(table, columns, conditions);
    }
}