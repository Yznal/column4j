package com.github.sibmaks.column4j.sample.query.table.select.criteria;

import com.github.sibmaks.column4j.sample.SampleTable;
import ru.itmo.column4j.query.table.select.criteria.CriteriaQueryBuilder;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class CriteriaQueryBuilderImpl<K, R> implements CriteriaQueryBuilder<R> {
    private final SampleTable<K> table;
    private final List<Map.Entry<String, Predicate<Optional<Object>>>> conditions;
    private final Function<List<Map.Entry<String, Predicate<Optional<Object>>>>, R> requestBuilder;

    public CriteriaQueryBuilderImpl(SampleTable<K> table,
                                    Function<List<Map.Entry<String, Predicate<Optional<Object>>>>, R> requestBuilder) {
        this.table = table;
        this.conditions = new ArrayList<>();
        this.requestBuilder = requestBuilder;
    }

    @Override
    public R build() {
        return requestBuilder.apply(List.copyOf(conditions));
    }

    @Override
    public <T> CriteriaQueryBuilder<R> eq(String column, T value) {
        if (!table.containsColumn(column)) {
            throw new IllegalArgumentException("Table not contains '%s' column".formatted(column));
        }
        conditions.add(Map.entry(column, it -> it.isPresent() && Objects.equals(it.get(), value)));
        return this;
    }

    @Override
    public <T> CriteriaQueryBuilder<R> notEquals(String column, T value) {
        if (!table.containsColumn(column)) {
            throw new IllegalArgumentException("Table not contains '%s' column".formatted(column));
        }
        conditions.add(Map.entry(column, it -> it.isEmpty() || !Objects.equals(it.get(), value)));
        return this;
    }

    @Override
    public CriteriaQueryBuilder<R> exists(String column) {
        if (!table.containsColumn(column)) {
            throw new IllegalArgumentException("Table not contains '%s' column".formatted(column));
        }
        conditions.add(Map.entry(column, Optional::isPresent));
        return this;
    }

    @Override
    public CriteriaQueryBuilder<R> notExists(String column) {
        if (!table.containsColumn(column)) {
            throw new IllegalArgumentException("Table not contains '%s' column".formatted(column));
        }
        conditions.add(Map.entry(column, Optional::isEmpty));
        return this;
    }

    @Override
    public <T extends Comparable<T>> CriteriaQueryBuilder<R> greater(String column, T value) {
        if (!table.containsColumn(column)) {
            throw new IllegalArgumentException("Table not contains '%s' column".formatted(column));
        }
        conditions.add(Map.entry(column, it -> it.isPresent() && value.compareTo((T) it.get()) <= 0));
        return this;
    }

    @Override
    public <T extends Comparable<T>> CriteriaQueryBuilder<R> greaterOrEquals(String column, T value) {
        if (!table.containsColumn(column)) {
            throw new IllegalArgumentException("Table not contains '%s' column".formatted(column));
        }
        conditions.add(Map.entry(column, it -> it.isPresent() && value.compareTo((T) it.get()) < 0));
        return this;
    }

    @Override
    public <T extends Comparable<T>> CriteriaQueryBuilder<R> less(String column, T value) {
        if (!table.containsColumn(column)) {
            throw new IllegalArgumentException("Table not contains '%s' column".formatted(column));
        }
        conditions.add(Map.entry(column, it -> it.isPresent() && value.compareTo((T) it.get()) >= 0));
        return this;
    }

    @Override
    public <T extends Comparable<T>> CriteriaQueryBuilder<R> lessOrEquals(String column, T value) {
        if (!table.containsColumn(column)) {
            throw new IllegalArgumentException("Table not contains '%s' column".formatted(column));
        }
        conditions.add(Map.entry(column, it -> it.isPresent() && value.compareTo((T) it.get()) > 0));
        return this;
    }

    @Override
    public <T extends Comparable<T>> CriteriaQueryBuilder<R> between(
            String column,
            T from,
            boolean inclusiveFrom,
            T to,
            boolean inclusiveTo) {
        if (inclusiveFrom) {
            greaterOrEquals(column, from);
        } else {
            greater(column, from);
        }
        if (inclusiveTo) {
            lessOrEquals(column, to);
        } else {
            less(column, to);
        }
        return this;
    }
}
