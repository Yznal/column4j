package com.github.sibmaks.column4j.sample.query.table.select.aggregation.aggregator.impl.sum;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.itmo.column4j.ColumnType;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SumAggregators {
    private static final Map<Class<?>, SumAggregatorCreator> SUM_CREATORS = Map.of(
            Integer.class, build(Integer.class, 0, Math::addExact),
            Long.class, build(Long.class, 0L, Math::addExact),
            Double.class, build(Double.class, 0., Double::sum),
            Float.class, build(Float.class, 0f, Float::sum)
    );

    /**
     * Получить сумм-агрегатор для колонки
     *
     * @param tableName название таблицы
     * @param columnName название колонки
     * @param columnType тип данных в колонке
     * @return сумм-агрегатор
     */
    public static SumAggregator<?> getSumAggregator(String tableName,
                                                    String columnName,
                                                    ColumnType<?> columnType) {
        var columnClazz = columnType.getType();
        var creator = SUM_CREATORS.get(columnClazz);
        return creator.create(tableName, columnName, columnType);
    }

    private static <T extends Number> SumAggregatorCreator build(Class<T> type, T base, BiFunction<T, T, T> summator) {
        return (tableName, columnName, columnType) -> new SumAggregator<>(tableName, columnName, columnType, type, base, summator);
    }
}
