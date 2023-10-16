package com.github.sibmaks.column4j.sample;

import lombok.Getter;
import ru.itmo.column4j.ColumnType;
import ru.itmo.column4j.Table;
import ru.itmo.column4j.query.table.TableQueryBuilder;
import com.github.sibmaks.column4j.sample.query.table.TableQueryBuilderImpl;

import java.util.*;
import java.util.function.Predicate;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class SampleTable<K> implements Table<K> {
    @Getter
    private final String tableName;
    private final Map<String, ColumnType<?>> columnTypes;
    @Getter
    private final String primaryKey;
    @Getter
    private final ColumnType<K> primaryKeyType;
    private final Map<String, Map<K, Object>> values;
    @Getter
    private final TableQueryBuilder<K> tableQueryBuilder;

    public SampleTable(String tableName,
                       String primaryKey,
                       ColumnType<K> primaryKeyType,
                       Map<String, ColumnType<?>> columnTypes) {
        this.tableName = tableName;

        var columnTypesMutable = new HashMap<>(columnTypes);
        columnTypesMutable.put(primaryKey, primaryKeyType);
        this.columnTypes = Map.copyOf(columnTypesMutable);

        this.primaryKey = primaryKey;
        this.primaryKeyType = primaryKeyType;
        this.values = new HashMap<>();
        this.values.put(primaryKey, new HashMap<>());
        for (var column : columnTypes.keySet()) {
            this.values.put(column, new HashMap<>());
        }
        this.tableQueryBuilder = new TableQueryBuilderImpl<>(this);
    }

    @Override
    public String getName() {
        return tableName;
    }

    /**
     * Проверка наличия колонки в таблице
     *
     * @param columnName название колонки
     * @return {@code true} - колонка содержится в таблице, {@code false} - иначе
     */
    public boolean containsColumn(String columnName) {
        return values.containsKey(columnName);
    }

    /**
     * Получить тип данных в колонке
     *
     * @param columnName название колонок
     * @return тип данных
     */
    public ColumnType<?> getColumnType(String columnName) {
        return columnTypes.get(columnName);
    }

    /**
     * Содержит ли таблица запрашиваемый первичный ключ
     *
     * @param key первичный ключ
     * @return {@code true} - первичный ключ содержится в таблице, {@code false} - иначе
     */
    public boolean containsPrimaryKey(K key) {
        return values.get(primaryKey).containsKey(key);
    }

    /**
     * Добавить первичный ключ в таблицу
     * @param key первичный ключ
     */
    public void addPrimaryKey(K key) {
        var primaryKeys = values.get(primaryKey);
        primaryKeys.put(key, key);
    }

    /**
     * Добавить значение в колонку по первичному ключу
     *
     * @param key ключ
     * @param columName название колонки
     * @param value значение
     */
    public void addValue(K key, String columName, Object value) {
        var columnValues = this.values.get(columName);
        columnValues.put(key, value);
    }

    public Map<String, Map<K, Object>> getValues() {
        return Collections.unmodifiableMap(values);
    }

    /**
     * Получить все первичные ключи таблицы
     *
     * @return множество первичных ключей
     */
    public Set<K> getPrimaryKeys() {
        return values.get(primaryKey).keySet();
    }

    public <T> Optional<T> get(K key, String name) {
        return Optional.ofNullable(values.get(name))
                .map(it -> (T) it.get(key));
    }

    public Map<K, Object> getColumn(String column) {
        return values.get(column);
    }

    public Set<K> getFilteredPrimaryKeys(List<Map.Entry<String, Predicate<Optional<Object>>>> conditions) {
        var allPrimaryKeys = getPrimaryKeys();
        var filteredPrimaryKeys = new HashSet<K>();
        for (K primaryKey : allPrimaryKeys) {
            boolean valid = true;
            for (var condition : conditions) {
                var column = condition.getKey();
                var optionalValue = get(primaryKey, column);
                var predicate = condition.getValue();
                if(!predicate.test(optionalValue)) {
                    valid = false;
                    break;
                }
            }
            if(valid) {
                filteredPrimaryKeys.add(primaryKey);
            }
        }
        return filteredPrimaryKeys;
    }
}
