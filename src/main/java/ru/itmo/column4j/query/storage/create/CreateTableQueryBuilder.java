package ru.itmo.column4j.query.storage.create;

import ru.itmo.column4j.ColumnType;
import ru.itmo.column4j.TableEngine;

import java.util.Map;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public interface CreateTableQueryBuilder<K> {

    /**
     * @return имя создаваемой таблицы
     */
    String getTableName();

    /**
     * @return имя первичного ключа
     */
    String getPrimaryKey();

    /**
     * @return тип первичного ключа
     */
    ColumnType<K> getPrimaryKeyType();

    /**
     * @return колонки, которые нужно создать в таблице
     */
    Map<String, ColumnType<?>> getColumns();

    /**
     * Добавляем колонку к создаваемой таблице
     *
     * @param name название колонки
     * @param columnType тип данных в колонке
     * @return ссылку на этот {@link CreateTableQueryBuilder}
     */
    <T> CreateTableQueryBuilder<K> column(String name, ColumnType<T> columnType);

    /**
     * Выбор "движка" для управления таблицей
     *
     * @param tableEngine тип "движка"
     * @return builder запроса создания таблицы со спецификой "движка"
     */
    <T extends CreateTableQueryEngineBuilder<K>> T engine(TableEngine<K, T> tableEngine);
}
