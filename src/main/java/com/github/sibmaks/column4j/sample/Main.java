package com.github.sibmaks.column4j.sample;

import ru.itmo.column4j.impl.ColumnTypes;
import ru.itmo.column4j.impl.StorageImpl;
import ru.itmo.column4j.query.table.group.GroupingResult;
import ru.itmo.column4j.query.table.group.GroupingResultGroup;
import ru.itmo.column4j.query.table.insert.InsertQueryBuilder;
import ru.itmo.column4j.query.table.select.SelectionResult;

import java.util.List;
import java.util.Set;

/**
 * @author sibmaks
 * @since 0.0.1
 */
public class Main {
    public static void main(String[] args) {
        var storage = new StorageImpl();

        var schemaQueryBuilder = storage.getStorageQueryBuilder();

        var createTableQuery = schemaQueryBuilder
                .createTable("my_timestamps", "timestamp", ColumnTypes.LONG)
                .column("dc", ColumnTypes.STRING)
                .column("cpu_usage", ColumnTypes.DOUBLE)
                .column("ram_usage", ColumnTypes.INT)
                .engine(new SampleEngine<>())
                .build();

        var table = storage.execute(createTableQuery);

        var dataQueryBuilder = table.getTableQueryBuilder();

        long pk1 = System.currentTimeMillis();
        long pk2 = pk1 + 1;
        dataQueryBuilder.<Long>insert()
                .add(pk1)
                .column("dc", "alpha")
                .column("cpu_usage", 90.1)
                // вставка данных пачкой за раз
                .add(pk2)
                .column("dc", "beta")
                .column("cpu_usage", 10)
                .column("ram_usage", 2)
                .build()
                .execute();

        // создание запроса через циклы
        var insertionQueryLoopBuilder = dataQueryBuilder.<Long>insert();
        InsertQueryBuilder<Long> insertQueryBuilder = null;
        int i = -1000;
        for (var dc : List.of("alpha", "beta", "omega")) {
            insertQueryBuilder = (insertQueryBuilder == null ? insertionQueryLoopBuilder : insertQueryBuilder)
                    .add(System.currentTimeMillis() + i--)
                    .column("dc", dc);
        }
        var insertionQueryLoop = insertQueryBuilder.build().execute();


        long pk3 = pk2 + 1;
        // ASK: Возможна ли создание primary key без меток?
        dataQueryBuilder.<Long>insert()
                .add(pk3)
                .build();

        var selection = dataQueryBuilder.select()
                .column("timestamp")
                .column("dc")
                .column("cpu_usage")
                .column("ram_usage")
                .build()
                .execute();

        System.out.println("Selection result");
        printQueryResult(selection);

        var selectionFiltered = dataQueryBuilder.select()
                .column("timestamp")
                .column("dc")
                .column("cpu_usage")
                .column("ram_usage")
                .where()
                .eq("dc", "alpha")
                .exists("cpu_usage")
                .build()
                .execute();

        System.out.println("Selection filtered result");
        printQueryResult(selectionFiltered);

        var aggregations = dataQueryBuilder.select()
                .count("cpu_usage", "cnt_cpu_usage")
                .max("cpu_usage", "max_cpu_usage")
                .min("cpu_usage", "min_cpu_usage")
                .average("cpu_usage", "avg_cpu_usage")
                .sum("cpu_usage", "sum_cpu_usage")

                .average("ram_usage", "avg_ram_usage")
                .build()
                .execute();

        System.out.println("Aggregation result");
        printQueryResult(aggregations);

        var grouping = dataQueryBuilder
                .groupBy("dc")
                .column("dc", "data-center")
                .count("cpu_usage", "cnt_cpu_usage")
                .max("cpu_usage", "max_cpu_usage")
                .min("cpu_usage", "min_cpu_usage")
                .average("cpu_usage", "avg_cpu_usage")
                .sum("cpu_usage", "sum_cpu_usage")
                .average("ram_usage", "avg_ram_usage")
                .build()
                .execute();

        System.out.println("Grouping result");
        printQueryResult(grouping);
    }

    private static void printQueryResult(SelectionResult queryResult) {
        var rsColumns = queryResult.getColumns();
        var rows = queryResult.getRows();
        while (rows.hasNext()) {
            var row = rows.next();
            for (var column : rsColumns) {
                System.out.printf("'%s'='%s'%n", column, row.get(column));
            }
            System.out.println("----------------------------");
        }
    }

    private static void printQueryResult(GroupingResult queryResult) {
        var columns = queryResult.getColumns();
        var groups = queryResult.getGroups();
        while (groups.hasNext()) {
            printQueryResult(columns, groups.next());
        }
    }
    private static void printQueryResult(Set<String> columns, GroupingResultGroup group) {
        var groupColumnName = group.getGroupColumnName();
        var groupColumnValue = group.getGroupColumnValue();
        System.out.printf("Group: '%s'='%s'%n".formatted(groupColumnName, groupColumnValue));
        var values = group.getValues();
        if(values == null) {
            var groups = group.getGroups();
            while (groups.hasNext()) {
                var innerGroup = groups.next();
                printQueryResult(columns, innerGroup);
            }
        } else {
            System.out.println("Values");
            while (values.hasNext()) {
                var row = values.next();
                for (var column : columns) {
                    System.out.printf("'%s'='%s'%n", column, row.get(column));
                }
                System.out.println("----------------------------");
            }
        }
    }
}