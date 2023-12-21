package org.column4j.dsl.select.impl.simple;

import org.column4j.column.impl.mutable.StringMutableColumnImpl;
import org.column4j.column.impl.mutable.primitive.Float32MutableColumnImpl;
import org.column4j.column.impl.mutable.primitive.Int32MutableColumnImpl;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.table.MutableTable;
import org.column4j.table.impl.MutableTableImpl;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class SimpleQueryFactoryTest {
    private static final Random RANDOM = new Random();

    @Test
    void testSelectEmptyTable() {
        var columnUsersOnline = new Int32MutableColumnImpl(1024, 0);
        var columnCpuUsage = new Float32MutableColumnImpl(1024, -1);
        var columnHost = new StringMutableColumnImpl(1024, null);

        var columns = new LinkedHashMap<String, MutableColumn<?, ?>>();
        columns.put("users", columnUsersOnline);
        columns.put("cpu_usage", columnCpuUsage);
        columns.put("host", columnHost);

        MutableTable mutableTable = new MutableTableImpl(columns);

        var selectionResult = SimpleQueryFactory.select(mutableTable)
                .column("users")
                .column("cpu_usage")
                .column("host")
                .execute();

        var rows = selectionResult.getRows();
        assertFalse(rows.iterator().hasNext());

    }

    @Test
    void testSelectWithoutFilters() {
        var columnUsersOnline = new Int32MutableColumnImpl(1024, 0);
        var columnCpuUsage = new Float32MutableColumnImpl(1024, -1);
        var columnHost = new StringMutableColumnImpl(1024, null);

        var columns = new LinkedHashMap<String, MutableColumn<?, ?>>();
        columns.put("users", columnUsersOnline);
        columns.put("cpu_usage", columnCpuUsage);
        columns.put("host", columnHost);

        MutableTable mutableTable = new MutableTableImpl(columns);

        var cursor = 0;
        var inserted = RANDOM.nextInt(64, 1024);
        for (; cursor < inserted; cursor++) {
            var users = RANDOM.nextInt(0, 1024);
            var cpuUsage = RANDOM.nextFloat(0, 100);
            var host = String.valueOf(RANDOM.nextInt(0, 8));

            mutableTable.writeInt32("users", cursor, users);
            mutableTable.writeFloat32("cpu_usage", cursor, cpuUsage);
            mutableTable.writeString("host", cursor, host);
        }

        // select without filter
        var selectionResult = SimpleQueryFactory.select(mutableTable)
                .column("users")
                .column("cpu_usage")
                .column("host", "app_host")
                .execute();

        var actualRows = 0;
        for (var row : selectionResult.getRows()) {
            assertEquals(columnUsersOnline.get(actualRows), row.getInt32("users"));
            assertEquals(columnCpuUsage.get(actualRows), row.getFloat32("cpu_usage"));
            assertEquals(columnHost.get(actualRows), row.getString("app_host"));
            actualRows++;
        }

        assertEquals(inserted, actualRows);
    }

    @Test
    void testSelectWithExcludeAllFilter() {
        var columnUsersOnline = new Int32MutableColumnImpl(1024, 0);
        var columnCpuUsage = new Float32MutableColumnImpl(1024, -1);
        var columnHost = new StringMutableColumnImpl(1024, null);

        var columns = new LinkedHashMap<String, MutableColumn<?, ?>>();
        columns.put("users", columnUsersOnline);
        columns.put("cpu_usage", columnCpuUsage);
        columns.put("host", columnHost);

        MutableTable mutableTable = new MutableTableImpl(columns);

        var cursor = 0;
        var inserted = RANDOM.nextInt(64, 1024);
        for (; cursor < inserted; cursor++) {
            var users = RANDOM.nextInt(0, 1024);
            var cpuUsage = RANDOM.nextFloat(0, 100);
            var host = String.valueOf(RANDOM.nextInt(0, 8));

            mutableTable.writeInt32("users", cursor, users);
            mutableTable.writeFloat32("cpu_usage", cursor, cpuUsage);
            mutableTable.writeString("host", cursor, host);
        }

        // select without filter
        var selectionResult = SimpleQueryFactory.select(mutableTable)
                .column("users")
                .column("cpu_usage")
                .column("host", "app_host")
                .where()
                .eq("host", "9")
                .execute();

        var rows = selectionResult.getRows();
        assertFalse(rows.iterator().hasNext());
    }

    @Test
    void testSelectWithFilter() {
        var columnUsersOnline = new Int32MutableColumnImpl(1024, 0);
        var columnCpuUsage = new Float32MutableColumnImpl(1024, -1);
        var columnHost = new StringMutableColumnImpl(1024, null);

        var columns = new LinkedHashMap<String, MutableColumn<?, ?>>();
        columns.put("users", columnUsersOnline);
        columns.put("cpu_usage", columnCpuUsage);
        columns.put("host", columnHost);

        MutableTable mutableTable = new MutableTableImpl(columns);

        var cursor = 0;
        var hostsEquals1 = 0;
        var inserted = RANDOM.nextInt(64, 1024);
        for (; cursor < inserted; cursor++) {
            var users = RANDOM.nextInt(0, 1024);
            var cpuUsage = RANDOM.nextFloat(0, 100);
            var host = String.valueOf(RANDOM.nextInt(0, 8));
            if("1".equals(host)) {
                hostsEquals1++;
            }

            mutableTable.writeInt32("users", cursor, users);
            mutableTable.writeFloat32("cpu_usage", cursor, cpuUsage);
            mutableTable.writeString("host", cursor, host);
        }

        var selectionFilteredResult = SimpleQueryFactory.select(mutableTable)
                .column("users")
                .column("cpu_usage")
                .column("host", "app_host")
                .where()
                .eq("host", "1")
                .execute();

        var actualFilteredRows = 0;
        for (var row : selectionFilteredResult.getRows()) {
            actualFilteredRows++;
        }

        assertEquals(hostsEquals1, actualFilteredRows);

    }

}