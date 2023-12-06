package org.column4j.table;

import org.column4j.mutable.ColumnFactory;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class TableSceneryTest {

    private final Random random = new Random();

    @Test
    void testCreateInsertSelect() {
        var table = Table.builder()
                .column("users", ColumnFactory.createInt32Column(0, 1024))
                .column("cpu_usage", ColumnFactory.createFloat32Column(0, 512))
                .column("errors", ColumnFactory.createInt16Column((short) -1, 512))
                .build();

        for (int i = 0; i < 10240; i++) {
            var users = random.nextInt(0, 1000);
            var cpuUsage = random.nextFloat(0.01f, 100f);

            var query = table.insert()
                    .column("users", users)
                    .column("cpu_usage", cpuUsage);

            if (random.nextInt(0, 1024) >= 1024 * 0.75) {
                short errors = (short) random.nextInt(1, 1000);
                query.column("errors", errors);
            }

            query.commit();
        }

        // TODO: implement select query
        var selectionResult = table.select()
                .column("users")
                .column("cpu_usage")
                .column("errors")
                .where()
                .between("errors", 1, true, Short.MAX_VALUE, true)
                .execute();

        var selectionColumns = selectionResult.getColumns();
        assertEquals(3, selectionColumns.size());
        assertTrue(selectionColumns.contains("users"));
        assertTrue(selectionColumns.contains("cpu_usage"));
        assertTrue(selectionColumns.contains("errors"));

        var aggregationResult = table.select()
                .min("users", "users_min")
                .max("users", "users_max")
                .count("users", "users_count")
                .where()
                .between("errors", 1, true, Short.MAX_VALUE, true)
                .execute();

        var columns = aggregationResult.getColumns();
        assertEquals(3, columns.size());
        assertTrue(columns.contains("users_min"));
        assertTrue(columns.contains("users_max"));
        assertTrue(columns.contains("users_count"));

        var rows = aggregationResult.getRows();
        assertTrue(rows.hasNext());

        var aggregatedRow = rows.next();
        assertFalse(rows.hasNext());
    }

}