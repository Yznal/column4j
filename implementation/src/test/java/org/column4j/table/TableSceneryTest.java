package org.column4j.table;

import org.column4j.mutable.ColumnFactory;
import org.junit.jupiter.api.Test;

import java.util.Random;

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


    }

}