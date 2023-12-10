package org.column4j.table.impl;

import org.column4j.column.impl.mutable.StringMutableColumnImpl;
import org.column4j.column.impl.mutable.primitive.Float32MutableColumnImpl;
import org.column4j.column.impl.mutable.primitive.Int32MutableColumnImpl;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.table.MutableTable;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class MutableTableImplTest {
    private final Random random = new Random();
    private final String[] hosts = {"host_a", "host_b", null};

    @Test
    void testMutableTableUsage() {
        var columnUsersOnline = new Int32MutableColumnImpl(1024, 0);
        var columnCpuUsage = new Float32MutableColumnImpl(512, -1);
        var columnHost = new StringMutableColumnImpl(8, null);

        var columns = new LinkedHashMap<String, MutableColumn<?, ?>>();
        columns.put("users", columnUsersOnline);
        columns.put("cpu_usage", columnCpuUsage);
        columns.put("host", columnHost);

        MutableTable mutableTable = new MutableTableImpl(columns);

        int cursor = 0;
        mutableTable.writeInt32(0, cursor, 100);
        mutableTable.writeFloat32("cpu_usage", cursor, 5f);
        mutableTable.writeString(2, cursor, "host_a");

        assertEquals(100, mutableTable.getInt32("users", cursor));
        assertEquals(5f, mutableTable.getFloat32(1, cursor));
        assertEquals("host_a", mutableTable.getString("host", cursor));

        assertEquals(100, columnUsersOnline.get(cursor));
        assertEquals(5f, columnCpuUsage.get(cursor));
        assertEquals("host_a", columnHost.get(cursor));

        cursor++;
        for (int index = 0; cursor < 10240; cursor++, index++) {
            var users = random.nextInt(0, 10000);
            var cpuUsage = random.nextFloat(0, 100);
            var host = hosts[random.nextInt(0, hosts.length)];

            mutableTable.writeInt32(0, cursor, users);
            mutableTable.writeFloat32(1, cursor, cpuUsage);
            mutableTable.writeString(2, cursor, host);
        }

        assertEquals(10, columnUsersOnline.getChunks().size());
        assertEquals(20, columnCpuUsage.getChunks().size());
        assertEquals(1280, columnHost.getChunks().size());
    }

}