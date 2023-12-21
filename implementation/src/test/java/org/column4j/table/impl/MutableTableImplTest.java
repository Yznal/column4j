package org.column4j.table.impl;

import org.column4j.column.ColumnType;
import org.column4j.column.impl.mutable.StringMutableColumnImpl;
import org.column4j.column.impl.mutable.primitive.Float32MutableColumnImpl;
import org.column4j.column.impl.mutable.primitive.Int32MutableColumnImpl;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.table.MutableTable;
import org.eclipse.collections.impl.list.mutable.primitive.FloatArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

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
        IntArrayList allUsers = new IntArrayList(10240);
        FloatArrayList allCpuUsage = new FloatArrayList(10240);
        List<String> allHosts = new ArrayList<>(10240);
        for (int index = 0; index < 10240; index++) {
            var users = random.nextInt(0, 10000);
            var cpuUsage = random.nextFloat(0, 100);
            var host = hosts[random.nextInt(0, hosts.length)];

            allUsers.add(users);
            allCpuUsage.add(cpuUsage);
            allHosts.add(host);

            mutableTable.writeInt32(0, cursor + index, users);
            mutableTable.writeFloat32(1, cursor + index, cpuUsage);
            mutableTable.writeString(2, cursor + index, host);
        }

        assertEquals(11, columnUsersOnline.getChunks().size());
        assertEquals(21, columnCpuUsage.getChunks().size());
        assertEquals(1281, columnHost.getChunks().size());

         for (int index = 0; index < 10240; index++) {
            var users = allUsers.get(index);
            var cpuUsage = allCpuUsage.get(index);
            var host = allHosts.get(index);

            assertEquals(users, mutableTable.getInt32(0, cursor + index));
            assertEquals(users, columnUsersOnline.get(cursor + index));

            assertEquals(cpuUsage, columnCpuUsage.get(cursor + index));
            assertEquals(cpuUsage, mutableTable.getFloat32(1, cursor + index));

            assertEquals(host, columnHost.get(cursor + index));
            assertEquals(host, mutableTable.getString(2, cursor + index));
        }

        assertEquals(10, columnUsersOnline.getChunks().size());
        assertEquals(20, columnCpuUsage.getChunks().size());
        assertEquals(1280, columnHost.getChunks().size());

        int error = mutableTable.createColumn("users", ColumnType.INT32);
        assertEquals(-1, error);
        int createdColIndex = mutableTable.createColumn ("added", ColumnType.FLOAT64);
        assertNotEquals(-1, createdColIndex);

        MutableColumn<?, ?> retrieved = mutableTable.getColumn(createdColIndex);
        assertEquals(ColumnType.FLOAT64, retrieved.type());
    }


    @Test
    void testMutableTableWriteOne() {
        var columnUsersOnline = new Int32MutableColumnImpl(1024, 0);
        var columnCpuUsage = new Float32MutableColumnImpl(512, -1.f);
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
        IntArrayList allUsers = new IntArrayList();
        for (int index = 0; index < 10240; index++) {
            var users = random.nextInt(0, 10000);
            allUsers.add(users);

            mutableTable.writeInt32(0, cursor + index, users);
        }

        assertEquals(11, columnUsersOnline.getChunks().size());
        assertEquals(1, columnCpuUsage.getChunks().size());
        assertEquals(1, columnHost.getChunks().size());

        for (int index = 0; index < 10240; index++) {
            var original = allUsers.get(index);
            assertEquals(original, columnUsersOnline.get(cursor + index));
            assertEquals(original, mutableTable.getInt32(0, cursor + index));

            assertEquals(-1.f, columnCpuUsage.get(cursor + index));
            assertEquals(-1.f, mutableTable.getFloat32(1, cursor + index));

            assertNull(columnHost.get(cursor + index));
            assertNull(mutableTable.getString(2, cursor + index));
        }
    }

    @Test
    void testMutableTableWriteSparse() {
        Random random = new Random(11);
        int usersOnlineTombstone = 0;
        float cpuUsageTombstone = -1.f;
        String hostTombstone = null;

        var columnUsersOnline = new Int32MutableColumnImpl(1024, usersOnlineTombstone);
        var columnCpuUsage = new Float32MutableColumnImpl(512, cpuUsageTombstone);
        var columnHost = new StringMutableColumnImpl(8, hostTombstone);

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
        IntArrayList allUsers = new IntArrayList(10240);
        FloatArrayList allCpuUsage = new FloatArrayList(10240);
        List<String> allHosts = new ArrayList<>(10240);

        Random usersRandom = new Random(17);
        Random cpuUsageRandom = new Random(33);
        Random hostRandom = new Random(42);

        int usersValues = 0;
        int cpuUsageValues = 0;
        int hostValues = 0;

        for (int index = 0; index < 10240; index++) {
            var users = usersRandom.nextBoolean() ? usersOnlineTombstone : random.nextInt(0, 10000);
            var cpuUsage = cpuUsageRandom.nextBoolean() ? cpuUsageTombstone : random.nextFloat(0, 100);
            var host = hostRandom.nextBoolean() ? hostTombstone : hosts[random.nextInt(0, hosts.length)];

            allUsers.add(users);
            allCpuUsage.add(cpuUsage);
            allHosts.add(host);

            if (users != usersOnlineTombstone) {
                mutableTable.writeInt32(0, cursor + index, users);
                usersValues++;
            }
            if (Float.compare(cpuUsage, cpuUsageTombstone) != 0) {
                mutableTable.writeFloat32(1, cursor + index, cpuUsage);
                cpuUsageValues++;
            }
            if (host != hostTombstone) {
                mutableTable.writeString(2, cursor + index, host);
                hostValues++;
            }
        }

        assertEquals(11, columnUsersOnline.getChunks().size());
        assertEquals(21, columnCpuUsage.getChunks().size());
        assertEquals(1281, columnHost.getChunks().size());

         for (int index = 0; index < 10240; index++) {
            var users = allUsers.get(index);
            var cpuUsage = allCpuUsage.get(index);
            var host = allHosts.get(index);

            assertEquals(users, mutableTable.getInt32(0, cursor + index));
            assertEquals(users, columnUsersOnline.get(cursor + index));

            assertEquals(cpuUsage, columnCpuUsage.get(cursor + index));
            assertEquals(cpuUsage, mutableTable.getFloat32(1, cursor + index));

            assertEquals(host, columnHost.get(cursor + index));
            assertEquals(host, mutableTable.getString(2, cursor + index));
        }
    }


}