package org.column4j.storage;

import org.column4j.column.ColumnType;
import org.column4j.column.mutable.MutableColumn;
import org.column4j.index.Dimension;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.column4j.index.Dimension.dim;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StorageImplTest {

    List<Collection<Dimension>> keys = List.of(
            List.of(dim("host", "a"), dim("zone", "east-1"), dim("name", "cpu")),
            List.of(dim("host", "b"), dim("zone", "east-1"), dim("name", "cpu")),
            List.of(dim("method", "GET"), dim("uri", "/ping"), dim("name", "resp_time")),
            List.of(dim("method", "POST"), dim("uri", "/payment"), dim("port", "443"), dim("name", "resp_time"))
    );

    @Test
    void testStorage() {
        Storage testStorage = new StorageImpl();

        keys.forEach(dims -> {
            assertEquals(0, testStorage.findColumns(dims).size());
        });

        testStorage.createColumn(keys.get(0), "0", ColumnType.INT32);
        testStorage.createColumn(keys.get(1), "1", ColumnType.INT32);
        testStorage.createColumn(keys.get(2), "2", ColumnType.FLOAT32);
        testStorage.createColumn(keys.get(3), "3", ColumnType.FLOAT64);

        List<MutableColumn<?, ?>> resZone = testStorage.findColumns(List.of(dim("zone", "east-1")));
        assertEquals(2, resZone.size());
        assertEquals(ColumnType.INT32, resZone.get(0).type());
        assertEquals(ColumnType.INT32, resZone.get(1).type());
        List<MutableColumn<?, ?>> resUri = testStorage.findColumns(List.of(dim("name", "resp_time")));
        assertEquals(2, resUri.size());
        if (resUri.get(0).type() == ColumnType.FLOAT32) {
            assertEquals(ColumnType.FLOAT64, resUri.get(0).type());
        } else if (resUri.get(0).type() == ColumnType.FLOAT64) {
            assertEquals(ColumnType.FLOAT32, resUri.get(0).type());
        } else {
            throw new IllegalStateException("Types mismatch");
        }

        assertThrows(IllegalArgumentException.class, () ->testStorage.createColumn(List.of(dim("name", "resp_time")), "1", ColumnType.FLOAT64));
    }
}