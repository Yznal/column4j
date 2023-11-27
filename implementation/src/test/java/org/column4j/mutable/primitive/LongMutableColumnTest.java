package org.column4j.mutable.primitive;

import org.column4j.ColumnType;
import org.column4j.mutable.primitive.impl.LongMutableColumnImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class LongMutableColumnTest {

    @Test
    void testColumnTypeIsINT64() {
        var column = new LongMutableColumnImpl();
        assertEquals(ColumnType.INT64, column.type());
    }

    @Test
    void testEmptyColumnNotCreateEmptyArrays() {
        var column1 = new LongMutableColumnImpl();
        var column2 = new LongMutableColumnImpl();
        assertSame(column1.getData(), column2.getData());
    }

    @Test
    void testEmptyColumnDefaultTombstoneIsLongMaxValue() {
        var column = new LongMutableColumnImpl();
        long tombstone = column.getTombstone();
        assertEquals(Long.MAX_VALUE, tombstone);
    }

    @Test
    void testEmptyColumn1stRowIsNotDefined() {
        var column = new LongMutableColumnImpl();
        assertEquals(-1, column.firstRowIndex());
    }

    @Test
    void testEmptyColumnNotUseSpace() {
        var column = new LongMutableColumnImpl();
        assertEquals(0, column.capacity());
        assertEquals(0, column.size());

        var data = column.getData();
        assertNotNull(data);
        assertEquals(0, data.length);
    }

    @Test
    void testColumnWithCustomTombstone() {
        long tombstone = 0;
        var column = new LongMutableColumnImpl(tombstone);
        assertEquals(-1, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(-1, column.firstRowIndex());

        long[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstone via method works the same as #tombstone method calling
        column.write(0, tombstone);

        assertEquals(-1, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        long longValue = 1;
        column.write(0, longValue);
        assertEquals(0, column.firstRowIndex());

        data = column.getData();
        assertEquals(longValue, data[0]);
    }

    @Test
    void testColumnWithCustomTombstoneAndCopyValues() {
        long tombstone = 0;
        long[] values = {tombstone, tombstone, tombstone};
        int notTombstoneIndex = 2;
        values[notTombstoneIndex] = 1;

        var column = new LongMutableColumnImpl(values, tombstone);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        long[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstone via method works the same as #tombstone method calling
        column.write(0, tombstone);

        assertEquals(notTombstoneIndex, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        long longValue = 1;
        column.write(0, longValue);
        assertEquals(0, column.firstRowIndex());

        data = column.getData();
        assertEquals(longValue, data[0]);

        // Find new not tombstone
        column.tombstone(0);
        assertEquals(notTombstoneIndex, column.firstRowIndex());
    }

    @Test
    void testCreateEmptyColumnAndWriteValue() {
        var column = new LongMutableColumnImpl();
        long tombstone = column.getTombstone();

        // Write value at 1st position
        long longValue = 1;
        column.write(0, longValue);

        var data = column.getData();
        assertNotNull(data);
        assertTrue(data.length > 0);
        assertEquals(longValue, data[0]);

        int capacity = column.capacity();
        assertEquals(0, column.firstRowIndex());
        assertTrue(capacity > 0);
        assertEquals(1, column.size());

        for (int i = 1; i < data.length; i++) {
            assertEquals(tombstone, data[i]);
        }
    }

    @Test
    void testColumnWriteTombstoneGap() {
        long longValue = 1;
        long[] source = {longValue};
        var column = new LongMutableColumnImpl(source);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() > 0);
        assertEquals(1, column.size());

        int gap = 42;
        column.write(gap, longValue);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() >= gap);
        assertEquals(gap + 1, column.size());

        // all values between two position is tombstone
        long tombstone = column.getTombstone();
        long[] data = column.getData();
        for (int i = 1; i < gap; i++) {
            assertEquals(tombstone, data[i]);
        }
    }

    @Test
    void testColumnWriteWithNegativePosition() {
        var column = new LongMutableColumnImpl();
        int negativePosition = -1;
        long longValue = 1;
        var exception = assertThrows(
                IllegalArgumentException.class,
                () -> column.write(negativePosition, longValue)
        );
        assertEquals("Out of range %d".formatted(negativePosition), exception.getMessage());
    }

}