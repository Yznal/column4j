package org.column4j.mutable.primitive;

import org.column4j.ColumnType;
import org.column4j.mutable.primitive.impl.ByteMutableColumnImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class ByteMutableColumnTest {

    @Test
    void testColumnTypeIsINT8() {
        var column = new ByteMutableColumnImpl();
        assertEquals(ColumnType.INT8, column.type());
    }

    @Test
    void testEmptyColumnNotCreateEmptyArrays() {
        var column1 = new ByteMutableColumnImpl();
        var column2 = new ByteMutableColumnImpl();
        assertSame(column1.getData(), column2.getData());
    }

    @Test
    void testEmptyColumnDefaultTombstoneIsByteMaxValue() {
        var column = new ByteMutableColumnImpl();
        byte tombstone = column.getTombstone();
        assertEquals(Byte.MAX_VALUE, tombstone);
    }

    @Test
    void testEmptyColumn1stRowIsNotDefined() {
        var column = new ByteMutableColumnImpl();
        assertEquals(-1, column.firstRowIndex());
    }

    @Test
    void testEmptyColumnNotUseSpace() {
        var column = new ByteMutableColumnImpl();
        assertEquals(0, column.capacity());
        assertEquals(0, column.size());

        var data = column.getData();
        assertNotNull(data);
        assertEquals(0, data.length);
    }

    @Test
    void testColumnWithCustomTombstone() {
        byte tombstone = 0;
        var column = new ByteMutableColumnImpl(tombstone);
        assertEquals(-1, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(-1, column.firstRowIndex());

        byte[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstoneHolder via method works the same as #tombstoneHolder method calling
        column.write(0, tombstone);

        assertEquals(-1, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        byte byteValue = 1;
        column.write(0, byteValue);
        assertEquals(0, column.firstRowIndex());

        data = column.getData();
        assertEquals(byteValue, data[0]);
    }

    @Test
    void testColumnWithCustomTombstoneAndCopyValues() {
        byte tombstone = 0;
        byte[] values = {tombstone, tombstone, tombstone};
        int notTombstoneIndex = 2;
        values[notTombstoneIndex] = 1;

        var column = new ByteMutableColumnImpl(values, tombstone);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        byte[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstoneHolder via method works the same as #tombstoneHolder method calling
        column.write(0, tombstone);

        assertEquals(notTombstoneIndex, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        byte byteValue = 1;
        column.write(0, byteValue);
        assertEquals(0, column.firstRowIndex());

        data = column.getData();
        assertEquals(byteValue, data[0]);

        // Find new not tombstoneHolder
        column.tombstone(0);
        assertEquals(notTombstoneIndex, column.firstRowIndex());
    }

    @Test
    void testCreateEmptyColumnAndWriteValue() {
        var column = new ByteMutableColumnImpl();
        byte tombstone = column.getTombstone();

        // Write value at 1st position
        byte byteValue = 1;
        column.write(0, byteValue);

        var data = column.getData();
        assertNotNull(data);
        assertTrue(data.length > 0);
        assertEquals(byteValue, data[0]);

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
        byte byteValue = 1;
        byte[] source = {byteValue};
        var column = new ByteMutableColumnImpl(source);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() > 0);
        assertEquals(1, column.size());

        int gap = 42;
        column.write(gap, byteValue);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() >= gap);
        assertEquals(gap + 1, column.size());

        // all values between two position is tombstoneHolder
        byte tombstone = column.getTombstone();
        byte[] data = column.getData();
        for (int i = 1; i < gap; i++) {
            assertEquals(tombstone, data[i]);
        }
    }

    @Test
    void testColumnWriteWithNegativePosition() {
        var column = new ByteMutableColumnImpl();
        int negativePosition = -1;
        byte byteValue = 1;
        var exception = assertThrows(
                IllegalArgumentException.class,
                () -> column.write(negativePosition, byteValue)
        );
        assertEquals("Out of range %d".formatted(negativePosition), exception.getMessage());
    }

}