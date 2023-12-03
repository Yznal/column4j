package org.column4j.mutable.primitive;

import org.column4j.ColumnType;
import org.column4j.mutable.impl.StringMutableColumnImpl;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class StringMutableColumnTest {

    @Test
    void testColumnTypeIsINT64() {
        var column = new StringMutableColumnImpl();
        assertEquals(ColumnType.STRING, column.type());
    }

    @Test
    void testEmptyColumnNotCreateEmptyArrays() {
        var column1 = new StringMutableColumnImpl();
        var column2 = new StringMutableColumnImpl();
        assertSame(column1.getData(), column2.getData());
    }

    @Test
    void testEmptyColumnDefaultTombstoneIsNull() {
        var column = new StringMutableColumnImpl();
        var tombstone = column.getTombstone();
        assertNull(tombstone);
    }

    @Test
    void testEmptyColumn1stRowIsNotDefined() {
        var column = new StringMutableColumnImpl();
        assertEquals(-1, column.firstRowIndex());
    }

    @Test
    void testEmptyColumnNotUseSpace() {
        var column = new StringMutableColumnImpl();
        assertEquals(0, column.capacity());
        assertEquals(0, column.size());

        var data = column.getData();
        assertNotNull(data);
        assertEquals(0, data.length);
    }

    @Test
    void testColumnWithCustomTombstone() {
        var tombstone = "";
        var column = new StringMutableColumnImpl(tombstone);
        assertEquals(-1, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(-1, column.firstRowIndex());

        String[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstoneHolder via method works the same as #tombstoneHolder method calling
        column.write(0, tombstone);

        assertEquals(-1, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        String shortValue = UUID.randomUUID().toString();
        column.write(0, shortValue);
        assertEquals(0, column.firstRowIndex());

        data = column.getData();
        assertEquals(shortValue, data[0]);
    }

    @Test
    void testColumnWithCustomTombstoneAndCopyValues() {
        var tombstone = "";
        String[] values = {tombstone, tombstone, tombstone};
        int notTombstoneIndex = 2;
        values[notTombstoneIndex] = UUID.randomUUID().toString();

        var column = new StringMutableColumnImpl(values, tombstone);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        String[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstoneHolder via method works the same as #tombstoneHolder method calling
        column.write(0, tombstone);

        assertEquals(notTombstoneIndex, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        String shortValue = UUID.randomUUID().toString();
        column.write(0, shortValue);
        assertEquals(0, column.firstRowIndex());

        data = column.getData();
        assertEquals(shortValue, data[0]);

        // Find new not tombstoneHolder
        column.tombstone(0);
        assertEquals(notTombstoneIndex, column.firstRowIndex());
    }

    @Test
    void testCreateEmptyColumnAndWriteValue() {
        var column = new StringMutableColumnImpl();
        var tombstone = column.getTombstone();

        // Write value at 1st position
        String shortValue = UUID.randomUUID().toString();
        column.write(0, shortValue);

        var data = column.getData();
        assertNotNull(data);
        assertTrue(data.length > 0);
        assertEquals(shortValue, data[0]);

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
        String shortValue = UUID.randomUUID().toString();
        String[] source = {shortValue};
        var column = new StringMutableColumnImpl(source);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() > 0);
        assertEquals(1, column.size());

        int gap = 42;
        column.write(gap, shortValue);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() >= gap);
        assertEquals(gap + 1, column.size());

        // all values between two position is tombstoneHolder
        String tombstone = column.getTombstone();
        String[] data = column.getData();
        for (int i = 1; i < gap; i++) {
            assertEquals(tombstone, data[i]);
        }
    }

    @Test
    void testColumnWriteWithNegativePosition() {
        var column = new StringMutableColumnImpl();
        int negativePosition = -1;
        String shortValue = UUID.randomUUID().toString();
        var exception = assertThrows(
                IllegalArgumentException.class,
                () -> column.write(negativePosition, shortValue)
        );
        assertEquals("Out of range %d".formatted(negativePosition), exception.getMessage());
    }

}