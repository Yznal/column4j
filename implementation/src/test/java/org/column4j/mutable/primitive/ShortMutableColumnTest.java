package org.column4j.mutable.primitive;

import org.column4j.ColumnType;
import org.column4j.mutable.primitive.impl.ShortMutableColumnImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class ShortMutableColumnTest {

    @Test
    void testColumnTypeIsINT16() {
        var column = new ShortMutableColumnImpl();
        assertEquals(ColumnType.INT16, column.type());
    }

    @Test
    void testEmptyColumnNotCreateEmptyArrays() {
        var column1 = new ShortMutableColumnImpl();
        var column2 = new ShortMutableColumnImpl();
        assertSame(column1.getData(), column2.getData());
    }

    @Test
    void testEmptyColumnDefaultTombstoneIsShortMaxValue() {
        var column = new ShortMutableColumnImpl();
        short tombstone = column.getTombstone();
        assertEquals(Short.MAX_VALUE, tombstone);
    }

    @Test
    void testEmptyColumn1stRowIsNotDefined() {
        var column = new ShortMutableColumnImpl();
        assertEquals(-1, column.firstRowIndex());
    }

    @Test
    void testEmptyColumnNotUseSpace() {
        var column = new ShortMutableColumnImpl();
        assertEquals(0, column.capacity());
        assertEquals(0, column.size());

        var data = column.getData();
        assertNotNull(data);
        assertEquals(0, data.length);
    }

    @Test
    void testColumnWithCustomTombstone() {
        short tombstone = 0;
        var column = new ShortMutableColumnImpl(tombstone);
        assertEquals(-1, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(-1, column.firstRowIndex());

        short[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstoneHolder via method works the same as #tombstoneHolder method calling
        column.write(0, tombstone);

        assertEquals(-1, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        short shortValue = 1;
        column.write(0, shortValue);
        assertEquals(0, column.firstRowIndex());

        data = column.getData();
        assertEquals(shortValue, data[0]);
    }

    @Test
    void testColumnWithCustomTombstoneAndCopyValues() {
        short tombstone = 0;
        short[] values = {tombstone, tombstone, tombstone};
        int notTombstoneIndex = 2;
        values[notTombstoneIndex] = 1;

        var column = new ShortMutableColumnImpl(values, tombstone);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        short[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstoneHolder via method works the same as #tombstoneHolder method calling
        column.write(0, tombstone);

        assertEquals(notTombstoneIndex, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        short shortValue = 1;
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
        var column = new ShortMutableColumnImpl();
        short tombstone = column.getTombstone();

        // Write value at 1st position
        short shortValue = 1;
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
        short shortValue = 1;
        short[] source = {shortValue};
        var column = new ShortMutableColumnImpl(source);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() > 0);
        assertEquals(1, column.size());

        int gap = 42;
        column.write(gap, shortValue);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() >= gap);
        assertEquals(gap + 1, column.size());

        // all values between two position is tombstoneHolder
        short tombstone = column.getTombstone();
        short[] data = column.getData();
        for (int i = 1; i < gap; i++) {
            assertEquals(tombstone, data[i]);
        }
    }

    @Test
    void testColumnWriteWithNegativePosition() {
        var column = new ShortMutableColumnImpl();
        int negativePosition = -1;
        short shortValue = 1;
        var exception = assertThrows(
                IllegalArgumentException.class,
                () -> column.write(negativePosition, shortValue)
        );
        assertEquals("Out of range %d".formatted(negativePosition), exception.getMessage());
    }

}