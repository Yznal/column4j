package org.column4j.mutable.primitive;

import org.column4j.ColumnType;
import org.column4j.mutable.primitive.impl.IntMutableColumnImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class IntMutableColumnTest {

    @Test
    void testColumnTypeIsINT8() {
        var column = new IntMutableColumnImpl();
        assertEquals(ColumnType.INT32, column.type());
    }

    @Test
    void testEmptyColumnNotCreateEmptyArrays() {
        var column1 = new IntMutableColumnImpl();
        var column2 = new IntMutableColumnImpl();
        assertSame(column1.getData(), column2.getData());
    }

    @Test
    void testEmptyColumnDefaultTombstoneIsIntMaxValue() {
        var column = new IntMutableColumnImpl();
        int tombstone = column.getTombstone();
        assertEquals(Integer.MAX_VALUE, tombstone);
    }

    @Test
    void testEmptyColumn1stRowIsNotDefined() {
        var column = new IntMutableColumnImpl();
        assertEquals(-1, column.firstRowIndex());
    }

    @Test
    void testEmptyColumnNotUseSpace() {
        var column = new IntMutableColumnImpl();
        assertEquals(0, column.capacity());
        assertEquals(0, column.size());

        var data = column.getData();
        assertNotNull(data);
        assertEquals(0, data.length);
    }

    @Test
    void testColumnWithCustomTombstone() {
        int tombstone = 0;
        var column = new IntMutableColumnImpl(tombstone);
        assertEquals(-1, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(-1, column.firstRowIndex());

        int[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstone via method works the same as #tombstone method calling
        column.write(0, tombstone);

        assertEquals(-1, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        int intValue = 1;
        column.write(0, intValue);
        assertEquals(0, column.firstRowIndex());

        data = column.getData();
        assertEquals(intValue, data[0]);
    }

    @Test
    void testColumnWithCustomTombstoneAndCopyValues() {
        int tombstone = 0;
        int[] values = {tombstone, tombstone, tombstone};
        int notTombstoneIndex = 2;
        values[notTombstoneIndex] = 1;

        var column = new IntMutableColumnImpl(values, tombstone);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        int[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstone via method works the same as #tombstone method calling
        column.write(0, tombstone);

        assertEquals(notTombstoneIndex, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        int intValue = 1;
        column.write(0, intValue);
        assertEquals(0, column.firstRowIndex());

        data = column.getData();
        assertEquals(intValue, data[0]);

        // Find new not tombstone
        column.tombstone(0);
        assertEquals(notTombstoneIndex, column.firstRowIndex());
    }

    @Test
    void testCreateEmptyColumnAndWriteValue() {
        var column = new IntMutableColumnImpl();
        int tombstone = column.getTombstone();

        // Write value at 1st position
        int intValue = 1;
        column.write(0, intValue);

        var data = column.getData();
        assertNotNull(data);
        assertTrue(data.length > 0);
        assertEquals(intValue, data[0]);

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
        int intValue = 1;
        int[] source = {intValue};
        var column = new IntMutableColumnImpl(source);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() > 0);
        assertEquals(1, column.size());

        int gap = 42;
        column.write(gap, intValue);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() >= gap);
        assertEquals(gap + 1, column.size());

        // all values between two position is tombstone
        int tombstone = column.getTombstone();
        int[] data = column.getData();
        for (int i = 1; i < gap; i++) {
            assertEquals(tombstone, data[i]);
        }
    }

    @Test
    void testColumnWriteWithNegativePosition() {
        var column = new IntMutableColumnImpl();
        int negativePosition = -1;
        int intValue = 1;
        var exception = assertThrows(
                IllegalArgumentException.class,
                () -> column.write(negativePosition, intValue)
        );
        assertEquals("Out of range %d".formatted(negativePosition), exception.getMessage());
    }

}