package org.column4j.mutable.primitive;

import org.column4j.ColumnType;
import org.column4j.mutable.primitive.impl.FloatMutableColumnImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class FloatMutableColumnTest {

    @Test
    void testColumnTypeIsINT8() {
        var column = new FloatMutableColumnImpl();
        assertEquals(ColumnType.FLOAT32, column.type());
    }

    @Test
    void testEmptyColumnNotCreateEmptyArrays() {
        var column1 = new FloatMutableColumnImpl();
        var column2 = new FloatMutableColumnImpl();
        assertSame(column1.getData(), column2.getData());
    }

    @Test
    void testEmptyColumnDefaultTombstoneIsFloatMaxValue() {
        var column = new FloatMutableColumnImpl();
        float tombstone = column.getTombstone();
        assertEquals(Float.MAX_VALUE, tombstone);
    }

    @Test
    void testEmptyColumn1stRowIsNotDefined() {
        var column = new FloatMutableColumnImpl();
        assertEquals(-1, column.firstRowIndex());
    }

    @Test
    void testEmptyColumnNotUseSpace() {
        var column = new FloatMutableColumnImpl();
        assertEquals(0, column.capacity());
        assertEquals(0, column.size());

        var data = column.getData();
        assertNotNull(data);
        assertEquals(0, data.length);
    }

    @Test
    void testColumnWithCustomTombstone() {
        float tombstone = 0;
        var column = new FloatMutableColumnImpl(tombstone);
        assertEquals(-1, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(-1, column.firstRowIndex());

        float[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstoneHolder via method works the same as #tombstoneHolder method calling
        column.write(0, tombstone);

        assertEquals(-1, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        float floatValue = 1;
        column.write(0, floatValue);
        assertEquals(0, column.firstRowIndex());

        data = column.getData();
        assertEquals(floatValue, data[0]);
    }

    @Test
    void testColumnWithCustomTombstoneAndCopyValues() {
        float tombstone = 0;
        float[] values = {tombstone, tombstone, tombstone};
        int notTombstoneIndex = 2;
        values[notTombstoneIndex] = 1;

        var column = new FloatMutableColumnImpl(values, tombstone);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        float[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstoneHolder via method works the same as #tombstoneHolder method calling
        column.write(0, tombstone);

        assertEquals(notTombstoneIndex, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        float floatValue = 1;
        column.write(0, floatValue);
        assertEquals(0, column.firstRowIndex());

        data = column.getData();
        assertEquals(floatValue, data[0]);

        // Find new not tombstoneHolder
        column.tombstone(0);
        assertEquals(notTombstoneIndex, column.firstRowIndex());
    }

    @Test
    void testCreateEmptyColumnAndWriteValue() {
        var column = new FloatMutableColumnImpl();
        float tombstone = column.getTombstone();

        // Write value at 1st position
        float floatValue = 1;
        column.write(0, floatValue);

        var data = column.getData();
        assertNotNull(data);
        assertTrue(data.length > 0);
        assertEquals(floatValue, data[0]);

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
        float floatValue = 1;
        float[] source = {floatValue};
        var column = new FloatMutableColumnImpl(source);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() > 0);
        assertEquals(1, column.size());

        int gap = 42;
        column.write(gap, floatValue);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() >= gap);
        assertEquals(gap + 1, column.size());

        // all values between two position is tombstoneHolder
        float tombstone = column.getTombstone();
        float[] data = column.getData();
        for (int i = 1; i < gap; i++) {
            assertEquals(tombstone, data[i]);
        }
    }

    @Test
    void testColumnWriteWithNegativePosition() {
        var column = new FloatMutableColumnImpl();
        int negativePosition = -1;
        float floatValue = 1;
        var exception = assertThrows(
                IllegalArgumentException.class,
                () -> column.write(negativePosition, floatValue)
        );
        assertEquals("Out of range %d".formatted(negativePosition), exception.getMessage());
    }

}