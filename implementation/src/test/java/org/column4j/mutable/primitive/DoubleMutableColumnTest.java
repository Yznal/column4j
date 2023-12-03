package org.column4j.mutable.primitive;

import org.column4j.ColumnType;
import org.column4j.mutable.primitive.impl.DoubleMutableColumnImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class DoubleMutableColumnTest {

    @Test
    void testColumnTypeIsFLOAT64() {
        var column = new DoubleMutableColumnImpl();
        assertEquals(ColumnType.FLOAT64, column.type());
    }

    @Test
    void testEmptyColumnNotCreateEmptyArrays() {
        var column1 = new DoubleMutableColumnImpl();
        var column2 = new DoubleMutableColumnImpl();
        assertSame(column1.getData(), column2.getData());
    }

    @Test
    void testEmptyColumnDefaultTombstoneIsDoubleMaxValue() {
        var column = new DoubleMutableColumnImpl();
        double tombstone = column.getTombstone();
        assertEquals(Double.MAX_VALUE, tombstone);
    }

    @Test
    void testEmptyColumn1stRowIsNotDefined() {
        var column = new DoubleMutableColumnImpl();
        assertEquals(-1, column.firstRowIndex());
    }

    @Test
    void testEmptyColumnNotUseSpace() {
        var column = new DoubleMutableColumnImpl();
        assertEquals(0, column.capacity());
        assertEquals(0, column.size());

        var data = column.getData();
        assertNotNull(data);
        assertEquals(0, data.length);
    }

    @Test
    void testColumnWithCustomTombstone() {
        double tombstone = 0;
        var column = new DoubleMutableColumnImpl(tombstone);
        assertEquals(-1, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(-1, column.firstRowIndex());

        double[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstoneHolder via method works the same as #tombstoneHolder method calling
        column.write(0, tombstone);

        assertEquals(-1, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        double doubleValue = Math.random();
        column.write(0, doubleValue);
        assertEquals(0, column.firstRowIndex());

        data = column.getData();
        assertEquals(doubleValue, data[0]);
    }

    @Test
    void testColumnWithCustomTombstoneAndCopyValues() {
        double tombstone = 0;
        double[] values = {tombstone, tombstone, tombstone};
        int notTombstoneIndex = 2;
        values[notTombstoneIndex] = 1;

        var column = new DoubleMutableColumnImpl(values, tombstone);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        column.tombstone(0);
        assertEquals(notTombstoneIndex, column.firstRowIndex());

        double[] data = column.getData();
        assertEquals(tombstone, data[0]);

        // writing tombstoneHolder via method works the same as #tombstoneHolder method calling
        column.write(0, tombstone);

        assertEquals(notTombstoneIndex, column.firstRowIndex());

        data = column.getData();
        assertEquals(tombstone, data[0]);

        double doubleValue = Math.random();
        column.write(0, doubleValue);
        assertEquals(0, column.firstRowIndex());

        data = column.getData();
        assertEquals(doubleValue, data[0]);

        // Find new not tombstoneHolder
        column.tombstone(0);
        assertEquals(notTombstoneIndex, column.firstRowIndex());
    }

    @Test
    void testCreateEmptyColumnAndWriteValue() {
        var column = new DoubleMutableColumnImpl();
        double tombstone = column.getTombstone();

        // Write value at 1st position
        double doubleValue = Math.random();
        column.write(0, doubleValue);

        var data = column.getData();
        assertNotNull(data);
        assertTrue(data.length > 0);
        assertEquals(doubleValue, data[0]);

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
        double doubleValue = Math.random();
        double[] source = {doubleValue};
        var column = new DoubleMutableColumnImpl(source);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() > 0);
        assertEquals(1, column.size());

        int gap = 42;
        column.write(gap, doubleValue);

        assertEquals(0, column.firstRowIndex());
        assertTrue(column.capacity() >= gap);
        assertEquals(gap + 1, column.size());

        // all values between two position is tombstoneHolder
        double tombstone = column.getTombstone();
        double[] data = column.getData();
        for (int i = 1; i < gap; i++) {
            assertEquals(tombstone, data[i]);
        }
    }

    @Test
    void testColumnWriteWithNegativePosition() {
        var column = new DoubleMutableColumnImpl();
        int negativePosition = -1;
        double doubleValue = Math.random();
        var exception = assertThrows(
                IllegalArgumentException.class,
                () -> column.write(negativePosition, doubleValue)
        );
        assertEquals("Out of range %d".formatted(negativePosition), exception.getMessage());
    }

}