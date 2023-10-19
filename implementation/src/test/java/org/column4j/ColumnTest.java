package org.column4j;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author sibmaks
 * @since 0.0.1
 */
@Disabled
class ColumnTest {

    @Test
    @DisplayName("Use case of int column")
    void testColumn() {
        var mutableIntsColumn = new IntMutableColumnImpl();

        int firstRowIndex = mutableIntsColumn.firstRowIndex();
        assertEquals(-1, firstRowIndex);

        int capacity = mutableIntsColumn.capacity();
        assertEquals(0, capacity);

        int size = mutableIntsColumn.size();
        assertEquals(0, size);

        // write in 5th row
        int not1stPos = 5;
        int magicNumber = 42;
        mutableIntsColumn.write(not1stPos, magicNumber);

        firstRowIndex = mutableIntsColumn.firstRowIndex();
        assertEquals(not1stPos, firstRowIndex, "Index of 1st row incorrect");

        capacity = mutableIntsColumn.capacity();
        assertEquals(1, capacity, "Column capacity isn't correct");

        size = mutableIntsColumn.size();
        assertEquals(not1stPos, size, "Column size isn't correct");

        // append tombstone
        mutableIntsColumn.tombstone(not1stPos + 1);
        assertEquals(not1stPos, firstRowIndex, "Index of 1st row should not changes of append");

        capacity = mutableIntsColumn.capacity();
        assertEquals(1, capacity, "Row capacity should not be changed on tombstone write");

        size = mutableIntsColumn.size();
        assertEquals(not1stPos + 1, size, "Row size should be increased on tombstone append");

        int[] data = mutableIntsColumn.getData();
        assertNotNull(data);

        assertEquals(not1stPos + 1, data.length);
        assertEquals(magicNumber, data[not1stPos]);
        // TODO: tombstone possibly Integer#MAX_VALUE or Integer#MIN_VALUE

    }

}