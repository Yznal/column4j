package com.yandex.column4j;

import com.yandex.column4j.base.ColumnType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author sibmaks
 * @since 0.0.1
 */
class ColumnTest {

    @Test
    @DisplayName("Тест кейс использования колонки")
    void testColumn() {
        var mutableIntsColumn = new MutableColumnImpl<int[]>(ColumnType.INT32);

        int firstRowIndex = mutableIntsColumn.firstRowIndex();
        assertEquals(-1, firstRowIndex);

        int capacity = mutableIntsColumn.capacity();
        assertEquals(0, capacity);

        int size = mutableIntsColumn.size();
        assertEquals(0, size);

        // записываем в 5-ую строчку число
        int not1stPos = 5;
        int magicNumber = 42;
        mutableIntsColumn.write(not1stPos, magicNumber);

        firstRowIndex = mutableIntsColumn.firstRowIndex();
        assertEquals(not1stPos, firstRowIndex, "Индекс первой заполненной строчки не совпадает с ожидаемым");

        capacity = mutableIntsColumn.capacity();
        assertEquals(1, capacity, "Количество не пустых строк не совпадает с ожидаемым");

        size = mutableIntsColumn.size();
        assertEquals(not1stPos, size, "Размер колонки не совпадает с ожидаемым");

        // записываем пустое значение
        mutableIntsColumn.tombstone(not1stPos + 1);
        assertEquals(not1stPos, firstRowIndex, "Индекс первой заполненной строчки не должен сдвигаться");

        capacity = mutableIntsColumn.capacity();
        assertEquals(1, capacity, "Количество не пустых строк не меняется при записи пустого значения");

        size = mutableIntsColumn.size();
        assertEquals(not1stPos + 1, size, "Размер колонки не совпадает с ожидаемым после записи пустого значения");

        int[] data = mutableIntsColumn.data();
        assertNotNull(data);

        assertEquals(not1stPos + 1, data.length);
        assertEquals(magicNumber, data[not1stPos]);
        // TODO: пустые значения видимо либо Integer#MAX_VALUE, либо Integer#MIN_VALUE

    }

}