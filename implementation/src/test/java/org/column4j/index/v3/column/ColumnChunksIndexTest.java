package org.column4j.index.v3.column;

import org.column4j.index.v3.column.ColumnChunksIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ColumnChunksIndexTest {

    ColumnChunksIndex testMeta;


    // [10,15)----[25,30)-----[40,45)------[92,97)
    @BeforeEach
    void setUp() {
        testMeta = new ColumnChunksIndex(5, 8);
        testMeta.addChunk(0 ,10);
        testMeta.addChunk(3 ,25);
        testMeta.addChunk(7 ,40);
        testMeta.addChunk(23 ,92);

    }

    @Test
    void addChunk() {
        assertFalse(testMeta.empty());
        assertArrayEquals(new int[] {0, 3, 7, 23, 0, 0, 0, 0}, testMeta.getData());
        testMeta.addChunk(30, 150);
        testMeta.addChunk(40, 170);
        assertArrayEquals(new int[] {0, 3, 7, 23, 30, 40, 0, 0}, testMeta.getData());
        //out of order
        testMeta.addChunk(25, 130);
        assertArrayEquals(new int[] {0, 3, 7, 23, 25, 30, 40, 0}, testMeta.getData());
        testMeta.addChunk(45, 180);
        assertEquals(8, testMeta.getData().length);
        // expand
        testMeta.addChunk(36, 190);
        assertEquals(16, testMeta.getData().length);
        assertArrayEquals(new int[] {0, 3, 7, 23, 25, 30, 40, 45, 36, 0,0,0,0,0,0,0}, testMeta.getData());
    }

    @Test
    void searchFullCover() {
        testSearch(new int[]{0, 4}, 0, 150);
    }

    @Test
    void searchFullExact() {
        testSearch(new int[]{0, 4}, 10, 119);
    }

    @Test
    void searchFullBorder() {
        testSearch(new int[]{0, 4}, 12, 100);
    }

    @Test
    void searchFullInsides() {
        testSearch(new int[]{0, 4}, 14, 92);
    }

    @Test
    void searchInnerCover() {
        testSearch(new int[]{1, 3}, 24, 45);
    }

    @Test
    void searchInnerExact() {
        testSearch(new int[]{1, 3}, 25, 44);
    }

    @Test
    void searchInnerBorder() {
        testSearch(new int[]{1, 3}, 29, 40);
    }

    @Test
    void searchInnerInsides() {
        testSearch(new int[]{1, 3}, 27, 43);
    }

    @Test
    void searchLowerUnboundCover() {
        testSearch(new int[]{0, 2}, -1000000, 35);
    }

    @Test
    void searchLowerUnboundExact() {
        testSearch(new int[]{0, 2}, -1000000, 29);
    }

    @Test
    void searchLowerUnboundBorder() {
        testSearch(new int[]{0, 2}, -1000000, 25);
    }

    @Test
    void searchLowerUnboundInsidex() {
        testSearch(new int[]{0, 2}, -1000000, 27);
    }

    @Test
    void searchHigherUnboundCover() {
        testSearch(new int[]{2, 4}, 35, 1000000);
    }

    @Test
    void searchHigherUnboundExact() {
        testSearch(new int[]{2, 4}, 40, 1000000);
    }

    @Test
    void searchHigherUnboundBorder() {
        testSearch(new int[]{2, 4}, 44, 1000000);
    }

    @Test
    void searchHigherUnboundInside() {
        testSearch(new int[]{2, 4}, 42, 1000000);
    }

    @Test
    void searchWrongBounds() {
        assertNull(testMeta.searchInterval(10, 5));
    }

    @Test
    void searchOutOfBounds() {
        assertNull(testMeta.searchInterval(0, 9));
        assertNull(testMeta.searchInterval(120, 130));
    }


    private void testSearch(int[] expected, int start, int end) {
        int[] result = testMeta.searchInterval(start, end);
        assertNotNull(result);
        assertArrayEquals(expected, result);
        testMeta.setLinearThreshold(-1); // forced binary search
        result = testMeta.searchInterval(start, end);
        assertNotNull(result);
        assertArrayEquals(expected, result);
    }

}