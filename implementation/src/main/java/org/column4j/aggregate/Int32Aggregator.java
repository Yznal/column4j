package org.column4j.aggregate;

import org.column4j.column.mutable.primitive.Int32MutableColumn;
import org.column4j.utils.Int32VectorUtils;

public class Int32Aggregator {
    static public int indexOfAnother(Int32MutableColumn column, int value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (to / chunkSize == from / chunkSize) {
            return offset + Int32VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, to % chunkSize);
        }

        int res = Int32VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < to / chunkSize; i++) {
            offset += chunkSize;
            res = Int32VectorUtils.indexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        return offset + Int32VectorUtils.indexOfAnother(column.getChunk(to / chunkSize).getData(), value, 0, to % chunkSize);
    }

    static public int lastIndexOfAnother(Int32MutableColumn column, int value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (to / chunkSize == from / chunkSize) {
            return offset + Int32VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, to % chunkSize);
        }

        int res =  Int32VectorUtils.lastIndexOfAnother(column.getChunk(to / chunkSize).getData(), value, 0, to % chunkSize);
        if (res != -1) {
            return offset + res;
        }

        for (int i = to / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = Int32VectorUtils.lastIndexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        return offset + Int32VectorUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
    }

    static public int min(Int32MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final int tombstone = column.getTombstone();

        if (to / chunkSize == from / chunkSize) {
            return Int32VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, to % chunkSize);
        }

        int res = Int32VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < to / chunkSize; i++) {
            res = (int)Math.min(res, column.getChunk(i).getStatistic().getMin());
        }

        final int minInTail = Int32VectorUtils.min(column.getChunk(to / chunkSize).getData(), tombstone, 0, to % chunkSize);
        res = (int)Math.min(res, minInTail);

        return res;
    }

    static public int max(Int32MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final int tombstone = column.getTombstone();

        if (to / chunkSize == from / chunkSize) {
            return Int32VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, to % chunkSize);
        }

        int res = Int32VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < to / chunkSize; i++) {
            res = Math.max(res, column.getChunk(i).getStatistic().getMax());
        }

        final int maxInTail = Int32VectorUtils.max(column.getChunk(to / chunkSize).getData(), tombstone, 0, to % chunkSize);
        res = Math.max(res, maxInTail);

        return res;
    }
}