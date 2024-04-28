package org.column4j.aggregate;

import org.column4j.column.mutable.primitive.Int64MutableColumn;
import org.column4j.utils.Int64VectorUtils;

public class Int64Aggregator {
    static public int indexOfAnother(Int64MutableColumn column, long value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (to / chunkSize == from / chunkSize) {
            return offset + Int64VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, to % chunkSize);
        }

        int res = Int64VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < to / chunkSize; i++) {
            offset += chunkSize;
            res = Int64VectorUtils.indexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        return offset + Int64VectorUtils.indexOfAnother(column.getChunk(to / chunkSize).getData(), value, 0, to % chunkSize);
    }

    static public int lastIndexOfAnother(Int64MutableColumn column, long value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (to / chunkSize == from / chunkSize) {
            return offset + Int64VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, to % chunkSize);
        }

        int res =  Int64VectorUtils.lastIndexOfAnother(column.getChunk(to / chunkSize).getData(), value, 0, to % chunkSize);
        if (res != -1) {
            return offset + res;
        }

        for (int i = to / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = Int64VectorUtils.lastIndexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        return offset + Int64VectorUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
    }

    static public long min(Int64MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final long tombstone = column.getTombstone();

        if (to / chunkSize == from / chunkSize) {
            return Int64VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, to % chunkSize);
        }

        long res = Int64VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < to / chunkSize; i++) {
            res = Math.min(res, column.getChunk(i).getStatistic().getMin());
        }

        final long minInTail = Int64VectorUtils.min(column.getChunk(to / chunkSize).getData(), tombstone, 0, to % chunkSize);
        res = Math.min(res, minInTail);

        return res;
    }

    static public long max(Int64MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final long tombstone = column.getTombstone();

        if (to / chunkSize == from / chunkSize) {
            return Int64VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, to % chunkSize);
        }

        long res = Int64VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < to / chunkSize; i++) {
            res = Math.max(res, column.getChunk(i).getStatistic().getMax());
        }

        final long maxInTail = Int64VectorUtils.max(column.getChunk(to / chunkSize).getData(), tombstone, 0, to % chunkSize);
        res = Math.max(res, maxInTail);

        return res;
    }
}
