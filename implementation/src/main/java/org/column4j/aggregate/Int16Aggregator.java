package org.column4j.aggregate;

import org.column4j.column.mutable.primitive.Int16MutableColumn;
import org.column4j.utils.Int16VectorUtils;

public class Int16Aggregator {
    static public int indexOfAnother(Int16MutableColumn column, short value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (to / chunkSize == from / chunkSize) {
            return offset + Int16VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, to % chunkSize);
        }

        int res = Int16VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < to / chunkSize; i++) {
            offset += chunkSize;
            res = Int16VectorUtils.indexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        return offset + Int16VectorUtils.indexOfAnother(column.getChunk(to / chunkSize).getData(), value, 0, to % chunkSize);
    }

    static public int lastIndexOfAnother(Int16MutableColumn column, short value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (to / chunkSize == from / chunkSize) {
            return offset + Int16VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, to % chunkSize);
        }

        int res =  Int16VectorUtils.lastIndexOfAnother(column.getChunk(to / chunkSize).getData(), value, 0, to % chunkSize);
        if (res != -1) {
            return offset + res;
        }

        for (int i = to / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = Int16VectorUtils.lastIndexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        return offset + Int16VectorUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
    }

    static public short min(Int16MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final short tombstone = column.getTombstone();

        if (to / chunkSize == from / chunkSize) {
            return Int16VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, to % chunkSize);
        }

        short res = Int16VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < to / chunkSize; i++) {
            res = (short)Math.min(res, column.getChunk(i).getStatistic().getMin());
        }

        final short minInTail = Int16VectorUtils.min(column.getChunk(to / chunkSize).getData(), tombstone, 0, to % chunkSize);
        res = (short)Math.min(res, minInTail);

        return res;
    }

    static public short max(Int16MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final short tombstone = column.getTombstone();

        if (to / chunkSize == from / chunkSize) {
            return Int16VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, to % chunkSize);
        }

        short res = Int16VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < to / chunkSize; i++) {
            res = (short)Math.max(res, column.getChunk(i).getStatistic().getMax());
        }

        final short maxInTail = Int16VectorUtils.max(column.getChunk(to / chunkSize).getData(), tombstone, 0, to % chunkSize);
        res = (short)Math.max(res, maxInTail);

        return res;
    }
}
