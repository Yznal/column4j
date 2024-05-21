package org.column4j.aggregate;

import org.column4j.column.impl.mutable.primitive.Int64MutableColumnImpl;
import org.column4j.column.mutable.primitive.Int64MutableColumn;
import org.column4j.utils.Int64VectorUtils;

public class Int64Aggregator {
    static public int indexOfAnother(Int64MutableColumn column, long value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int64VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res = Int64VectorUtils.indexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            offset += chunkSize;
            res = Int64VectorUtils.indexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        res = Int64VectorUtils.indexOfAnother(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
        if (res != -1) {
            return offset + res;
        }
        return -1;
    }

    static public int lastIndexOfAnother(Int64MutableColumn column, long value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int64VectorUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res =  Int64VectorUtils.lastIndexOfAnother(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
        if (res != -1) {
            return offset + res;
        }

        for (int i = (to - 1) / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = Int64VectorUtils.lastIndexOfAnother(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        res = Int64VectorUtils.lastIndexOfAnother(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }
        return -1;
    }

    static public int indexOf(Int64MutableColumn column, long value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = from - from % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int64VectorUtils.indexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res = Int64VectorUtils.indexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            offset += chunkSize;
            res = Int64VectorUtils.indexOf(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset += chunkSize;
        res = Int64VectorUtils.indexOf(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
        if (res != -1) {
            return offset + res;
        }
        return -1;
    }

    static public int lastIndexOf(Int64MutableColumn column, long value, int from, int to) {
        final int chunkSize = column.chunkSize();
        int offset = to - to % chunkSize;

        if (from / chunkSize == (to - 1) / chunkSize) {
            return offset + Int64VectorUtils.lastIndexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        int res =  Int64VectorUtils.lastIndexOf(column.getChunk((to - 1) / chunkSize).getData(), value, 0, (to - 1) % chunkSize + 1);
        if (res != -1) {
            return offset + res;
        }

        for (int i = (to - 1) / chunkSize - 1; i > from / chunkSize; i--) {
            offset -= chunkSize;
            res = Int64VectorUtils.lastIndexOf(column.getChunk(i).getData(), value, 0, column.chunkSize());
            if (res != -1) {
                return offset + res;
            }
        }

        offset -= chunkSize;
        res = Int64VectorUtils.lastIndexOf(column.getChunk(from / chunkSize).getData(), value, from % chunkSize, column.chunkSize());
        if (res != -1) {
            return offset + res;
        }
        return -1;
    }

    static public long min(Int64MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final long tombstone = column.getTombstone();

        if (from / chunkSize == (to - 1) / chunkSize) {
            return Int64VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, (to - 1) % chunkSize);
        }

        long res = Int64VectorUtils.min(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            res = Math.min(res, column.getChunk(i).getStatistic().getMin());
        }

        final long minInTail = Int64VectorUtils.min(column.getChunk((to - 1) / chunkSize).getData(), tombstone, 0, (to - 1) % chunkSize + 1);
        res = Math.min(res, minInTail);

        return res;
    }

    static public long max(Int64MutableColumn column, int from, int to) {
        final int chunkSize = column.chunkSize();
        final long tombstone = column.getTombstone();

        if (from / chunkSize == (to - 1) / chunkSize) {
            return Int64VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, (to - 1) % chunkSize + 1);
        }

        long res = Int64VectorUtils.max(column.getChunk(from / chunkSize).getData(), tombstone, from % chunkSize, chunkSize);

        for (int i = from / chunkSize + 1; i < (to - 1) / chunkSize; i++) {
            res = Math.max(res, column.getChunk(i).getStatistic().getMax());
        }

        final long maxInTail = Int64VectorUtils.max(column.getChunk((to - 1) / chunkSize).getData(), tombstone, 0, (to - 1) % chunkSize + 1);
        res = Math.max(res, maxInTail);

        return res;
    }

    static public Int64MutableColumn mul(Int64MutableColumn column1, Int64MutableColumn column2, int elements, int resChunkSize) {
        long[] data1 = column1.getByIndexes(0, elements);
        long[] data2 = column2.getByIndexes(0, elements);
        long[] resData = Int64VectorUtils.mul(data1, data2, 0, 0, elements);
        var resColumn = new Int64MutableColumnImpl(resChunkSize, column1.getTombstone() * column2.getTombstone());
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Int64MutableColumn sum(Int64MutableColumn column1, Int64MutableColumn column2, int elements, int resChunkSize) {
        long[] data1 = column1.getByIndexes(0, elements);
        long[] data2 = column2.getByIndexes(0, elements);
        long[] resData = Int64VectorUtils.sum(data1, data2, 0, 0, elements);
        var resColumn = new Int64MutableColumnImpl(resChunkSize, column1.getTombstone() + column2.getTombstone());
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Int64MutableColumn mul(Int64MutableColumn[] columns, int elements, int resChunkSize) {
        long resTombstone = columns[0].getTombstone();
        long[] resData = columns[0].getByIndexes(0, elements - 1);

        for (int i = 1; i < columns.length; i++) {
            resTombstone *= columns[i].getTombstone();
            long[] data = columns[i].getByIndexes(0, elements);
            resData = Int64VectorUtils.mul(data, resData, 0, 0, elements);
        }

        var resColumn = new Int64MutableColumnImpl(resChunkSize, resTombstone);
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }

    static public Int64MutableColumn sum(Int64MutableColumn[] columns, int elements, int resChunkSize) {
        long resTombstone = columns[0].getTombstone();
        long[] resData = columns[0].getByIndexes(0, elements);

        for (int i = 1; i < columns.length; i++) {
            resTombstone += columns[i].getTombstone();
            long[] data = columns[i].getByIndexes(0, elements);
            resData = Int64VectorUtils.sum(data, resData, 0, 0, elements);
        }

        var resColumn = new Int64MutableColumnImpl(resChunkSize, resTombstone);
        resColumn.writeByIndexes(0, elements - 1, resData);
        return resColumn;
    }
}
